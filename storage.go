package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// FileMeta is the per-object metadata. Everything needed to render a file
// chip (name/mime/size) and to verify integrity (sha256) rides on the object
// itself — there is no DB mapping table.
type FileMeta struct {
	Code            string `json:"code"`
	Name            string `json:"name"`
	Mime            string `json:"mime"`
	Size            int64  `json:"size"`
	UploaderSession string `json:"uploader_session,omitempty"`
	CreatedAt       string `json:"created_at"`
	Sha256          string `json:"sha256"`
}

// BlobStore abstracts the object-store backend. Local and S3 (any S3-compatible
// endpoint, e.g. MinIO) are the two implementations; both are addressed purely
// by `code` (the object key).
type BlobStore interface {
	Put(ctx context.Context, meta FileMeta, data []byte) error
	Get(ctx context.Context, code string) (FileMeta, []byte, error)
	Stat(ctx context.Context, code string) (FileMeta, error)
	Presign(ctx context.Context, code string, ttl time.Duration) (string, error)
}

// NewBlobStore selects a backend from env. `FILES_STORAGE=s3` uses any
// S3-compatible endpoint (MinIO / AWS S3); the default is a local disk
// directory (FILES_LOCAL_DIR).
func NewBlobStore(env func(string) string) BlobStore {
	switch strings.ToLower(env("FILES_STORAGE")) {
	case "s3":
		return NewS3Store(env)
	default:
		return NewLocalStore(env)
	}
}

// ---- local disk backend (default) ----

type LocalStore struct {
	root string
}

func NewLocalStore(env func(string) string) *LocalStore {
	root := env("FILES_LOCAL_DIR")
	if root == "" {
		root = "/files"
	}
	return &LocalStore{root: root}
}

func (s *LocalStore) Put(ctx context.Context, meta FileMeta, data []byte) error {
	if err := os.MkdirAll(s.dir(meta.Code), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(s.path(meta.Code), data, 0o644); err != nil {
		return err
	}
	b, _ := json.Marshal(meta)
	return os.WriteFile(s.metaPath(meta.Code), b, 0o644)
}

func (s *LocalStore) Get(ctx context.Context, code string) (FileMeta, []byte, error) {
	data, err := os.ReadFile(s.path(code))
	if err != nil {
		return FileMeta{}, nil, err
	}
	meta, err := s.Stat(ctx, code)
	if err != nil {
		return FileMeta{}, nil, err
	}
	return meta, data, nil
}

func (s *LocalStore) Stat(ctx context.Context, code string) (FileMeta, error) {
	b, err := os.ReadFile(s.metaPath(code))
	if err != nil {
		return FileMeta{}, err
	}
	var meta FileMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return FileMeta{}, err
	}
	return meta, nil
}

func (s *LocalStore) Presign(ctx context.Context, code string, ttl time.Duration) (string, error) {
	return "", fmt.Errorf("presign not supported for local store")
}

func (s *LocalStore) dir(code string) string {
	if len(code) >= 2 {
		return filepath.Join(s.root, code[:2])
	}
	return s.root
}

func (s *LocalStore) path(code string) string {
	return filepath.Join(s.dir(code), code)
}

func (s *LocalStore) metaPath(code string) string {
	return filepath.Join(s.dir(code), code+".json")
}

// ---- S3-compatible backend (MinIO / AWS S3) ----

type S3Store struct {
	client    *s3.Client
	presigner *s3.PresignClient
	bucket    string
	keyPrefix string
}

func NewS3Store(env func(string) string) *S3Store {
	endpoint := env("FILES_S3_ENDPOINT")
	region := env("FILES_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	access := env("FILES_S3_ACCESS_KEY")
	secret := env("FILES_S3_SECRET_KEY")
	pathStyle := env("FILES_S3_PATH_STYLE")
	usePathStyle := pathStyle == "" || pathStyle == "1" || pathStyle == "true"

	var opts []func(*awscfg.LoadOptions) error
	if endpoint != "" {
		opts = append(opts, awscfg.WithBaseEndpoint(endpoint))
	}
	if access != "" {
		opts = append(opts, awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(access, secret, ""),
		))
	}
	opts = append(opts, awscfg.WithRegion(region))
	cfg, err := awscfg.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		panic(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if usePathStyle {
			o.UsePathStyle = true
		}
	})
	return &S3Store{
		client:    client,
		presigner: s3.NewPresignClient(client),
		bucket:    env("FILES_S3_BUCKET"),
		keyPrefix: env("FILES_S3_PREFIX"),
	}
}

func (s *S3Store) key(code string) string {
	if s.keyPrefix == "" {
		return code
	}
	return strings.TrimRight(s.keyPrefix, "/") + "/" + code
}

func (s *S3Store) Put(ctx context.Context, meta FileMeta, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s.key(meta.Code)),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(meta.Mime),
		Metadata:    metaToAWS(meta),
	})
	return err
}

func (s *S3Store) Get(ctx context.Context, code string) (FileMeta, []byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(code)),
	})
	if err != nil {
		return FileMeta{}, nil, err
	}
	defer out.Body.Close()
	meta := metaFromAWS(code, aws.ToString(out.ContentType), out.Metadata)
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(out.Body); err != nil {
		return FileMeta{}, nil, err
	}
	meta.Size = int64(buf.Len())
	return meta, buf.Bytes(), nil
}

func (s *S3Store) Stat(ctx context.Context, code string) (FileMeta, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(code)),
	})
	if err != nil {
		return FileMeta{}, err
	}
	meta := metaFromAWS(code, aws.ToString(out.ContentType), out.Metadata)
	if out.ContentLength != nil {
		meta.Size = *out.ContentLength
	}
	return meta, nil
}

func (s *S3Store) Presign(ctx context.Context, code string, ttl time.Duration) (string, error) {
	res, err := s.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(code)),
	}, func(o *s3.PresignOptions) {
		if ttl > 0 {
			o.Expires = ttl
		}
	})
	if err != nil {
		return "", err
	}
	return res.URL, nil
}

func metaToAWS(m FileMeta) map[string]string {
	return map[string]string{
		"code":             m.Code,
		"name":             m.Name,
		"mime":             m.Mime,
		"size":             fmt.Sprintf("%d", m.Size),
		"uploader_session": m.UploaderSession,
		"created_at":       m.CreatedAt,
		"sha256":           m.Sha256,
	}
}

func metaFromAWS(code string, contentType string, md map[string]string) FileMeta {
	get := func(k string) string {
		for kk, vv := range md {
			if strings.EqualFold(kk, k) {
				return vv
			}
		}
		return ""
	}
	var size int64
	if v := get("size"); v != "" {
		_, _ = fmt.Sscanf(v, "%d", &size)
	}
	mime := get("mime")
	if mime == "" {
		mime = contentType
	}
	return FileMeta{
		Code:            code,
		Name:            get("name"),
		Mime:            mime,
		Size:            size,
		UploaderSession: get("uploader_session"),
		CreatedAt:       get("created_at"),
		Sha256:          get("sha256"),
	}
}

// ---- helpers ----

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// randomCode mints an unguessable, collision-resistant object key. The code
// is a plain random hex string; it is NOT derived from content (content
// dedup is handled by the sha256→code index, never by the key itself).
func randomCode(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// defaultCodeLength is the object key length in bytes (→ 2*n hex chars).
const defaultCodeLength = 12
