package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	abcprotocol "github.com/abcp-sdk/abc-protocol-go"
	"github.com/abcp-sdk/abc-protocol-go/extension"
	"github.com/abcp-sdk/abc-protocol-go/manifest"
	natsbus "github.com/abcp-sdk/abc-protocol-go/transport/nats"
)

//go:embed manifest.yaml
var manifestYaml []byte

// memory-extension replaces the Rust memory-tools service: it persists the
// session todo list in Postgres and exposes it both as a NATS tool
// (`todowrite`) and over HTTP (`GET/POST /api/v1/todos`).
//
// The HTTP list endpoint answers the UI contract (sessionId/position/
// createdAt), mapping the legacy table columns (session_id/created_unix)
// on the way out; the underlying table stays compatible with the Rust
// version's schema so no data migration is required.

type server struct {
	pool  *pgxpool.Pool
	blobs BlobStore
	ext   *extension.Extension
}

// Todo mirrors the UI-facing shape (frontend TodoSchema).
type Todo struct {
	ID        string `json:"id"`
	SessionID string `json:"sessionId"`
	Content   string `json:"content"`
	Status    string `json:"status"`
	Priority  string `json:"priority"`
	Position  int64  `json:"position"`
	CreatedAt string `json:"createdAt"`
}

func main() {
	log := slog.Default().With("svc", "memory-extension")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := OpenPool(ctx, pgConfig())
	if err != nil {
		log.Error("pg connect failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	s := &server{pool: pool, blobs: NewBlobStore(os.Getenv)}

	nbus, err := natsbus.Connect(envOr("NATS_URL", "nats://nats.zergx.svc.cluster.local:4222"))
	if err != nil {
		log.Error("nats connect failed", "err", err)
		os.Exit(1)
	}
	m, err := manifest.ParseManifest(manifestYaml)
	if err != nil {
		log.Error("load manifest failed", "err", err)
		os.Exit(1)
	}
	if err := extension.Serve(
		extension.New(nbus, m.BuildConfig(manifest.Bindings{Handlers: s.handlers()})),
		extension.ServeOptions{
			Handler: s.router(),
			Run: func(_ context.Context, ext *extension.Extension) {
				s.ext = ext
			},
		},
	); err != nil {
		log.Error("serve failed", "err", err)
		os.Exit(1)
	}
}

// ---- pg ----

type PgConfig struct {
	Host, Port, User, Password, DB string
}

func pgConfig() PgConfig {
	return PgConfig{
		Host:     envOr("POSTGRES_HOST", "postgres.zergx.svc.cluster.local"),
		Port:     envNormalizePort(envOr("POSTGRES_PORT", "5432")),
		User:     envOr("POSTGRES_USER", "root"),
		Password: envOr("POSTGRES_PASSWORD", "devpassword"),
		DB:       envOr("POSTGRES_DB_AGENT", "zergx_agent"),
	}
}

func (c PgConfig) dsn() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", c.User, c.Password, c.Host, c.Port, c.DB)
}

func OpenPool(ctx context.Context, cfg PgConfig) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, cfg.dsn())
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	// Same schema as the Rust memory-tools (idempotent CREATE IF NOT EXISTS).
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS todos (
			id BIGSERIAL PRIMARY KEY,
			session_id TEXT NOT NULL,
			content TEXT NOT NULL,
			status TEXT NOT NULL,
			priority TEXT NOT NULL,
			created_unix BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT)
		);
		CREATE INDEX IF NOT EXISTS idx_todos_session ON todos (session_id);
		CREATE TABLE IF NOT EXISTS file_dedup (
			sha256 TEXT PRIMARY KEY,
			code   TEXT NOT NULL UNIQUE,
			created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT)
		);`)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// ---- store ops ----

func (s *server) writeTodos(ctx context.Context, sid string, todos []map[string]interface{}) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM todos WHERE session_id = $1`, sid); err != nil {
		return 0, err
	}
	for _, t := range todos {
		content := strArg(t, "content")
		status := strArg(t, "status")
		if status == "" {
			status = "pending"
		}
		priority := strArg(t, "priority")
		if priority == "" {
			priority = "medium"
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO todos (session_id, content, status, priority) VALUES ($1,$2,$3,$4)`,
			sid, content, status, priority); err != nil {
			return 0, err
		}
	}
	return len(todos), tx.Commit(ctx)
}

func (s *server) listTodos(ctx context.Context, sid string) ([]Todo, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, session_id, content, status, priority, created_unix
		 FROM todos WHERE session_id = $1 ORDER BY id`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Todo{}
	for rows.Next() {
		var id int64
		var sessionID, content, status, priority string
		var createdUnix int64
		if err := rows.Scan(&id, &sessionID, &content, &status, &priority, &createdUnix); err != nil {
			return nil, err
		}
		out = append(out, Todo{
			ID:        strconv.FormatInt(id, 10),
			SessionID: sessionID,
			Content:   content,
			Status:    status,
			Priority:  priority,
			Position:  int64(len(out) + 1),
			CreatedAt: time.Unix(createdUnix, 0).UTC().Format(time.RFC3339),
		})
	}
	return out, rows.Err()
}

// ---- tools (NATS) ----

// handlers returns the memory tool handlers. Descriptions/schemas live in
// manifest.yaml (the single declarative protocol source); each handler is
// bound by tool name.
func (s *server) handlers() map[string]extension.ToolSpec {
	return map[string]extension.ToolSpec{
		"todowrite": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				if sessionName == "" {
					return extension.ToolResultData{}, fmt.Errorf("missing session context (session_name)")
				}
				sid := sessionName
				var todos []map[string]interface{}
				if arr, ok := args["todos"].([]interface{}); ok {
					for _, e := range arr {
						if m, ok := e.(map[string]interface{}); ok {
							todos = append(todos, m)
						}
					}
				}
				n, err := s.writeTodos(ctx, sid, todos)
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("todowrite failed: %w", err)
				}
				// Notify live UI listeners over the session SSE stream so the
				// chat page drops its 5s todos polling.
				if s.ext != nil {
					if perr := s.ext.PublishSessionEvent(context.Background(), sid, "todos-updated", map[string]interface{}{"count": n}); perr != nil {
						slog.Warn("todos-updated publish failed", "err", perr)
					}
				}
				return extension.ToolResultData{Content: fmt.Sprintf("Updated %d todo(s).", n), Data: map[string]interface{}{"count": n}}, nil
			},
		},
		"history_search": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				if sessionName == "" {
					return extension.ToolResultData{}, fmt.Errorf("missing session context (session_name)")
				}
				p := searchParams{
					session: sessionName,
					query:   abcprotocol.ArgString(args, "query"),
					from:    abcprotocol.ArgString(args, "from"),
					to:      abcprotocol.ArgString(args, "to"),
					limit:   int(abcprotocol.ArgInt(args, "limit", 50)),
				}
				entries, err := s.searchHistory(ctx, p)
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("history_search failed: %w", err)
				}
				b, _ := json.Marshal(entries)
				summary := fmt.Sprintf("%d matching history message(s).", len(entries))
				return extension.ToolResultData{Content: summary, Data: map[string]interface{}{"count": len(entries), "entries": json.RawMessage(b)}}, nil
			},
		},
		"history_range": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				if sessionName == "" {
					return extension.ToolResultData{}, fmt.Errorf("missing session context (session_name)")
				}
				from := int(abcprotocol.ArgInt(args, "from", 0))
				to := int(abcprotocol.ArgInt(args, "to", 0))
				limit := int(abcprotocol.ArgInt(args, "limit", 200))
				entries, err := s.chainEntries(ctx, sessionName, from, to, limit)
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("history_range failed: %w", err)
				}
				b, _ := json.Marshal(entries)
				summary := fmt.Sprintf("%d history message(s) (depth window [%d,%d)).", len(entries), from, to)
				return extension.ToolResultData{Content: summary, Data: map[string]interface{}{"count": len(entries), "entries": json.RawMessage(b)}}, nil
			},
		},
		"file_info": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				code := abcprotocol.ArgString(args, "code")
				if code == "" {
					return extension.ToolResultData{}, fmt.Errorf("code is required")
				}
				meta, err := s.blobs.Stat(ctx, code)
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("file not found: %s", code)
				}
				b, _ := json.Marshal(meta)
				return extension.ToolResultData{
					Content: fmt.Sprintf("File %s: %s (%s, %d bytes, sha256=%s, uploaded %s)",
						meta.Code, meta.Name, meta.Mime, meta.Size, meta.Sha256, meta.CreatedAt),
					Data: map[string]interface{}{"meta": json.RawMessage(b)},
				}, nil
			},
		},
		"file_read": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				code := abcprotocol.ArgString(args, "code")
				if code == "" {
					return extension.ToolResultData{}, fmt.Errorf("code is required")
				}
				meta, data, err := s.blobs.Get(ctx, code)
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("file not found: %s", code)
				}
				if err := verifySha(meta.Sha256, data); err != nil {
					return extension.ToolResultData{}, fmt.Errorf("checksum mismatch")
				}
				offset := int(abcprotocol.ArgInt(args, "offset", 1))
				limit := int(abcprotocol.ArgInt(args, "limit", 200))
				text := string(data)
				if !looksText(data) {
					return extension.ToolResultData{
						Content: fmt.Sprintf("File %s is binary (%s, %d bytes); content not readable as text.", meta.Code, meta.Mime, meta.Size),
						Data:    map[string]interface{}{"binary": true, "size": meta.Size, "mime": meta.Mime},
					}, nil
				}
				lines := strings.Split(text, "\n")
				if offset < 1 {
					offset = 1
				}
				if offset > len(lines) {
					offset = len(lines)
				}
				end := offset - 1 + limit
				if end > len(lines) {
					end = len(lines)
				}
				if offset-1 > len(lines) {
					return extension.ToolResultData{Content: "Empty or out-of-range.", Data: map[string]interface{}{"total_lines": len(lines)}}, nil
				}
				body := strings.Join(lines[offset-1:end], "\n")
				return extension.ToolResultData{
					Content: fmt.Sprintf("File %s (%s, %d bytes):\n%s", meta.Code, meta.Mime, meta.Size, body),
					Data:    map[string]interface{}{"total_lines": len(lines), "shown": end - (offset - 1)},
				}, nil
			},
		},
		"file_url": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				code := abcprotocol.ArgString(args, "code")
				if code == "" {
					return extension.ToolResultData{}, fmt.Errorf("code is required")
				}
				if _, err := s.blobs.Stat(ctx, code); err != nil {
					return extension.ToolResultData{}, fmt.Errorf("file not found: %s", code)
				}
				ttl := time.Duration(envInt("FILES_PRESIGN_TTL_SECS", 900)) * time.Second
				url, err := s.blobs.Presign(ctx, code, ttl)
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("presign not supported: %w", err)
				}
				return extension.ToolResultData{
					Content: fmt.Sprintf("Direct download URL (valid %d s): %s", int(ttl.Seconds()), url),
					Data:    map[string]interface{}{"url": url, "ttl_secs": int(ttl.Seconds())},
				}, nil
			},
		},
	}
}

// ---- HTTP ----

func (s *server) router() http.Handler {
	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "name": "memory-extension"})
		})
		r.Get("/todos", s.listTodosHTTP)
		r.Post("/todos", s.writeTodosHTTP)
		r.Get("/history/search", s.searchHTTP)
		r.Get("/history/range", s.rangeHTTP)
		r.Post("/files", s.uploadFile)
		r.Get("/files/{code}", s.getFile)
		r.Get("/files/{code}/meta", s.fileMeta)
		r.Post("/files/{code}/presign", s.filePresign)
	})
	return r
}

func (s *server) searchHTTP(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("session")
	if sid == "" {
		sid = r.URL.Query().Get("session_id")
	}
	if sid == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "session required"})
		return
	}
	limit := atoiDefault(r.URL.Query().Get("limit"), 50)
	entries, err := s.searchHistory(r.Context(), searchParams{
		session: sid,
		query:   r.URL.Query().Get("query"),
		from:    r.URL.Query().Get("from"),
		to:      r.URL.Query().Get("to"),
		limit:   limit,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"entries": entries})
}

func (s *server) rangeHTTP(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("session")
	if sid == "" {
		sid = r.URL.Query().Get("session_id")
	}
	if sid == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "session required"})
		return
	}
	from := atoiDefault(r.URL.Query().Get("from"), 0)
	to := atoiDefault(r.URL.Query().Get("to"), 0)
	limit := atoiDefault(r.URL.Query().Get("limit"), 200)
	entries, err := s.chainEntries(r.Context(), sid, from, to, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"entries": entries})
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func (s *server) listTodosHTTP(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("session_id")
	if sid == "" {
		sid = "default"
	}
	todos, err := s.listTodos(r.Context(), sid)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"todos": todos})
}

func (s *server) writeTodosHTTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID  string                   `json:"session_id"`
		SessionID2 string                   `json:"sessionId"`
		Todos      []map[string]interface{} `json:"todos"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "invalid body"})
		return
	}
	sid := body.SessionID
	if sid == "" {
		sid = body.SessionID2
	}
	if sid == "" {
		sid = "default"
	}
	n, err := s.writeTodos(r.Context(), sid, body.Todos)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "count": n})
}

// ---- helpers ----

// ---- file storage ----

// uploadFile handles an S3-style multipart upload. It computes the content
// sha256, dedups against the index, and on a new file stores bytes + metadata
// in the blob store and records the sha256→code mapping in the index.
func (s *server) uploadFile(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "invalid multipart: " + err.Error()})
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "file field required"})
		return
	}
	defer file.Close()

	buf, err := readAll(file)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	if len(buf) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "empty file"})
		return
	}

	hash := sha256Hex(buf)
	uploader := r.FormValue("uploader_session")

	// Dedup: reuse the existing code for the same content.
	if code, ok := s.codeForHash(r.Context(), hash); ok {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"ok": true, "code": code, "hash": hash, "name": header.Filename,
			"mime": header.Header.Get("Content-Type"),
			"size": len(buf), "deduped": true,
		})
		return
	}

	// New file: mint a random object key and persist.
	code := randomCode(defaultCodeLength)
	meta := FileMeta{
		Code:            code,
		Name:            header.Filename,
		Mime:            header.Header.Get("Content-Type"),
		Size:            int64(len(buf)),
		UploaderSession: uploader,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
		Sha256:          hash,
	}
	if err := s.blobs.Put(r.Context(), meta, buf); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": "storage put failed: " + err.Error()})
		return
	}
	if err := s.indexCode(r.Context(), hash, code); err != nil {
		// Best-effort: the object exists but the index write failed; still
		// return the code so the uploader can use it, but flag it.
		slog.Warn("file_dedup index failed", "hash", hash, "code", code, "err", err)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok": true, "code": code, "hash": hash, "name": header.Filename,
		"mime": header.Header.Get("Content-Type"),
		"size": len(buf), "deduped": false,
	})
}

// getFile streams the object bytes with its metadata. It is the frontend's
// download/preview endpoint (e.g. rendering an image thumbnail).
func (s *server) getFile(w http.ResponseWriter, r *http.Request) {
	code := chi.URLParam(r, "code")
	meta, data, err := s.blobs.Get(r.Context(), code)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"ok": false, "error": "file not found"})
		return
	}
	if err := verifySha(meta.Sha256, data); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": "checksum mismatch"})
		return
	}
	ct := meta.Mime
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	if meta.Name != "" {
		w.Header().Set("Content-Disposition", "inline; filename=\""+sanitizeFilename(meta.Name)+"\"")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// fileMeta returns only the object metadata (no bytes).
func (s *server) fileMeta(w http.ResponseWriter, r *http.Request) {
	code := chi.URLParam(r, "code")
	meta, err := s.blobs.Stat(r.Context(), code)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"ok": false, "error": "file not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "meta": meta})
}

// filePresign mints a short-lived presigned GET URL (S3 backend only). It is
// used to hand a sandbox / worker a direct, credential-less download of a
// large file. The local backend has no presign support and returns an error.
func (s *server) filePresign(w http.ResponseWriter, r *http.Request) {
	code := chi.URLParam(r, "code")
	ttl := time.Duration(envInt("FILES_PRESIGN_TTL_SECS", 900)) * time.Second
	if _, err := s.blobs.Stat(r.Context(), code); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"ok": false, "error": "file not found"})
		return
	}
	url, err := s.blobs.Presign(r.Context(), code, ttl)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "presign not supported"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "url": url, "ttl_secs": int64(ttl.Seconds())})
}

// ---- file index (sha256 → code) ----

func (s *server) codeForHash(ctx context.Context, hash string) (string, bool) {
	var code string
	err := s.pool.QueryRow(ctx, `SELECT code FROM file_dedup WHERE sha256 = $1`, hash).Scan(&code)
	if err != nil {
		return "", false
	}
	return code, true
}

func (s *server) indexCode(ctx context.Context, hash, code string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO file_dedup (sha256, code) VALUES ($1, $2)
		 ON CONFLICT (sha256) DO NOTHING`, hash, code)
	return err
}

// ---- helpers ----

func readAll(f interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func verifySha(expect string, data []byte) error {
	if expect == "" {
		return nil
	}
	if sha256Hex(data) != expect {
		return fmt.Errorf("sha256 mismatch")
	}
	return nil
}

func looksText(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	if len(b) > 1<<20 {
		// Only sniff the head of large files; treat oversized tails as binary.
		b = b[:1<<20]
	}
	for _, c := range b {
		if c == 0 {
			return false
		}
	}
	return true
}

func sanitizeFilename(name string) string {
	name = strings.Map(func(r rune) rune {
		if r == '"' || r == '\\' || r < 32 {
			return '_'
		}
		return r
	}, name)
	if name == "" {
		return "file"
	}
	return name
}

func strArg(m map[string]interface{}, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}
