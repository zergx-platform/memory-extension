package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"strings"

	"golang.org/x/image/draw"
)

// FileMeta mirrors the agent's file metadata (returned by GET /files/{code} and
// GET /files/{code}/meta). The agent owns storage; this extension only reads it.
type FileMeta struct {
	Code            string `json:"code"`
	Name            string `json:"name"`
	Mime            string `json:"mime"`
	Size            int64  `json:"size"`
	UploaderSession string `json:"uploader_session,omitempty"`
	CreatedAt       string `json:"created_at"`
	Sha256          string `json:"sha256"`
}

// resizeToDataURL downsizes an already-decoded image to at most maxDim px on
// its longest edge (preserving aspect ratio), re-encodes it and returns a
// `data:<mime>;base64,<...>` URL suitable for an OpenAI-style VLM call.
// Never ships the raw bytes to the model.
func resizeToDataURL(data []byte, mime string, maxDim, jpegQuality int) (string, error) {
	img, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	orig := img.Bounds()
	w, h := orig.Dx(), orig.Dy()
	if maxDim <= 0 {
		maxDim = 1024
	}
	if w > maxDim || h > maxDim {
		if w >= h {
			h = h * maxDim / w
			w = maxDim
		} else {
			w = w * maxDim / h
			h = maxDim
		}
		dst := image.NewRGBA(image.Rect(0, 0, w, h))
		draw.CatmullRom.Scale(dst, dst.Bounds(), img, orig.Bounds(), draw.Over, nil)
		img = dst
	}
	outMime := "image/jpeg"
	if strings.Contains(strings.ToLower(mime), "png") {
		outMime = "image/png"
	}
	if format == "png" {
		outMime = "image/png"
	}
	var buf bytes.Buffer
	if outMime == "image/png" {
		if err := png.Encode(&buf, img); err != nil {
			return "", err
		}
	} else {
		if jpegQuality <= 0 {
			jpegQuality = 85
		}
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: jpegQuality}); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("data:%s;base64,%s", outMime, base64.StdEncoding.EncodeToString(buf.Bytes())), nil
}
