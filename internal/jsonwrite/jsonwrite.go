// Package jsonwrite centralizes HTTP JSON response helpers.
package jsonwrite

import (
	"encoding/json"
	"net/http"
)

// JSON writes v as a JSON response with the given status code.
func JSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Err writes the canonical error envelope {"ok":false,"error":msg}.
func Err(w http.ResponseWriter, code int, msg string) {
	JSON(w, code, map[string]any{"ok": false, "error": msg})
}
