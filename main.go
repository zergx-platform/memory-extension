package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	extensionsdk "forgejo.develop.10.199.64.20.nip.io/rucoder/extension-sdk-go"
)

// memory-extension replaces the Rust memory-tools service: it persists the
// session todo list in Postgres and exposes it both as a NATS tool
// (`todowrite`) and over HTTP (`GET/POST /api/v1/todos`).
//
// The HTTP list endpoint answers the UI contract (sessionId/position/
// createdAt), mapping the legacy table columns (session_id/created_unix)
// on the way out; the underlying table stays compatible with the Rust
// version's schema so no data migration is required.

type server struct {
	pool *pgxpool.Pool
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := OpenPool(ctx, pgConfig())
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	s := &server{pool: pool}

	if err := extensionsdk.Serve(
		extensionsdk.Config{
			ID:      "memory-extension",
			Version: "0.1.0",
			NATSURL: envOr("NATS_URL", "nats://nats.develop.svc.cluster.local:4222"),
			Tools:   s.tools(),
		},
		extensionsdk.ServeOptions{Handler: s.router()},
	); err != nil {
		panic(err)
	}
}

// ---- pg ----

type PgConfig struct {
	Host, Port, User, Password, DB string
}

func pgConfig() PgConfig {
	return PgConfig{
		Host:     envOr("POSTGRES_HOST", "postgres.develop.svc.cluster.local"),
		Port:     normalizePort(envOr("POSTGRES_PORT", "5432")),
		User:     envOr("POSTGRES_USER", "root"),
		Password: envOr("POSTGRES_PASSWORD", "devpassword"),
		DB:       envOr("POSTGRES_DB_AGENT", "rucoder_agent"),
	}
}

func normalizePort(v string) string {
	if i := strings.LastIndexByte(v, ':'); i >= 0 {
		if c := v[i+1:]; c != "" {
			return c
		}
	}
	return v
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
		CREATE INDEX IF NOT EXISTS idx_todos_session ON todos (session_id);`)
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
			Position:  0,
			CreatedAt: strconv.FormatInt(createdUnix, 10),
		})
	}
	return out, rows.Err()
}

// ---- tools (NATS) ----

func (s *server) tools() map[string]extensionsdk.ToolSpec {
	return map[string]extensionsdk.ToolSpec{
		"todowrite": {
			Description: "Replace the session's todo list (stored in the database, not a file).",
			InputSchema: extensionsdk.Schema(map[string]interface{}{
				"todos": map[string]interface{}{
					"type": "array",
					"items": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"content":  map[string]interface{}{"type": "string"},
							"status":   map[string]interface{}{"type": "string", "enum": []string{"pending", "in_progress", "completed", "cancelled"}},
							"priority": map[string]interface{}{"type": "string", "enum": []string{"high", "medium", "low"}},
						},
						"required": []string{"content", "status", "priority"},
					},
				},
			}, "todos"),
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, map[string]interface{}, error) {
				sid := extensionsdk.ArgString(args, "_session")
				if sid == "" {
					sid = extensionsdk.ArgString(args, "_session_id")
				}
				if sid == "" {
					sid = "default"
				}
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
					return "", nil, fmt.Errorf("todowrite failed: %w", err)
				}
				return fmt.Sprintf("Updated %d todo(s).", n), map[string]interface{}{"count": n}, nil
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
	})
	return r
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
		SessionID string                   `json:"session_id"`
		Todos     []map[string]interface{} `json:"todos"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "invalid body"})
		return
	}
	sid := body.SessionID
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

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func strArg(m map[string]interface{}, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
