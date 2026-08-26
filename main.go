package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	abep "abep.dev/sdk"
	natsbus "abep.dev/sdk/nats"
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
	log := slog.Default().With("svc", "memory-extension")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := OpenPool(ctx, pgConfig())
	if err != nil {
		log.Error("pg connect failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	s := &server{pool: pool}

	nbus, err := natsbus.Connect(envOr("NATS_URL", "nats://nats.develop.svc.cluster.local:4222"))
	if err != nil {
		log.Error("nats connect failed", "err", err)
		os.Exit(1)
	}
	manifest, err := abep.ParseManifest(manifestYaml)
	if err != nil {
		log.Error("load manifest failed", "err", err)
		os.Exit(1)
	}
	if err := abep.Serve(
		nbus,
		manifest.Config(s.handlers(), nil, nil),
		abep.ServeOptions{Handler: s.router()},
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

// handlers returns the memory tool handlers. Descriptions/schemas live in
// manifest.yaml (the single declarative protocol source); each handler is
// bound by tool name.
func (s *server) handlers() map[string]abep.ToolSpec {
	return map[string]abep.ToolSpec{
		"todowrite": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string, _ func(string)) (string, map[string]interface{}, error) {
				sid := sessionName
				if sid == "" {
					sid = abep.ArgString(args, "_session")
				}
				if sid == "" {
					sid = abep.ArgString(args, "_session_id")
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
		"history_search": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string, _ func(string)) (string, map[string]interface{}, error) {
				sid := sessionName
				if sid == "" {
					sid = abep.ArgString(args, "_session")
				}
				if sid == "" {
					sid = abep.ArgString(args, "_session_id")
				}
				if sid == "" {
					return "", nil, fmt.Errorf("缺少会话上下文（_session）")
				}
				p := searchParams{
					session: sid,
					query:   abep.ArgString(args, "query"),
					from:    abep.ArgString(args, "from"),
					to:      abep.ArgString(args, "to"),
					limit:   int(abep.ArgInt(args, "limit", 50)),
				}
				entries, err := s.searchHistory(ctx, p)
				if err != nil {
					return "", nil, fmt.Errorf("history_search failed: %w", err)
				}
				b, _ := json.Marshal(entries)
				summary := fmt.Sprintf("%d 条匹配的历史消息。", len(entries))
				return summary, map[string]interface{}{"count": len(entries), "entries": json.RawMessage(b)}, nil
			},
		},
		"history_range": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string, _ func(string)) (string, map[string]interface{}, error) {
				sid := sessionName
				if sid == "" {
					sid = abep.ArgString(args, "_session")
				}
				if sid == "" {
					sid = abep.ArgString(args, "_session_id")
				}
				if sid == "" {
					return "", nil, fmt.Errorf("缺少会话上下文（_session）")
				}
				from := int(abep.ArgInt(args, "from", 0))
				to := int(abep.ArgInt(args, "to", 0))
				limit := int(abep.ArgInt(args, "limit", 200))
				entries, err := s.chainEntries(ctx, sid, from, to, limit)
				if err != nil {
					return "", nil, fmt.Errorf("history_range failed: %w", err)
				}
				b, _ := json.Marshal(entries)
				summary := fmt.Sprintf("%d 条历史消息（depth 窗口 [%d,%d)）。", len(entries), from, to)
				return summary, map[string]interface{}{"count": len(entries), "entries": json.RawMessage(b)}, nil
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
