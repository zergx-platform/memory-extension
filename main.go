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
	pool *pgxpool.Pool
	ext  *extension.Extension
}

// vlmModel resolves the configured vision model for a session (session >
// global > default), from the extension config store.
func (s *server) vlmModel(sessionName string) string {
	if s.ext == nil {
		return ""
	}
	if v, ok := s.ext.GetConfig("vlm_model", sessionName).(string); ok {
		return v
	}
	return ""
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
			Position:  int64(len(out) + 1),
			CreatedAt: time.Unix(createdUnix, 0).UTC().Format(time.RFC3339),
		})
	}
	return out, rows.Err()
}

// ---- helpers ----

// renderHistoryList turns history entries into a model-visible, readable list
// so the agent actively sees each matched message (role, tool, change id,
// time, truncated content) rather than only a count. `Data.entries` keeps the
// structured form for the UI. Per-entry text is capped (200 runes).
func renderHistoryList(locale string, entries []historyEntry) string {
	if len(entries) == 0 {
		return "history_search: no matching messages."
	}
	var sb strings.Builder
	label := "history"
	if locale == "zh" {
		label = "历史"
	}
	fmt.Fprintf(&sb, "%s %d 条：\n", label, len(entries))
	for i, e := range entries {
		role := e.Role
		if role == "" {
			role = "?"
		}
		meta := fmt.Sprintf("[%d] %s (depth %d)", i, role, e.Depth)
		if e.CreatedAt != "" {
			meta += " " + e.CreatedAt
		}
		if e.ToolName != "" {
			meta += " tool=" + e.ToolName
		}
		if e.ChangeID != "" {
			meta += " change=" + e.ChangeID
		}
		body := e.Content
		if runes := []rune(body); len(runes) > 200 {
			body = string(runes[:200]) + "…"
		}
		fmt.Fprintf(&sb, "%s\n", meta)
		if body != "" {
			fmt.Fprintf(&sb, "    %s\n", body)
		}
	}
	return sb.String()
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
				loc := localeOf(ctx, s.ext, sessionName, envOr("ZERGX_LOCALE", "en"))
				content := t(loc, fmt.Sprintf("Updated %d todo(s).", n), fmt.Sprintf("已更新 %d 条待办。", n))
				return extension.ToolResultData{Content: content, Data: map[string]interface{}{"count": n}}, nil
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
				loc := localeOf(ctx, s.ext, sessionName, envOr("ZERGX_LOCALE", "en"))
				content := renderHistoryList(loc, entries)
				return extension.ToolResultData{Content: content, Data: map[string]interface{}{"count": len(entries), "entries": json.RawMessage(b)}}, nil
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
				loc := localeOf(ctx, s.ext, sessionName, envOr("ZERGX_LOCALE", "en"))
				content := renderHistoryList(loc, entries)
				return extension.ToolResultData{Content: content, Data: map[string]interface{}{"count": len(entries), "entries": json.RawMessage(b)}}, nil
			},
		},
		"file_info": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				code := abcprotocol.ArgString(args, "code")
				if code == "" {
					return extension.ToolResultData{}, fmt.Errorf("code is required")
				}
				meta, err := s.fileMetaFromAgent(ctx, code)
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("file not found: %s", code)
				}
				b, _ := json.Marshal(meta)
				loc := localeOf(ctx, s.ext, sessionName, envOr("ZERGX_LOCALE", "en"))
				content := t(loc,
					fmt.Sprintf("File %s: %s (%s, %d bytes, sha256=%s, uploaded %s)",
						meta.Code, meta.Name, meta.Mime, meta.Size, meta.Sha256, meta.CreatedAt),
					fmt.Sprintf("文件 %s：%s（%s，%d 字节，sha256=%s，上传于 %s）",
						meta.Code, meta.Name, meta.Mime, meta.Size, meta.Sha256, meta.CreatedAt))
				return extension.ToolResultData{
					Content: content,
					Data:    map[string]interface{}{"meta": json.RawMessage(b)},
				}, nil
			},
		},
		"image_read": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				code := abcprotocol.ArgString(args, "code")
				if code == "" {
					return extension.ToolResultData{}, fmt.Errorf("code is required")
				}
				prompt := abcprotocol.ArgString(args, "prompt")
				if prompt == "" {
					prompt = "Describe this image"
				}
				meta, bytes, err := s.fileBytesFromAgent(ctx, code)
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("file not found: %s", code)
				}
				if !strings.HasPrefix(meta.Mime, "image/") {
					return extension.ToolResultData{}, fmt.Errorf("file %s is not an image (%s)", meta.Code, meta.Mime)
				}
				// Downscale before sending so oversized images stay within the
				// VLM input budget (never ship the raw bytes).
				b64, err := resizeToDataURL(bytes, meta.Mime, envInt("IMAGE_READ_MAX_DIM", 1024), envInt("IMAGE_READ_JPEG_QUALITY", 85))
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("image resize failed: %w", err)
				}
				text, err := s.vlmChat(ctx, prompt, b64)
				if err != nil {
					return extension.ToolResultData{}, fmt.Errorf("image_read failed: %w", err)
				}
				return extension.ToolResultData{Content: text, Data: map[string]interface{}{
					"model": s.vlmModel(sessionName),
					"code":  meta.Code, "mime": meta.Mime, "size": meta.Size,
				}}, nil
			},
		},
	}
}

// vlmChat performs a single-turn VLM completion by delegating to the agent's
// OpenAI-compatible `POST /api/v1/llm/chat/completions`. The agent resolves
// the `provider_id/model_id` reference against its registered providers, so
// the extension never holds base_url/api_key. The model comes from the
// extension config knob 'vlm_model' (session > global > default), falling
// back to the env vars for backward compatibility.
func (s *server) vlmChat(ctx context.Context, prompt, imageDataURL string) (string, error) {
	agentURL := envOr("AGENT_BASE_URL", envOr("ZERGX_AGENT_URL", "http://agent.zergx.svc.cluster.local:80"))
	model := s.vlmModel("")
	if model == "" {
		// Prefer the provider most likely to be vision-capable, else the
		// default model ref. Overridable via env throughout.
		model = envOr("IMAGE_READ_MODEL", envOr("ZERGX_LLM_MODEL", "deepseek-v4-pro"))
	}
	body := map[string]interface{}{
		"model": model,
		"messages": []map[string]interface{}{
			{
				"role": "user",
				"content": []map[string]interface{}{
					{"type": "text", "text": prompt},
					{"type": "image", "image": imageDataURL},
				},
			},
		},
	}
	payload, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(agentURL, "/")+"/api/v1/llm/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok := envOr("AGENT_API_KEY", ""); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("agent LLM endpoint HTTP %d: %.200s", resp.StatusCode, respBody)
	}
	var envelope struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return "", fmt.Errorf("decode agent response: %w", err)
	}
	if len(envelope.Choices) == 0 {
		return "", fmt.Errorf("agent returned no choices")
	}
	return envelope.Choices[0].Message.Content, nil
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

// fileMetaFromAgent fetches a file's metadata from the agent's files API.
func (s *server) fileMetaFromAgent(ctx context.Context, code string) (FileMeta, error) {
	agentURL := envOr("AGENT_BASE_URL", envOr("ZERGX_AGENT_URL", "http://agent.zergx.svc.cluster.local:80"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(agentURL, "/")+"/api/v1/files/"+code+"/meta", nil)
	if err != nil {
		return FileMeta{}, err
	}
	if tok := envOr("AGENT_API_KEY", ""); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return FileMeta{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return FileMeta{}, fmt.Errorf("agent files API HTTP %d", resp.StatusCode)
	}
	var meta FileMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return FileMeta{}, err
	}
	return meta, nil
}

// fileBytesFromAgent fetches a file's bytes + metadata from the agent's files API.
func (s *server) fileBytesFromAgent(ctx context.Context, code string) (FileMeta, []byte, error) {
	agentURL := envOr("AGENT_BASE_URL", envOr("ZERGX_AGENT_URL", "http://agent.zergx.svc.cluster.local:80"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(agentURL, "/")+"/api/v1/files/"+code, nil)
	if err != nil {
		return FileMeta{}, nil, err
	}
	if tok := envOr("AGENT_API_KEY", ""); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return FileMeta{}, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return FileMeta{}, nil, fmt.Errorf("agent files API HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return FileMeta{}, nil, err
	}
	meta := FileMeta{Code: code, Mime: resp.Header.Get("Content-Type"), Size: int64(len(data))}
	// Prefer metadata from the meta endpoint when available.
	if m, merr := s.fileMetaFromAgent(ctx, code); merr == nil {
		meta = m
	}
	return meta, data, nil
}

func strArg(m map[string]interface{}, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}
