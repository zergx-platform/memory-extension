package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zergx-platform/memory-extension/internal/env"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool connects to the temp-namespace Postgres (gate by MEMORY_TEST_PG).
// The agent DB is shared with the live agent, so tests use an isolated table
// suffix pattern via a dedicated schema-less approach: they TRUNCATE todos
// before/after and only touch rows for a synthetic session id.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if os.Getenv("MEMORY_TEST_PG") != "1" {
		t.Skip("set MEMORY_TEST_PG=1 to run PG-backed tests")
	}
	cfg := PgConfig{
		Host:     env.Or("MEMORY_TEST_PG_HOST", "postgres.zergx.svc.cluster.local"),
		Port:     env.NormalizePort(env.Or("MEMORY_TEST_PG_PORT", "5432")),
		User:     env.Or("MEMORY_TEST_PG_USER", "root"),
		Password: env.Or("MEMORY_TEST_PG_PASSWORD", "devpassword"),
		DB:       env.Or("MEMORY_TEST_PG_DB", "zergx_agent"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := OpenPool(ctx, cfg)
	if err != nil {
		t.Skipf("pg unavailable: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM todos WHERE session_id LIKE 'memtest-%'`)
		pool.Close()
	})
	return pool
}

func newServer(t *testing.T, pool *pgxpool.Pool) *server {
	t.Helper()
	return &server{pool: pool}
}

func do(t *testing.T, h http.Handler, method, path string, body string) (int, map[string]interface{}) {
	t.Helper()
	var rd *strings.Reader
	if body != "" {
		rd = strings.NewReader(body)
	} else {
		rd = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, rd)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var v map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&v)
	return rec.Code, v
}

func TestHealth(t *testing.T) {
	s := newServer(t, testPool(t))
	code, v := do(t, s.router(), "GET", "/api/v1/health", "")
	if code != 200 || v["name"] != "memory-extension" {
		t.Fatalf("health = %d %v", code, v)
	}
}

func TestTodosWriteListRoundtrip(t *testing.T) {
	s := newServer(t, testPool(t))
	h := s.router()
	sid := "memtest-" + fmt.Sprint(time.Now().UnixNano())

	// write
	code, v := do(t, h, "POST", "/api/v1/todos",
		`{"session_id":"`+sid+`","todos":[
			{"content":"first","status":"pending","priority":"high"},
			{"content":"second","status":"in_progress","priority":"medium"}
		]}`)
	if code != 200 || v["count"] != float64(2) {
		t.Fatalf("write = %d %v", code, v)
	}

	// list — UI contract shape
	code, v = do(t, h, "GET", "/api/v1/todos?session_id="+sid, "")
	if code != 200 {
		t.Fatalf("list = %d %v", code, v)
	}
	todos := v["todos"].([]interface{})
	if len(todos) != 2 {
		t.Fatalf("todos len = %d", len(todos))
	}
	first := todos[0].(map[string]interface{})
	for _, k := range []string{"id", "sessionId", "content", "status", "priority", "position", "createdAt"} {
		if _, ok := first[k]; !ok {
			t.Fatalf("todo missing UI field %q: %v", k, first)
		}
	}
	if first["sessionId"] != sid || first["content"] != "first" {
		t.Fatalf("first todo = %v", first)
	}

	// replace (atomic delete+insert)
	code, _ = do(t, h, "POST", "/api/v1/todos",
		`{"session_id":"`+sid+`","todos":[{"content":"only","status":"completed","priority":"low"}]}`)
	if code != 200 {
		t.Fatalf("replace = %d", code)
	}
	_, v = do(t, h, "GET", "/api/v1/todos?session_id="+sid, "")
	if len(v["todos"].([]interface{})) != 1 {
		t.Fatalf("replace should yield 1 todo: %v", v)
	}
}

func TestTodosDefaultSession(t *testing.T) {
	s := newServer(t, testPool(t))
	h := s.router()
	code, v := do(t, h, "GET", "/api/v1/todos", "")
	if code != 200 || v["todos"] == nil {
		t.Fatalf("default session list = %d %v", code, v)
	}
}

func TestTodowriteTool(t *testing.T) {
	s := newServer(t, testPool(t))
	sid := "memtest-" + fmt.Sprint(time.Now().UnixNano())
	tools := s.handlers()
	spec, ok := tools["todowrite"]
	if !ok {
		t.Fatal("todowrite tool missing")
	}
	res, err := spec.Execute(context.Background(), map[string]interface{}{
		"todos": []interface{}{
			map[string]interface{}{"content": "x", "status": "pending", "priority": "high"},
		},
	}, "call-1", sid)
	if err != nil {
		t.Fatalf("execute = %v", err)
	}
	if !strings.Contains(res.Content, "1") {
		t.Fatalf("content = %q", res.Content)
	}
	meta, _ := res.Data.(map[string]interface{})
	if meta["count"] != 1 {
		t.Fatalf("meta = %v", meta)
	}
	// verify persisted
	todos, _ := s.listTodos(context.Background(), sid)
	if len(todos) != 1 || todos[0].Content != "x" {
		t.Fatalf("list after tool = %+v", todos)
	}
}

func TestHistorySearchKeyword(t *testing.T) {
	s := newServer(t, testPool(t))
	sid := "memtest-" + fmt.Sprint(time.Now().UnixNano())

	// Build a small chain in the agent tables directly (sessions + messages).
	ctx := context.Background()
	var tip string
	s.pool.QueryRow(ctx, `INSERT INTO sessions (name, model, preset, created_at, updated_at)
		VALUES ($1,'','','2026-01-01 00:00:00','2026-01-01 00:00:00')
		RETURNING tip_id`, sid).Scan(&tip)

	// Insert 3 messages chained: m1 (oldest) -> m2 -> m3 (tip).
	m1 := "m-" + fmt.Sprint(time.Now().UnixNano()) + "-1"
	m2 := "m-" + fmt.Sprint(time.Now().UnixNano()) + "-2"
	m3 := "m-" + fmt.Sprint(time.Now().UnixNano()) + "-3"
	_, _ = s.pool.Exec(ctx, `INSERT INTO messages (id, role, prev_id, created_at) VALUES
		($1,'user','','2026-01-01 00:00:01'),
		($2,'assistant','','2026-01-01 00:00:02'),
		($3,'assistant','','2026-01-01 00:00:03')`, m1, m2, m3)
	_, _ = s.pool.Exec(ctx, `UPDATE messages SET prev_id = $1 WHERE id = $2`, m2, m3)
	_, _ = s.pool.Exec(ctx, `UPDATE messages SET prev_id = $1 WHERE id = $2`, m1, m2)
	_, _ = s.pool.Exec(ctx, `UPDATE sessions SET tip_id = $1 WHERE name = $2`, m3, sid)
	// Text lives in parts(type=text).
	_, _ = s.pool.Exec(ctx, `INSERT INTO parts (id, message_id, type, seq, data) VALUES
		($1,$2,'text',0,'{"text":"hello world"}'),
		($3,$4,'text',0,'{"text":"I wrote a parser"}'),
		($5,$6,'text',0,'{"text":"finalized the code"}')`,
		"p1-"+m1, m1, "p2-"+m2, m2, "p3-"+m3, m3)

	// keyword hit on the middle message
	entries, err := s.searchHistory(ctx, searchParams{session: sid, query: "parser", limit: 10})
	if err != nil {
		t.Fatalf("search = %v", err)
	}
	if len(entries) != 1 || entries[0].Content != "I wrote a parser" {
		t.Fatalf("search entries = %+v", entries)
	}
	// depth: m3 is tip=0, m2=1
	if entries[0].Depth != 1 {
		t.Fatalf("depth = %d, want 1", entries[0].Depth)
	}
	// time window excludes the hit
	entries, _ = s.searchHistory(ctx, searchParams{session: sid, query: "parser", from: "2026-01-01 00:00:03", limit: 10})
	if len(entries) != 0 {
		t.Fatalf("time window should exclude: %+v", entries)
	}
}

func TestHistoryRangeDepth(t *testing.T) {
	s := newServer(t, testPool(t))
	sid := "memtest-" + fmt.Sprint(time.Now().UnixNano())
	ctx := context.Background()

	var tip string
	s.pool.QueryRow(ctx, `INSERT INTO sessions (name, model, preset, created_at, updated_at)
		VALUES ($1,'','','2026-01-01 00:00:00','2026-01-01 00:00:00')
		RETURNING tip_id`, sid).Scan(&tip)

	m1 := "r-" + fmt.Sprint(time.Now().UnixNano()) + "-1"
	m2 := "r-" + fmt.Sprint(time.Now().UnixNano()) + "-2"
	_, _ = s.pool.Exec(ctx, `INSERT INTO messages (id, role, prev_id, created_at) VALUES
		($1,'user','','2026-01-01 00:00:01'),
		($2,'user','','2026-01-01 00:00:02')`, m1, m2)
	_, _ = s.pool.Exec(ctx, `UPDATE messages SET prev_id = $1 WHERE id = $2`, m1, m2)
	_, _ = s.pool.Exec(ctx, `UPDATE sessions SET tip_id = $1 WHERE name = $2`, m2, sid)
	_, _ = s.pool.Exec(ctx, `INSERT INTO parts (id, message_id, type, seq, data) VALUES
		($1,$2,'text',0,'{"text":"first"}'),
		($3,$4,'text',0,'{"text":"second"}')`,
		"pr1-"+m1, m1, "pr2-"+m2, m2)

	// depth window [0,1): the newest message only
	entries, err := s.chainEntries(ctx, sid, 0, 1, 100)
	if err != nil {
		t.Fatalf("range = %v", err)
	}
	if len(entries) != 1 || entries[0].Content != "second" {
		t.Fatalf("range entries = %+v", entries)
	}
	if entries[0].Depth != 0 {
		t.Fatalf("newest depth = %d, want 0", entries[0].Depth)
	}
	// [1,2): the older message
	entries, _ = s.chainEntries(ctx, sid, 1, 2, 100)
	if len(entries) != 1 || entries[0].Content != "first" || entries[0].Depth != 1 {
		t.Fatalf("older range = %+v", entries)
	}
}
