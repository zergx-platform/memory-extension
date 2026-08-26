package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	abep "abep.dev/sdk"
)

// ---- manifest binding ----

// TestManifestBinding asserts the YAML manifest is the single source of
// truth: every declared tool has a bound handler, every handler is declared,
// and descriptions/schemas are carried onto the wire config.
func TestManifestBinding(t *testing.T) {
	m, err := abep.ParseManifest(manifestYaml)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if m.ID != "memory" {
		t.Fatalf("manifest id = %q", m.ID)
	}
	if len(m.Tools) != 3 {
		t.Fatalf("manifest tools = %d, want 3", len(m.Tools))
	}

	cfg := m.Config(manifestHandlers(), nil, nil)
	if cfg.ID != "memory" {
		t.Fatalf("cfg id = %q", cfg.ID)
	}

	declared := map[string]bool{}
	for _, tool := range m.Tools {
		if strings.TrimSpace(tool.Description) == "" {
			t.Fatalf("tool %q has empty description", tool.Name)
		}
		if tool.InputSchema == nil {
			t.Fatalf("tool %q has no input_schema", tool.Name)
		}
		declared[tool.Name] = true
		if _, ok := cfg.Tools[tool.Name]; !ok {
			t.Fatalf("tool %q declared but no handler bound", tool.Name)
		}
	}

	for name := range manifestHandlers() {
		if !declared[name] {
			t.Fatalf("handler %q bound but not declared in manifest", name)
		}
	}
	if len(cfg.Tools) != len(declared) {
		t.Fatalf("cfg tools = %d, declared = %d", len(cfg.Tools), len(declared))
	}
}

// manifestHandlers returns the handler map without needing a live server.
func manifestHandlers() map[string]abep.ToolSpec {
	return (&server{}).handlers()
}

// ---- wire-level e2e over the inproc transport ----

// startExt serves the extension on the hub and waits for subscription setup.
func startExt(t *testing.T, hub *abep.InprocHub, cfg abep.Config) (*abep.Agent, *abep.Extension) {
	t.Helper()
	ext := abep.NewExtension(abep.NewInprocBus(hub), cfg)
	agent := abep.NewAgent(abep.NewInprocBus(hub))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = ext.Serve(ctx) }()
	time.Sleep(100 * time.Millisecond)
	return agent, ext
}

func TestDiscoverWire(t *testing.T) {
	hub := abep.NewInprocHub()
	m, err := abep.ParseManifest(manifestYaml)
	if err != nil {
		t.Fatal(err)
	}
	agent, ext := startExt(t, hub, m.Config(manifestHandlers(), nil, nil))
	defer ext.Close()
	defer agent.Close()

	manifests, err := agent.Discover(context.Background(), 500)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("discover = %d manifests, want 1", len(manifests))
	}
	got := manifests[0]
	if got.ID != "memory" {
		t.Fatalf("discover id = %q", got.ID)
	}
	if len(got.Tools) != 3 {
		t.Fatalf("discover tools = %d, want 3", len(got.Tools))
	}
	names := map[string]bool{}
	for _, tool := range got.Tools {
		names[tool.Name] = true
		if tool.Description == "" {
			t.Fatalf("discovered tool %q has empty description", tool.Name)
		}
	}
	for _, want := range []string{"todowrite", "history_search", "history_range"} {
		if !names[want] {
			t.Fatalf("discovered tools missing %q: %v", want, names)
		}
	}
}

// TestTodowriteWire drives todowrite through the abep envelope (session_name
// as the first-class field, no _session arg) and verifies persistence.
func TestTodowriteWire(t *testing.T) {
	s := newServer(t, testPool(t))
	hub := abep.NewInprocHub()
	m, err := abep.ParseManifest(manifestYaml)
	if err != nil {
		t.Fatal(err)
	}
	agent, ext := startExt(t, hub, m.Config(s.handlers(), nil, nil))
	defer ext.Close()
	defer agent.Close()

	sid := "memtest-" + fmt.Sprint(time.Now().UnixNano())
	res, err := agent.CallTool(context.Background(), sid, "memory", "todowrite", "w1",
		map[string]interface{}{
			"todos": []interface{}{
				map[string]interface{}{"content": "wire todo", "status": "pending", "priority": "high"},
			},
		}, func(string) {})
	if err != nil {
		t.Fatalf("todowrite wire: %v", err)
	}
	if !strings.Contains(res.Content, "1") {
		t.Fatalf("todowrite content = %q", res.Content)
	}

	todos, err := s.listTodos(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(todos) != 1 || todos[0].Content != "wire todo" {
		t.Fatalf("persisted todos = %+v", todos)
	}
}

// TestHistorySearchWire drives history_search through the envelope: the
// session_name field (not _session) must resolve the chain.
func TestHistorySearchWire(t *testing.T) {
	s := newServer(t, testPool(t))
	sid := "memtest-" + fmt.Sprint(time.Now().UnixNano())

	ctx := context.Background()
	var tip string
	s.pool.QueryRow(ctx, `INSERT INTO sessions (name, model, preset, created_at, updated_at)
		VALUES ($1,'','','2026-01-01 00:00:00','2026-01-01 00:00:00')
		RETURNING tip_id`, sid).Scan(&tip)
	m1 := "w-" + fmt.Sprint(time.Now().UnixNano()) + "-1"
	m2 := "w-" + fmt.Sprint(time.Now().UnixNano()) + "-2"
	_, _ = s.pool.Exec(ctx, `INSERT INTO messages (id, role, prev_id, created_at) VALUES
		($1,'user','','2026-01-01 00:00:01'),
		($2,'assistant','','2026-01-01 00:00:02')`, m1, m2)
	_, _ = s.pool.Exec(ctx, `UPDATE messages SET prev_id = $1 WHERE id = $2`, m1, m2)
	_, _ = s.pool.Exec(ctx, `UPDATE sessions SET tip_id = $1 WHERE name = $2`, m2, sid)
	_, _ = s.pool.Exec(ctx, `INSERT INTO parts (id, message_id, type, seq, data) VALUES
		($1,$2,'text',0,'{"text":"needle content"}'),
		($3,$4,'text',0,'{"text":"other"}')`,
		"wp1-"+m1, m1, "wp2-"+m2, m2)

	hub := abep.NewInprocHub()
	m, err := abep.ParseManifest(manifestYaml)
	if err != nil {
		t.Fatal(err)
	}
	agent, ext := startExt(t, hub, m.Config(s.handlers(), nil, nil))
	defer ext.Close()
	defer agent.Close()

	res, err := agent.CallTool(context.Background(), sid, "memory", "history_search", "s1",
		map[string]interface{}{"query": "needle", "limit": 10}, func(string) {})
	if err != nil {
		t.Fatalf("history_search wire: %v", err)
	}
	meta, ok := res.Metadata.(map[string]interface{})
	if !ok {
		t.Fatalf("history_search metadata = %T (%v)", res.Metadata, res.Metadata)
	}
	if meta["count"] != float64(1) {
		t.Fatalf("history_search count = %v, want 1", meta["count"])
	}
}

// TestHistoryRangeWire drives history_range through the envelope.
func TestHistoryRangeWire(t *testing.T) {
	s := newServer(t, testPool(t))
	sid := "memtest-" + fmt.Sprint(time.Now().UnixNano())

	ctx := context.Background()
	var tip string
	s.pool.QueryRow(ctx, `INSERT INTO sessions (name, model, preset, created_at, updated_at)
		VALUES ($1,'','','2026-01-01 00:00:00','2026-01-01 00:00:00')
		RETURNING tip_id`, sid).Scan(&tip)
	m1 := "rw-" + fmt.Sprint(time.Now().UnixNano()) + "-1"
	m2 := "rw-" + fmt.Sprint(time.Now().UnixNano()) + "-2"
	_, _ = s.pool.Exec(ctx, `INSERT INTO messages (id, role, prev_id, created_at) VALUES
		($1,'user','','2026-01-01 00:00:01'),
		($2,'user','','2026-01-01 00:00:02')`, m1, m2)
	_, _ = s.pool.Exec(ctx, `UPDATE messages SET prev_id = $1 WHERE id = $2`, m1, m2)
	_, _ = s.pool.Exec(ctx, `UPDATE sessions SET tip_id = $1 WHERE name = $2`, m2, sid)
	_, _ = s.pool.Exec(ctx, `INSERT INTO parts (id, message_id, type, seq, data) VALUES
		($1,$2,'text',0,'{"text":"first"}'),
		($3,$4,'text',0,'{"text":"second"}')`,
		"rp1-"+m1, m1, "rp2-"+m2, m2)

	hub := abep.NewInprocHub()
	m, err := abep.ParseManifest(manifestYaml)
	if err != nil {
		t.Fatal(err)
	}
	agent, ext := startExt(t, hub, m.Config(s.handlers(), nil, nil))
	defer ext.Close()
	defer agent.Close()

	res, err := agent.CallTool(context.Background(), sid, "memory", "history_range", "r1",
		map[string]interface{}{"from": 0, "to": 1, "limit": 100}, func(string) {})
	if err != nil {
		t.Fatalf("history_range wire: %v", err)
	}
	meta, ok := res.Metadata.(map[string]interface{})
	if !ok {
		t.Fatalf("history_range metadata = %T (%v)", res.Metadata, res.Metadata)
	}
	if meta["count"] != float64(1) {
		t.Fatalf("history_range count = %v, want 1", meta["count"])
	}
}

// ---- error paths over the wire ----

// TestHistorySearchMissingSessionWire: no session_name and no _session must
// produce a tool error, not a silent default.
func TestHistorySearchMissingSessionWire(t *testing.T) {
	s := newServer(t, testPool(t))
	hub := abep.NewInprocHub()
	m, err := abep.ParseManifest(manifestYaml)
	if err != nil {
		t.Fatal(err)
	}
	agent, ext := startExt(t, hub, m.Config(s.handlers(), nil, nil))
	defer ext.Close()
	defer agent.Close()

	res, err := agent.CallTool(context.Background(), "", "memory", "history_search", "e1",
		map[string]interface{}{"query": "x"}, func(string) {})
	if err != nil {
		t.Fatalf("history_search should return a tool-level error, got transport error: %v", err)
	}
	if !strings.Contains(res.Content, "failed") && !strings.Contains(res.Content, "会话") {
		t.Fatalf("history_search missing session content = %q", res.Content)
	}
}
