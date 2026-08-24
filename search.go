package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// history_entry is one message in a session chain, flattened for retrieval.
type historyEntry struct {
	Session   string `json:"session"`
	Role      string `json:"role"`
	Content   string `json:"content"`
	ToolName  string `json:"tool_name,omitempty"`
	ChangeID  string `json:"change_id,omitempty"`
	CreatedAt string `json:"created_at"`
	// Depth is the message's position in the chain: 0 = tip (newest),
	// increasing toward the root. Serves as the message-level seq.
	Depth int `json:"depth"`
}

// chain SQL walks a session's message chain from its tip via prev_id, with
// depth as the message-level sequence. Session tip resolves via
// sessions.tip_id; empty tip falls back to the newest message of that role
// chain is session-scoped only through the tip, so a session with no tip has
// no retrievable history.
const chainSQL = `
WITH RECURSIVE chain AS (
  SELECT m.id, m.role, m.content, m.prev_id, m.created_at, 0 AS depth
  FROM messages m
  WHERE m.id = $1
  UNION ALL
  SELECT m.id, m.role, m.content, m.prev_id, m.created_at, c.depth + 1
  FROM messages m JOIN chain c ON m.id = c.prev_id
)
SELECT id, role, content, created_at, depth
FROM chain
WHERE depth < $2
ORDER BY depth ASC`

// sessionTip resolves the message-id tip of a session.
func (s *server) sessionTip(ctx context.Context, sid string) (string, error) {
	var tip string
	err := s.pool.QueryRow(ctx,
		`SELECT tip_id FROM sessions WHERE name = $1`, sid).Scan(&tip)
	if err != nil {
		return "", err
	}
	return tip, nil
}

// chainEntries walks a session chain from its tip, oldest-first, applying a
// depth window [depthFrom, depthTo) where depth=0 is the newest message.
// depthFrom/depthTo are chain positions: 0..N. Negative values count from the
// end (newest side): -1 = the tip.
func (s *server) chainEntries(ctx context.Context, sid string, depthFrom, depthTo, limit int) ([]historyEntry, error) {
	tip, err := s.sessionTip(ctx, sid)
	if err != nil {
		return nil, err
	}
	if tip == "" {
		return []historyEntry{}, nil
	}
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, chainSQL, tip, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var all []chainRaw
	for rows.Next() {
		var r chainRaw
		if err := rows.Scan(&r.id, &r.role, &r.content, &r.created, &r.depth); err != nil {
			return nil, err
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Normalize negative window bounds against the chain length.
	total := len(all)
	norm := func(d int) int {
		if d < 0 {
			return total + d // -1 → total-1 (the tip index in oldest-first order)
		}
		return d
	}
	from, to := norm(depthFrom), norm(depthTo)
	if from < 0 {
		from = 0
	}
	if to > total {
		to = total
	}
	if from >= to {
		return []historyEntry{}, nil
	}

	// Merge tool parts (tool name + change_id) keyed by message id.
	toolByMsg := map[string][]partInfo{}
	if ids := msgIDs(all); len(ids) > 0 {
		toolByMsg = s.partsForMessages(ctx, ids)
	}

	out := make([]historyEntry, 0, to-from)
	for _, r := range all[from:to] {
		e := historyEntry{
			Session:   sid,
			Role:      r.role,
			Content:   r.content,
			CreatedAt: r.created,
			Depth:     r.depth, // SQL depth: tip = 0 (newest)
		}
		if parts, ok := toolByMsg[r.id]; ok {
			for _, p := range parts {
				if e.ToolName == "" {
					e.ToolName = p.name
				}
				if p.changeID != "" && e.ChangeID == "" {
					e.ChangeID = p.changeID
				}
			}
		}
		out = append(out, e)
	}
	return out, nil
}

type partInfo struct {
	name     string
	changeID string
}

type chainRaw struct {
	id, role, content, created string
	depth                      int
}

func msgIDs(rows []chainRaw) []string {
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.id)
	}
	return ids
}

// partsForMessages extracts tool name + change_id for a set of message ids
// from the parts table. change_id lives in tool_result.data.metadata.change_id
// (written by repo-extension); tool name lives in tool.data.name.
func (s *server) partsForMessages(ctx context.Context, ids []string) map[string][]partInfo {
	out := map[string][]partInfo{}
	if len(ids) == 0 {
		return out
	}
	// parts.message_id references messages.id; join via = ANY.
	rows, err := s.pool.Query(ctx, `
		SELECT message_id, type, data FROM parts
		WHERE message_id = ANY($1)
		ORDER BY message_id, seq`, ids)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var mid, typ, data string
		if err := rows.Scan(&mid, &typ, &data); err != nil {
			continue
		}
		var v map[string]interface{}
		if json.Unmarshal([]byte(data), &v) != nil {
			continue
		}
		var p partInfo
		switch typ {
		case "tool":
			p.name = strVal(v, "name")
		case "tool_result":
			if md, ok := v["metadata"].(map[string]interface{}); ok {
				p.changeID = strVal(md, "change_id")
			}
		}
		out[mid] = append(out[mid], p)
	}
	return out
}

// searchParams is the flattened argument set for history_search.
type searchParams struct {
	session string
	query   string
	from    string // inclusive timestamp
	to      string // inclusive timestamp
	limit   int
}

func (s *server) searchHistory(ctx context.Context, p searchParams) ([]historyEntry, error) {
	if p.session == "" {
		return nil, fmt.Errorf("session is required")
	}
	if p.limit <= 0 {
		p.limit = 50
	}
	// Deep-walk a bounded chain from the tip, filtering client-side so the
	// window semantics stay identical across keyword/time/seq searches.
	tip, err := s.sessionTip(ctx, p.session)
	if err != nil {
		return nil, err
	}
	if tip == "" {
		return []historyEntry{}, nil
	}
	rows, err := s.pool.Query(ctx, chainSQL, tip, 10000)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var all []chainRaw
	for rows.Next() {
		var r chainRaw
		if err := rows.Scan(&r.id, &r.role, &r.content, &r.created, &r.depth); err != nil {
			return nil, err
		}
		all = append(all, r)
	}
	_ = rows.Err()

	toolByMsg := s.partsForMessages(ctx, msgIDs(all))

	out := []historyEntry{}
	for _, r := range all {
		if p.from != "" && r.created < p.from {
			continue
		}
		if p.to != "" && r.created > p.to {
			continue
		}
		e := historyEntry{
			Session:   p.session,
			Role:      r.role,
			Content:   r.content,
			CreatedAt: r.created,
			Depth:     r.depth,
		}
		if parts, ok := toolByMsg[r.id]; ok {
			for _, pt := range parts {
				if e.ToolName == "" {
					e.ToolName = pt.name
				}
				if pt.changeID != "" && e.ChangeID == "" {
					e.ChangeID = pt.changeID
				}
			}
		}
		if p.query != "" {
			hay := strings.ToLower(e.Content + " " + e.ToolName + " " + e.ChangeID)
			if !strings.Contains(hay, strings.ToLower(p.query)) {
				continue
			}
		}
		out = append(out, e)
		if len(out) >= p.limit {
			break
		}
	}
	return out, nil
}

func strVal(m map[string]interface{}, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}
