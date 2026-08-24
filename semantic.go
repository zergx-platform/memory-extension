package main

import (
	"context"
)

// semanticHit is one message matched by vector similarity.
type semanticHit struct {
	MessageID string  `json:"message_id"`
	Role      string  `json:"role"`
	Content   string  `json:"content"`
	CreatedAt string  `json:"created_at"`
	Score     float32 `json:"score"`
}

// chainIDs returns the message ids of a session's chain, newest first, up to
// cap entries. The chain is walked from sessions.tip_id via prev_id.
func (s *server) chainIDs(ctx context.Context, sid string, cap int) ([]string, error) {
	if cap <= 0 {
		cap = 500
	}
	tip, err := s.sessionTip(ctx, sid)
	if err != nil {
		return nil, err
	}
	if tip == "" {
		return []string{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE chain AS (
			SELECT id, prev_id, 0 AS depth FROM messages WHERE id = $1
			UNION ALL
			SELECT m.id, m.prev_id, c.depth + 1
			FROM messages m JOIN chain c ON m.id = c.prev_id
		)
		SELECT id FROM chain WHERE depth < $2 ORDER BY depth ASC`, tip, cap)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ensureEmbedding computes and stores the embedding for a message if absent,
// returning it. Idempotent: already-embedded messages are skipped.
func (s *server) ensureEmbedding(ctx context.Context, msgID, content string) error {
	var exists bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM message_embeddings WHERE message_id = $1)`, msgID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	vec, err := s.embed.embed(ctx, content)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO message_embeddings (message_id, embedding) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		msgID, vectorLit(vec))
	return err
}

// searchSimilar returns the k messages of this session most similar to the
// query. Unembedded messages in the chain are embedded lazily (bounded by
// embedCap) before the cosine scan.
func (s *server) searchSimilar(ctx context.Context, sid, query string, k, embedCap int) ([]semanticHit, error) {
	if k <= 0 {
		k = 5
	}
	if embedCap <= 0 {
		embedCap = 200
	}

	ids, err := s.chainIDs(ctx, sid, embedCap)
	if err != nil {
		return nil, err
	}
	// Lazily embed the chain's messages (bounded, best-effort per row).
	for _, id := range ids {
		var content string
		if err := s.pool.QueryRow(ctx,
			`SELECT content FROM messages WHERE id = $1`, id).Scan(&content); err != nil {
			continue
		}
		if content == "" {
			continue
		}
		if err := s.ensureEmbedding(ctx, id, content); err != nil {
			continue
		}
	}

	qvec, err := s.embed.embed(ctx, query)
	if err != nil {
		return nil, err
	}
	lit := vectorLit(qvec)

	// Score = cosine similarity = 1 - cosine_distance. Restrict to this
	// session's chain ids via = ANY($2).
	rows, err := s.pool.Query(ctx, `
		SELECT me.message_id, m.role, m.content, m.created_at,
		       1 - (me.embedding <=> $1) AS score
		FROM message_embeddings me
		JOIN messages m ON m.id = me.message_id
		WHERE me.message_id = ANY($2)
		ORDER BY me.embedding <=> $1
		LIMIT $3`, lit, ids, k)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []semanticHit
	for rows.Next() {
		var h semanticHit
		if err := rows.Scan(&h.MessageID, &h.Role, &h.Content, &h.CreatedAt, &h.Score); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
