package main

import (
	"context"

	"github.com/abcp-sdk/abc-protocol-go/extension"
)

// localeOf resolves the session's effective locale for a tool call. It reads
// the agent-projected `vars.agent.locale` (provider "agent") and falls back to
// the env default. Since each extension serves many sessions concurrently, we
// read KV per call (the locale is small and already cached by the agent).
func localeOf(ctx context.Context, ext *extension.Extension, sessionName, fallback string) string {
	if ext == nil || sessionName == "" {
		return fallback
	}
	// Provider is "agent" (the agent writes vars.agent.locale); the extension
	// reads it back by explicit provider id + session.
	v := ext.GetSessionVariable(ctx, "agent", sessionName, "locale", "")
	if v == "" {
		return fallback
	}
	return v
}

// isZh reports whether a locale is Chinese-family (zh / zh-cn / zh-hans/…).
func isZh(locale string) bool {
	return len(locale) >= 2 && locale[0] == 'z' && locale[1] == 'h'
}

// t picks the localized prose: English default, or the zh override.
func t(locale, en, zh string) string {
	if isZh(locale) {
		return zh
	}
	return en
}
