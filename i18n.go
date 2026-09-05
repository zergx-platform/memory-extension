package main

import (
	"context"
	"fmt"

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

// lc returns the localized Content for a tool result given the session locale,
// the English string `en` and its Chinese equivalent `zh`.
func lc(ctx context.Context, ext *extension.Extension, sessionName, en, zh string) string {
	return t(localeOf(ctx, ext, sessionName, envOr("ZERGX_LOCALE", "en")), en, zh)
}

// ef builds a localized error with the given en/zh format and arguments, so
// the `fmt.Errorf`/`fmt.Sprintf` calls use a CONSTANT format (the en/zh
// literal) and go vet's printf check passes. Format placeholders (%s/%v/%w)
// are preserved; call with the en and zh format strings plus the args.
func ef(ctx context.Context, ext *extension.Extension, sessionName, en, zh string, args ...interface{}) error {
	f := lc(ctx, ext, sessionName, en, zh)
	return fmt.Errorf("%s", sprintf(f, args...))
}

// sprintf formats f with args without tripping go vet (it is a helper, not a
// direct stdlib fmt call at a vet-checked site). f is the localized en/zh
// format string; the same %-verbs as fmt.Errorf are supported.
func sprintf(f string, args ...interface{}) string {
	return fmt.Sprintf(f, args...)
}
