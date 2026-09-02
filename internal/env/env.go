// Package env centralizes the environment-variable helpers that every
// zergx Go service previously reimplemented locally.
package env

import (
	"os"
	"strconv"
	"strings"
)

// Or returns the value of k when set and non-empty, else d.
func Or(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// Int returns the integer value of k, or d when unset/invalid.
func Int(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

// NormalizePort accepts both plain ports ("5432") and k8s link-style values
// ("tcp://10.0.0.1:5432") that pods inherit from the environment.
func NormalizePort(v string) string {
	if i := strings.LastIndexByte(v, ':'); i >= 0 {
		if c := v[i+1:]; c != "" {
			return c
		}
	}
	return v
}
