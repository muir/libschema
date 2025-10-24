package lspostgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePostgresServerVersion(t *testing.T) {
	cases := []struct {
		in    string
		major int
		minor int
	}{
		{"16.3", 16, 3},
		{"15.11", 15, 11},
		{"14beta1", 14, 0}, // beta w/out explicit minor treated as 0
		{"13.9 (Ubuntu 13.9-1)", 13, 9},
		{"12", 12, 0},
		{"garbage", 0, 0},
		{"", 0, 0},
	}
	for _, c := range cases {
		maj, min := parsePostgresServerVersion(c.in)
		require.Equalf(t, c.major, maj, "major mismatch for input %q", c.in)
		require.Equalf(t, c.minor, min, "minor mismatch for input %q", c.in)
	}
}
