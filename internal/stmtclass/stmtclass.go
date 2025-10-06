package stmtclass

import (
	"regexp"
	"strings"

	"github.com/muir/sqltoken"
)

// Dialect enumerates supported SQL dialect nuances for classification.
type Dialect int

const (
	DialectMySQL Dialect = iota
	DialectPostgres
)

// Bit flags describing aggregate script properties.
// A script may have multiple statements; we aggregate over them.
// Flags are intentionally small so they can be OR'ed and tested cheaply.
const (
	IsDDL uint32 = 1 << iota
	IsDML
	IsNonIdempotent // Statement lacks idempotency guard (no IF EXISTS / IF NOT EXISTS) or inherently non-idempotent form
	IsMultipleStatements
	IsEasilyIdempotentFix // Non-idempotent but trivially fixable by adding IF NOT EXISTS (currently only CREATE TABLE case)
)

// StatementFlags describes a single statement's flags plus its text.
type StatementFlags struct {
	Flags uint32
	Text  string
}

// FlagNames returns a stable slice of short names describing the bitfield f.
// Order is deterministic for log friendliness.
func FlagNames(f uint32) []string {
	var n []string
	if f&IsDDL != 0 {
		n = append(n, "DDL")
	}
	if f&IsDML != 0 {
		n = append(n, "DML")
	}
	if f&IsNonIdempotent != 0 {
		n = append(n, "NonIdem")
	}
	if f&IsMultipleStatements != 0 {
		n = append(n, "Multi")
	}
	if f&IsEasilyIdempotentFix != 0 {
		n = append(n, "EasyFix")
	}
	return n
}

// Summary captures aggregated properties across a set of classified statements.
// FirstNonIdempotentDDL is only set when the first statement that is both DDL & NonIdempotent is found.
type Summary struct {
	HasDDL                bool
	HasDML                bool
	FirstNonIdempotentDDL string
	AggregateFlags        uint32
}

// SummarizeStatements aggregates flags for convenience and discovers the first non-idempotent DDL.
func SummarizeStatements(stmts []StatementFlags, aggregate uint32) Summary {
	var s Summary
	s.AggregateFlags = aggregate
	for _, st := range stmts {
		if st.Flags&IsDDL != 0 {
			s.HasDDL = true
		}
		if st.Flags&IsDML != 0 {
			s.HasDML = true
		}
		if s.FirstNonIdempotentDDL == "" && (st.Flags&(IsDDL|IsNonIdempotent) == (IsDDL | IsNonIdempotent)) {
			s.FirstNonIdempotentDDL = st.Text
		}
	}
	return s
}

// ClassifyTokens classifies an already-tokenized script for the given dialect.
// Prefer this over ClassifyScript when the caller can specify dialect explicitly.
func ClassifyTokens(d Dialect, ts sqltoken.Tokens) (stmts []StatementFlags, aggregate uint32) {
	split := ts.Strip().CmdSplit()
	if len(split) > 1 {
		aggregate |= IsMultipleStatements
	}
	for _, s := range split {
		if len(s) == 0 {
			continue
		}
		txt := s.String()
		first := strings.ToLower(s[0].Text)
		var f uint32
		switch first {
		case "create":
			f = classifyCreate(d, txt)
		case "alter":
			f = classifyAlter(d, txt)
		case "drop":
			f = classifyDrop(d, txt)
		case "rename", "comment":
			f = IsDDL | IsNonIdempotent
		case "truncate":
			f = IsDDL
		case "insert", "update", "delete", "replace", "call", "do", "load", "handler", "import", "with":
			f = IsDML
		default:
			// unknown leading token -> leave flags zero; caller can treat as neutral
		}
		stmts = append(stmts, StatementFlags{Flags: f, Text: txt})
		aggregate |= f
	}
	return stmts, aggregate
}

var (
	mysqlCreateEasy = []string{"create table"}
	pgCreateEasy    = []string{"create table", "create index", "create schema", "create sequence", "create extension"}
	mysqlDropEasy   = []string{"drop table", "drop database"}
	pgDropEasy      = []string{"drop table", "drop index", "drop schema", "drop sequence", "drop extension"}
)

func classifyCreate(d Dialect, txt string) uint32 {
	f := IsDDL
	low := strings.ToLower(strings.TrimSpace(txt))
	listEasy := mysqlCreateEasy
	if d == DialectPostgres {
		listEasy = pgCreateEasy
	}
	var matchedEasy bool
	for _, p := range listEasy {
		if strings.HasPrefix(low, p) {
			matchedEasy = true
			break
		}
	}
	if matchedEasy {
		if !ifExistsRE.MatchString(low) {
			f |= IsNonIdempotent | IsEasilyIdempotentFix
		}
		return f
	}
	// Not an easy-fix CREATE; mark non-idempotent if unguarded
	if !ifExistsRE.MatchString(low) {
		f |= IsNonIdempotent
	}
	return f
}

func classifyAlter(d Dialect, txt string) uint32 {
	f := IsDDL
	low := strings.ToLower(strings.TrimSpace(txt))
	if ifExistsRE.MatchString(low) {
		return f
	}
	f |= IsNonIdempotent
	if d == DialectPostgres && strings.Contains(low, " add column") {
		f |= IsEasilyIdempotentFix
	}
	return f
}

func classifyDrop(d Dialect, txt string) uint32 {
	f := IsDDL
	low := strings.ToLower(strings.TrimSpace(txt))
	if ifExistsRE.MatchString(low) {
		return f
	}
	f |= IsNonIdempotent
	listEasy := mysqlDropEasy
	if d == DialectPostgres {
		listEasy = pgDropEasy
	}
	for _, p := range listEasy {
		if strings.HasPrefix(low, p) {
			f |= IsEasilyIdempotentFix
			break
		}
	}
	return f
}

// ClassifyScript retained for backward compatibility; defaults to MySQL dialect.
func ClassifyScript(ts sqltoken.Tokens) (stmts []StatementFlags, aggregate uint32) {
	return ClassifyTokens(DialectMySQL, ts)
}

// ifExistsRE duplicated from stmtcheck to avoid circular dependency.
// Keep regex identical; tests will enforce behavior.
// (?i) case-insensitive; matches IF EXISTS or IF NOT EXISTS
var ifExistsRE = regexp.MustCompile(`(?i)\bIF\s+(?:NOT\s+)?EXISTS\b`)
