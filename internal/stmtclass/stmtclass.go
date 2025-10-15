// Package stmtclass provides lightweight runtime SQL statement classification
// used to decide transactional vs non-transactional execution and idempotency
// hints.
package stmtclass

import (
	"regexp"
	"strings"

	"github.com/muir/sqltoken"
)

// Dialect enumerates supported SQL dialects.
type Dialect int

const (
	DialectMySQL Dialect = iota
	DialectPostgres
)

// Flag is a bitmask describing statement / script properties.
type Flag uint32

// Individual flag bits.
const (
	IsDDL Flag = 1 << iota
	IsDML
	IsNonIdempotent
	IsMultipleStatements
	IsEasilyIdempotentFix
	IsMustNonTx // Statement must run outside a transaction (e.g. PG CREATE INDEX CONCURRENTLY)
)

// StatementFlags associates a statement's original text with its flags.
type StatementFlags struct {
	Flags Flag
	Text  string
}

// Summarized aggregates useful characteristics for higher-level logic.
// Summary maps each flag to the first statement text that exhibited it.
// Flags that are synthetic (e.g. IsMultipleStatements) will map to "" unless
// explicitly derived from a concrete statement.
type Summary map[Flag]string

// flagsInOrder provides a single stable ordering for iteration & presentation.
// (Replaces previous duplicated allFlags / flagNameOrder slices.)
var flagsInOrder = []Flag{IsMultipleStatements, IsDDL, IsDML, IsNonIdempotent, IsEasilyIdempotentFix, IsMustNonTx}

// Summarize builds a Summary. For each flag it records the first statement whose Flags include it.
func Summarize(stmts []StatementFlags, aggregate Flag) Summary {
	out := make(Summary)
	// Record synthetic multi flag explicitly if present.
	if aggregate&IsMultipleStatements != 0 {
		out[IsMultipleStatements] = ""
	}
	for _, st := range stmts {
		for _, f := range flagsInOrder {
			if f == IsMultipleStatements { // already handled; no statement owns it
				continue
			}
			if st.Flags&f != 0 {
				if _, exists := out[f]; !exists {
					out[f] = st.Text
				}
			}
		}
	}
	return out
}

// FlagNames returns human friendly names of flags in the aggregate mask (debug/testing).
var flagNameMap = map[Flag]string{
	IsMultipleStatements:  "Multi",
	IsDDL:                 "DDL",
	IsDML:                 "DML",
	IsNonIdempotent:       "NonIdem",
	IsEasilyIdempotentFix: "EasyFix",
	IsMustNonTx:           "MustNonTx",
}

func FlagNames(agg Flag) (names []string) {
	for _, f := range flagsInOrder {
		if agg&f != 0 {
			names = append(names, flagNameMap[f])
		}
	}
	return names
}

// Postgres statements that inherently require execution outside a transaction.
// Lower-cased, whitespace-collapsed prefixes.
var pgMustNonTxPrefixes = []string{
	"create index concurrently",
	"create unique index concurrently",
	"drop index concurrently",
	"refresh materialized view concurrently",
	"reindex concurrently",
	"vacuum full",
	"cluster",
	"create database",
	"drop database",
	"create tablespace",
	"drop tablespace",
	"create subscription",
	"alter subscription",
	"drop subscription",
}

// isPostgresMustNonTxVersion returns true when a statement must execute outside a
// transaction for the specified major version.
func isPostgresMustNonTxVersion(txt string, major int) bool {
	norm := strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(txt))), " ")
	// ALTER TYPE ... ADD VALUE is only non-transactional pre-12.
	if major > 0 && major < 12 && strings.HasPrefix(norm, "alter type") && strings.Contains(norm, " add value") {
		return true
	}
	for _, p := range pgMustNonTxPrefixes {
		if strings.HasPrefix(norm, p) {
			return true
		}
	}
	return false
}

// Helpers for (easily) idempotent CREATE/DROP classification.
var (
	mysqlCreateEasy = []string{"create table"}
	pgCreateEasy    = []string{"create table", "create index", "create schema", "create sequence", "create extension"}
	mysqlDropEasy   = []string{"drop table", "drop database"}
	pgDropEasy      = []string{"drop table", "drop index", "drop schema", "drop sequence", "drop extension"}
)

func classifyCreate(d Dialect, txt string) Flag {
	f := IsDDL
	low := strings.ToLower(strings.TrimSpace(txt))
	list := mysqlCreateEasy
	if d == DialectPostgres {
		list = pgCreateEasy
	}
	for _, p := range list {
		if strings.HasPrefix(low, p) {
			if !ifExistsRE.MatchString(low) {
				f |= IsNonIdempotent | IsEasilyIdempotentFix
			}
			return f
		}
	}
	if !ifExistsRE.MatchString(low) { // generic CREATE without IF EXISTS
		f |= IsNonIdempotent
	}
	return f
}

func classifyAlter(d Dialect, txt string) Flag {
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

func classifyDrop(d Dialect, txt string) Flag {
	f := IsDDL
	low := strings.ToLower(strings.TrimSpace(txt))
	if ifExistsRE.MatchString(low) {
		return f
	}
	f |= IsNonIdempotent
	list := mysqlDropEasy
	if d == DialectPostgres {
		list = pgDropEasy
	}
	for _, p := range list {
		if strings.HasPrefix(low, p) {
			f |= IsEasilyIdempotentFix
			break
		}
	}
	return f
}

// ClassifyTokens classifies a tokenized script. For Postgres, majorVersion supplies
// version gating (pass 0 if unknown). Returns per-statement flags and aggregate mask.
func ClassifyTokens(d Dialect, majorVersion int, ts sqltoken.Tokens) (stmts []StatementFlags, aggregate Flag) {
	split := ts.Strip().CmdSplit()
	if len(split) > 1 {
		aggregate |= IsMultipleStatements
	}
	for _, s := range split {
		if len(s) == 0 { // empty (e.g., pure comment)
			continue
		}
		txt := s.String()
		first := strings.ToLower(s[0].Text)
		var f Flag
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
		}
		if d == DialectPostgres && isPostgresMustNonTxVersion(txt, majorVersion) {
			f |= IsMustNonTx
		}
		stmts = append(stmts, StatementFlags{Flags: f, Text: txt})
		aggregate |= f
	}
	return stmts, aggregate
}

// ifExistsRE matches IF EXISTS / IF NOT EXISTS (case-insensitive) used to identify
// guarded DDL statements for idempotency classification.
var ifExistsRE = regexp.MustCompile(`(?i)\bIF\s+(?:NOT\s+)?EXISTS\b`)
