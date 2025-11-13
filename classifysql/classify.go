package classifysql

import (
	"errors"
	"regexp"
	"strings"

	"github.com/muir/sqltoken"
)

// TODO(classifysql): remove internal/stmtclass and use this class instead

// Dialect enumerates supported SQL dialects.
type Dialect int

const (
	DialectMySQL Dialect = iota
	DialectPostgres
	DialectSingleStore = DialectMySQL
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

var flagNameMap = map[Flag]string{
	IsDDL:                 "DDL",
	IsDML:                 "DML",
	IsNonIdempotent:       "NonIdem",
	IsMultipleStatements:  "Multi",
	IsEasilyIdempotentFix: "EasyFix",
	IsMustNonTx:           "MustNonTx",
}

var flagsInOrder = []Flag{IsDDL, IsDML, IsNonIdempotent, IsEasilyIdempotentFix, IsMustNonTx, IsMultipleStatements}

// Statement represents one SQL statement with its original, unstripped tokens and classification flags.
type Statement struct {
	Flags   Flag
	Tokens  sqltoken.Tokens // unstripped tokens for this statement
	dialect Dialect
}

// Statements is a slice of Statement.
type Statements []Statement

// Postgres statements that inherently require execution outside a transaction.
// Lower-cased, whitespace-collapsed prefixes.
// TODO(classifysql): remove pgMustNonTxPrefixes from internal/stmtclass once migrated.
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

var (
	mysqlCreateEasy = []string{"create table"}
	mysqlDropEasy   = []string{"drop table", "drop database"}
	pgCreateEasy    = []string{"create table", "create index", "create schema", "create sequence", "create extension"}
	pgDropEasy      = []string{"drop table", "drop index", "drop schema", "drop sequence", "drop extension"}
)

// ClassifyTokens tokenizes and classifies a SQL script, returning per-statement flags and original tokens.
// Only error case is invalid dialect.
// strings.Join(ClassifyTokens().TokensList.Strings, ";") should return the original sqlString
func ClassifyTokens(d Dialect, majorVersion int, sqlString string) (Statements, error) {
	if strings.TrimSpace(sqlString) == "" {
		return nil, nil
	}
	var tokens sqltoken.Tokens
	switch d {
	case DialectPostgres:
		// TokenizePostgreSQL preserves comments/whitespace in tokens
		tokens = sqltoken.TokenizePostgreSQL(sqlString)
	case DialectMySQL:
		// TokenizeMySQL preserves comments/whitespace in tokens
		tokens = sqltoken.TokenizeMySQL(sqlString)
	default:
		return nil, errors.New("invalid dialect")
	}
	split := tokens.CmdSplitUnstripped()
	stmts := make(Statements, len(split))
	for i, raw := range split {
		stmts[i].Tokens = raw
		stmts[i].dialect = d
		stripped := raw.Strip()
		if len(stripped) == 0 {
			// just comments/whitespace
			continue
		}
		stmts[i].Flags = classifyFirstVerb(d, majorVersion, stripped)
	}
	// Add IsMultipleStatements if there are multiple real statements
	if stmts.countNonEmpty() > 1 {
		for i := range stmts {
			stmts[i].Flags |= IsMultipleStatements
		}
	}
	return stmts, nil
}

// classifyFirstVerb applies verb-based classification logic (copied from stmtclass with minimal adaptation).
func classifyFirstVerb(d Dialect, majorVersion int, stripped sqltoken.Tokens) Flag {
	first := strings.ToLower(stripped[0].Text)
	txt := stripped.String()
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
	return f
}

// Regroup returns groups of statements that are compatible to run together in a single migration.
// Algorithm (single pass):
// Postgres: statements with IsMustNonTx must be isolated; all others can group together.
// MySQL: each DDL (IsDDL) isolated; consecutive/any DML grouped together.
// Mixed order preserved by starting a new group when incompatibility detected.
func (s Statements) Regroup() []Statements {
	if len(s) == 0 {
		return nil
	}
	d := s[0].dialect
	var groups []Statements
	var current Statements
	flush := func() {
		if len(current) > 0 {
			groups = append(groups, current)
			current = nil
		}
	}
	for _, st := range s {
		if d == DialectPostgres {
			if st.Flags&IsMustNonTx != 0 { // isolate
				flush()
				groups = append(groups, Statements{st})
				continue
			}
			current = append(current, st)
			continue
		}
		// MySQL grouping rules
		if st.Flags&IsDDL != 0 { // isolate DDL
			flush()
			groups = append(groups, Statements{st})
			continue
		}
		// DML -> can merge with existing current if it contains only DML
		if len(current) > 0 {
			current = append(current, st)
		} else {
			current = Statements{st}
		}
	}
	flush()
	// Clear Multi flag from any isolated single-statement group; keep in groups with >1 real statements.
	for gi, g := range groups {
		if g.countNonEmpty() == 1 {
			// isolated
			for si := range g {
				g[si].Flags &^= IsMultipleStatements
			}
			groups[gi] = g
		}
	}
	return groups
}

func (s Statements) countNonEmpty() int {
	nonEmpty := 0
	for _, st := range s {
		if len(st.Tokens.Strip()) == 0 {
			continue
		}
		nonEmpty++
	}
	return nonEmpty
}

func (s Statements) TokensList() sqltoken.TokensList {
	// Preserve placeholder entries including empty statements to reconstruct original segmentation.
	tl := make(sqltoken.TokensList, len(s))
	for i, stmt := range s {
		tl[i] = stmt.Tokens
	}
	return tl
}

// Summary maps each flag to the first statement Tokens that exhibited it. IsMultipleStatements is synthetic.
type Summary map[Flag]sqltoken.Tokens

// Summarize builds a Summary from the classified statements.
func (s Statements) Summarize() Summary {
	out := make(Summary)
	for _, st := range s {
		for _, f := range flagsInOrder {
			if st.Flags&f != 0 {
				if _, exists := out[f]; !exists {
					out[f] = st.Tokens
				}
			}
		}
	}
	return out
}

// Names returns human-friendly names of set bits in the flag mask.
func (f Flag) Names() []string {
	var names []string
	for _, bit := range flagsInOrder {
		if f&bit != 0 {
			names = append(names, flagNameMap[bit])
		}
	}
	return names
}

// isPostgresMustNonTxVersion returns true when a statement must execute outside a transaction for the specified major version.
// TODO(classifysql): remove duplicate from internal/stmtclass.
func isPostgresMustNonTxVersion(txt string, major int) bool {
	norm := strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(txt))), " ")
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

var ifExistsRE = regexp.MustCompile(`(?i)\bIF\s+(?:NOT\s+)?EXISTS\b`)

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
