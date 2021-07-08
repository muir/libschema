package lsmysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandSplitting(t *testing.T) {
	cases := []struct {
		input string
		want  []string
	}{
		{
			input: ``,
			want:  []string{""},
		},
		{
			input: `stuff`,
			want:  []string{"stuff"},
		},
		{
			input: `stuff1;stuff2`,
			want:  []string{"stuff1;", "stuff2"},
		},
		{
			input: "before--cmt;\nafter",
			want:  []string{"before--cmt;\nafter"},
		},
		{
			input: "r-an-do-m-",
			want:  []string{"r-an-do-m-"},
		},
		{
			input: `singles '' '\''; ';\''`,
			want:  []string{`singles '' '\'';`, ` ';\''`},
		},
		{
			input: `doubles "" "\""; ";\""`,
			want:  []string{`doubles "" "\"";`, ` ";\""`},
		},
		{
			input: `singles -'' -'\''; -';\''`,
			want:  []string{`singles -'' -'\'';`, ` -';\''`},
		},
		{
			input: `doubles -"" -"\""; -";\""`,
			want:  []string{`doubles -"" -"\"";`, ` -";\""`},
		},
		{
			input: `r-an-do-m ";;"; ';'-";" -';'-`,
			want:  []string{`r-an-do-m ";;";`, ` ';'-";" -';'-`},
		},
		{
			input: `-//`,
			want:  []string{`-//`},
		},
		{
			input: `-//-/- `,
			want:  []string{`-//-/- `},
		},
		{
			input: `/";" ";" -/";" `,
			want:  []string{`/";" ";" -/";" `},
		},
		{
			input: `/';' ';' /*;*/ -/';' `,
			want:  []string{`/';' ';' /*;*/ -/';' `},
		},
		{
			input: `-/*;*/ -//*
				;*/ `,
			want: []string{`-/*;*/ -//*
				;*/ `},
		},
		{
			input: `'#;', "#;', -# /# #;
				foo`,
			want: []string{`'#;', "#;', -# /# #;
				foo`},
		},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.want, SplitCommands(tc.input), tc.input)
	}
}
