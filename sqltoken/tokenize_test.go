package sqltoken

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMySQLTokenizing(t *testing.T) {
	cases := []Tokens{
		{},
		{
			{Type: Word, Text: "stuff"},
		},
		{
			{Type: Word, Text: "stuff"},
			{Type: Semicolon, Text: ";"},
			{Type: Word, Text: "morestuff"},
		},
		{
			{Type: Word, Text: "stuff"},
			{Type: Comment, Text: "--cmt;\n"},
			{Type: Word, Text: "stuff2"},
		},
		{
			{Type: Word, Text: "r"},
			{Type: Punctuation, Text: "-"},
			{Type: Word, Text: "an"},
			{Type: Punctuation, Text: "-"},
			{Type: Word, Text: "dom"},
		},
		{
			{Type: Word, Text: "singles"},
			{Type: Whitespace, Text: " "},
			{Type: Literal, Text: "''"},
			{Type: Whitespace, Text: " \t"},
			{Type: Literal, Text: "'\\''"},
			{Type: Semicolon, Text: ";"},
			{Type: Whitespace, Text: " "},
			{Type: Literal, Text: "';\\''"},
		},
		{
			{Type: Word, Text: "doubles"},
			{Type: Whitespace, Text: " "},
			{Type: Literal, Text: `""`},
			{Type: Whitespace, Text: " \t"},
			{Type: Literal, Text: `"\""`},
			{Type: Semicolon, Text: ";"},
			{Type: Whitespace, Text: " "},
			{Type: Literal, Text: `";\""`},
		},
		{
			{Type: Word, Text: "singles"},
			{Type: Whitespace, Text: " "},
			{Type: Punctuation, Text: "-"},
			{Type: Literal, Text: "''"},
			{Type: Whitespace, Text: " \t"},
			{Type: Punctuation, Text: "-"},
			{Type: Literal, Text: "'\\''"},
			{Type: Semicolon, Text: ";"},
			{Type: Whitespace, Text: " "},
			{Type: Punctuation, Text: "-"},
			{Type: Literal, Text: "';\\''"},
		},
		{
			{Type: Word, Text: "doubles"},
			{Type: Whitespace, Text: " "},
			{Type: Punctuation, Text: "-"},
			{Type: Literal, Text: `""`},
			{Type: Whitespace, Text: " \t"},
			{Type: Punctuation, Text: "-"},
			{Type: Literal, Text: `"\""`},
			{Type: Semicolon, Text: ";"},
			{Type: Whitespace, Text: " "},
			{Type: Punctuation, Text: "-"},
			{Type: Literal, Text: `";\""`},
		},
		{
			{Type: Word, Text: "r"},
			{Type: Punctuation, Text: "-"},
			{Type: Word, Text: "an"},
			{Type: Punctuation, Text: "-"},
			{Type: Word, Text: "dom"},
			{Type: Whitespace, Text: " "},
			{Type: Literal, Text: `";;"`},
			{Type: Semicolon, Text: ";"},
			{Type: Literal, Text: "';'"},
			{Type: Punctuation, Text: "-"},
			{Type: Literal, Text: `";"`},
			{Type: Whitespace, Text: " "},
			{Type: Punctuation, Text: "-"},
			{Type: Literal, Text: "';'"},
			{Type: Punctuation, Text: "-"},
		},
		{
			{Type: Punctuation, Text: "-//"},
		},
		{
			{Type: Punctuation, Text: "-//-/-"},
			{Type: Whitespace, Text: " "},
		},
		{
			{Type: Punctuation, Text: "/"},
			{Type: Literal, Text: `";"`},
			{Type: Whitespace, Text: "\r\n"},
			{Type: Literal, Text: `";"`},
			{Type: Whitespace, Text: " "},
			{Type: Punctuation, Text: "-/"},
			{Type: Literal, Text: `";"`},
			{Type: Whitespace, Text: " "},
		},
		{
			{Type: Punctuation, Text: "/"},
			{Type: Literal, Text: "';'"},
			{Type: Whitespace, Text: " "},
			{Type: Literal, Text: "';'"},
			{Type: Whitespace, Text: " "},
			{Type: Comment, Text: "/*;*/"},
			{Type: Punctuation, Text: "-/"},
			{Type: Literal, Text: "';'"},
			{Type: Whitespace, Text: " "},
		},
		{
			{Type: Punctuation, Text: "-"},
			{Type: Comment, Text: "/*;*/"},
			{Type: Whitespace, Text: " "},
			{Type: Punctuation, Text: "-/"},
			{Type: Comment, Text: "/*\n\t;*/"},
			{Type: Whitespace, Text: " "},
		},
		{
			{Type: Punctuation, Text: "-"},
			{Type: Comment, Text: "# /# #;\n"},
			{Type: Whitespace, Text: "\t"},
			{Type: Word, Text: "foo"},
		},
		{
			{Type: Literal, Text: "'#;'"},
			{Type: Punctuation, Text: ","},
			{Type: Whitespace, Text: " "},
			{Type: Literal, Text: `"#;"`},
			{Type: Punctuation, Text: ","},
			{Type: Whitespace, Text: " "},
			{Type: Punctuation, Text: "-"},
			{Type: Comment, Text: "# /# #;\n"},
			{Type: Whitespace, Text: "\t"},
			{Type: Word, Text: "foo"},
		},
	}

	for _, tc := range cases {
		text := tc.String()
		t.Log("---------------------------------------")
		t.Log(text)
		t.Log("-----------------")
		got := TokenizeMySQL(text)
		require.Equal(t, text, got.String(), tc.String())
		require.Equal(t, tc, got, tc.String())
	}
}
