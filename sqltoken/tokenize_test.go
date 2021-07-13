package sqltoken

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var commonCases = []Tokens{
	{},
	{
		{Type: Word, Text: "c01"},
	},
	{
		{Type: Word, Text: "c02"},
		{Type: Semicolon, Text: ";"},
		{Type: Word, Text: "morestuff"},
	},
	{
		{Type: Word, Text: "c03"},
		{Type: Comment, Text: "--cmt;\n"},
		{Type: Word, Text: "stuff2"},
	},
	{
		{Type: Word, Text: "c04"},
		{Type: Punctuation, Text: "-"},
		{Type: Word, Text: "an"},
		{Type: Punctuation, Text: "-"},
		{Type: Word, Text: "dom"},
	},
	{
		{Type: Word, Text: "c05_singles"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "''"},
		{Type: Whitespace, Text: " \t"},
		{Type: Literal, Text: "'\\''"},
		{Type: Semicolon, Text: ";"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "';\\''"},
	},
	{
		{Type: Word, Text: "c06_doubles"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `""`},
		{Type: Whitespace, Text: " \t"},
		{Type: Literal, Text: `"\""`},
		{Type: Semicolon, Text: ";"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `";\""`},
	},
	{
		{Type: Word, Text: "c07_singles"},
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
		{Type: Word, Text: "c08_doubles"},
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
		{Type: Word, Text: "c09"},
		{Type: Whitespace, Text: " "},
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
		{Type: Word, Text: "c10"},
		{Type: Punctuation, Text: "-//"},
	},
	{
		{Type: Word, Text: "c11"},
		{Type: Punctuation, Text: "-//-/-"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c12"},
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
		{Type: Word, Text: "c13"},
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
		{Type: Word, Text: "c14"},
		{Type: Punctuation, Text: "-"},
		{Type: Comment, Text: "/*;*/"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-/"},
		{Type: Comment, Text: "/*\n\t;*/"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c15"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".5"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c16"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".5"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0.5"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "30.5"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "40"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "40.13"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "40.15e8"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "40e8"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".4e8"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".4e20"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c17"},
		{Type: Whitespace, Text: " "},
		{Type: Comment, Text: "/* foo \n */"},
	},
	{
		{Type: Word, Text: "c18"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "'unterminated "},
	},
	{
		{Type: Word, Text: "c19"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `"unterminated `},
	},
	{
		{Type: Word, Text: "c20"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `'unterminated \`},
	},
	{
		{Type: Word, Text: "c21"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `"unterminated \`},
	},
	{
		{Type: Word, Text: "c22"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: ".@"},
	},
	{
		{Type: Word, Text: "c23"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: ".@"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c24"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7"},
		{Type: Word, Text: "ee"},
	},
	{
		{Type: Word, Text: "c25"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7"},
		{Type: Word, Text: "eg"},
	},
	{
		{Type: Word, Text: "c26"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7"},
		{Type: Word, Text: "ee"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c27"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7"},
		{Type: Word, Text: "eg"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c28"},
		{Type: Whitespace, Text: " "},
		{Type: Comment, Text: "/* foo "},
	},
	{
		{Type: Word, Text: "c29"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7e8"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c30"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7e8"},
	},
	{
		{Type: Word, Text: "c31"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7.0"},
		{Type: Word, Text: "e"},
	},
	{
		{Type: Word, Text: "c32"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "7.0"},
		{Type: Word, Text: "e"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c33"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "eèҾ"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "ҾeèҾ"},
	},
	{
		{Type: Word, Text: "c34"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "⁖"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "+⁖"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "+⁖*"},
	},
	{
		{Type: Word, Text: "c35"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "๒"},
	},
	{
		{Type: Word, Text: "c36"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c37"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "๒⎖๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c38"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "⎖๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c39"},
		{Type: Whitespace, Text: " "},
		{Type: Comment, Text: "-- comment w/o end"},
	},
	{
		{Type: Word, Text: "c40"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: ".๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c40"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "abnormal"},
		{Type: Whitespace, Text: " "}, // this is a unicode space character
		{Type: Word, Text: "space"},
	},
	{
		{Type: Word, Text: "c41"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "abnormal"},
		{Type: Whitespace, Text: "  "}, // this is a unicode space character
		{Type: Word, Text: "space"},
	},
	{
		{Type: Word, Text: "c42"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "abnormal"},
		{Type: Whitespace, Text: "  "}, // this is a unicode space character
		{Type: Word, Text: "space"},
	},
	{
		{Type: Word, Text: "c43"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "."},
	},
	{
		{Type: Word, Text: "c44"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "๒๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c45"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3e๒๒๒"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "c46"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7"},
	},
	{
		{Type: Word, Text: "c47"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7e19"},
	},
	{
		{Type: Word, Text: "c48"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "3.7e2"},
	},
	{
		{Type: Word, Text: "c49"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "😀"}, // I'm not sure I agree with the classification
	},
	{
		{Type: Word, Text: "c50"},
		{Type: Whitespace, Text: " \x00"},
	},
	{
		{Type: Word, Text: "c51"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "x"},
		{Type: Whitespace, Text: "\x00"},
	},
}

var mySQLCases = []Tokens{
	{
		{Type: Word, Text: "m01"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "-"},
		{Type: Comment, Text: "# /# #;\n"},
		{Type: Whitespace, Text: "\t"},
		{Type: Word, Text: "foo"},
	},
	{
		{Type: Word, Text: "m02"},
		{Type: Whitespace, Text: " "},
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
	{
		{Type: Word, Text: "m03"},
		{Type: Whitespace, Text: " "},
		{Type: QuestionMark, Text: "?"},
		{Type: QuestionMark, Text: "?"},
	},
	{
		{Type: Word, Text: "m04"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$"},
		{Type: Number, Text: "5"},
	},
	{
		{Type: Word, Text: "m05"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "U"},
		{Type: Punctuation, Text: "&"},
		{Type: Literal, Text: `'d\0061t\+000061'`},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m06"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0x1f"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "x'1f'"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "X'1f'"},
	},
	{
		{Type: Word, Text: "m07"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0b01"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "b'010'"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "B'110'"},
	},
	{
		{Type: Word, Text: "m08"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0b01"},
	},
	{
		{Type: Word, Text: "m09"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0x01"},
	},
	{
		{Type: Word, Text: "m10"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "x'1f"},
		{Type: Punctuation, Text: "&"},
	},
	{
		{Type: Word, Text: "m10"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "b'1"},
		{Type: Number, Text: "7"},
	},
	{
		{Type: Word, Text: "m11"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$$"},
		{Type: Word, Text: "footext"},
		{Type: Punctuation, Text: "$$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "m12"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "b'10"},
	},
	{
		{Type: Word, Text: "m13"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "x'1f"},
	},
}

var postgreSQLCases = []Tokens{
	{
		{Type: Word, Text: "p01"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "#"},
		{Type: Word, Text: "foo"},
		{Type: Whitespace, Text: "\n"},
	},
	{
		{Type: Word, Text: "p02"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "?"},
		{Type: Whitespace, Text: "\n"},
	},
	{
		{Type: Word, Text: "p03"},
		{Type: Whitespace, Text: " "},
		{Type: DollarNumber, Text: "$17"},
		{Type: DollarNumber, Text: "$8"},
	},
	{
		{Type: Word, Text: "p04"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: `U&'d\0061t\+000061'`},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p05"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0"},
		{Type: Word, Text: "x1f"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "x"},
		{Type: Literal, Text: "'1f'"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "X"},
		{Type: Literal, Text: "'1f'"},
	},
	{
		{Type: Word, Text: "p06"},
		{Type: Whitespace, Text: " "},
		{Type: Number, Text: "0"},
		{Type: Word, Text: "b01"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "b"},
		{Type: Literal, Text: "'010'"},
		{Type: Whitespace, Text: " "},
		{Type: Word, Text: "B"},
		{Type: Literal, Text: "'110'"},
	},
	{
		{Type: Word, Text: "p07"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$$footext$$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p08"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$$foo!text$$"},
	},
	{
		{Type: Word, Text: "p09"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$q$foo$$text$q$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p10"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$q$foo$$text$q$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p11"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p12"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$$"},
	},
	{
		{Type: Word, Text: "p13"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$"},
		{Type: Word, Text: "q"},
		{Type: Punctuation, Text: "$"},
		{Type: Whitespace, Text: " "},
	},
	{
		{Type: Word, Text: "p14"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$ҾeèҾ$ $ DLa 32498 $ҾeèҾ$"},
		{Type: Punctuation, Text: "$"},
	},
	{
		{Type: Word, Text: "p15"},
		{Type: Whitespace, Text: " "},
		{Type: Literal, Text: "$ҾeèҾ$ $ DLa 32498 $ҾeèҾ$"},
	},
	{
		{Type: Word, Text: "p16"},
		{Type: Whitespace, Text: " "},
		{Type: Punctuation, Text: "$"},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "-$"},
		{Type: Word, Text: "bar"},
		{Type: Punctuation, Text: "$"},
		{Type: Word, Text: "foo"},
		{Type: Punctuation, Text: "-$"},
		{Type: Whitespace, Text: " "},
	},
}

func doTestInner(t *testing.T, tc Tokens, f func(string) Tokens) {
	text := tc.String()
	t.Log("---------------------------------------")
	t.Log(text)
	t.Log("-----------------")
	got := f(text)
	require.Equal(t, text, got.String(), tc.String())
	require.Equal(t, tc, got, tc.String())
}

func doTest(t *testing.T, tc Tokens, f func(string) Tokens) {
	if len(tc) == 0 {
		t.Run("null", func(t *testing.T) {
			doTestInner(t, tc, f)
			return
		})
		return
	}
	t.Run(tc[0].Text, func(t *testing.T) {
		doTestInner(t, tc, f)
		return
	})
}

func testMySQL(t *testing.T, tc Tokens) {
	doTest(t, tc, func(s string) Tokens {
		return TokenizeMySQL(s)
	})
}

func testPostgreSQL(t *testing.T, tc Tokens) {
	doTest(t, tc, func(s string) Tokens {
		return TokenizePostgreSQL(s)
	})
}

func TestMySQLTokenizing(t *testing.T) {
	for _, tc := range commonCases {
		testMySQL(t, tc)
	}
	for _, tc := range mySQLCases {
		testMySQL(t, tc)
	}
}

func TestPostgresSQLTokenizing(t *testing.T) {
	for _, tc := range commonCases {
		testPostgreSQL(t, tc)
	}
	for _, tc := range postgreSQLCases {
		testPostgreSQL(t, tc)
	}
}
