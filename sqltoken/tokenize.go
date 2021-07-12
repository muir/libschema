package sqltoken

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type TokenType string

const (
	Comment      TokenType = "comment"
	Whitespace             = "whitespace"
	QuestionMark           = "questionMark" // used in MySQL substitution
	DollarNumber           = "dollarNumber" // used in PostgreSQL substitution
	ColonWord              = "colonWord"    // used in sqlx substitution
	Literal                = "literal"      // strings
	Number                 = "number"
	Semicolon              = "semicolon"
	Punctuation            = "punctuation"
	Word                   = "word"
	Other                  = "other" // control characters and other non-printables
)

func combineOkay(t TokenType) bool {
	switch t {
	case Number, QuestionMark, DollarNumber, ColonWord:
		return false
	}
	return true
}

type Token struct {
	Type TokenType
	Text string
}

// Config specifies the behavior of Tokenize as relates to behavior
// that differs between SQL implementations
type Config struct {
	// Tokenize ? as type Question (used by MySQL)
	NoticeQuestionMark bool

	// Tokenize $7 as type DollarNumber (used by PostgreSQL)
	NoticeDollarNumber bool

	// Tokenize :word as type ColonWord (used by sqlx)
	NoticeColonWord bool

	// Tokenize # as type comment (used by MySQL)
	NoticeHashComment bool

	// $q$ stuff $q$ and $$stuff$$ quoting (used by PostgreSQL)
	NoticeDollarQuotes bool

	// NoticeHexValues 0xa0 (used by MySQL)
	NoticeHexNumbers bool
}

type Tokens []Token

func TokenizeMySQL(s string) Tokens {
	return Tokenize(s, Config{
		NoticeQuestionMark: true,
		NoticeHashComment:  true,
		NoticeHexNumbers:   true,
	})
}

func TokenizePostgreSQL(s string) Tokens {
	return Tokenize(s, Config{
		NoticeDollarNumber: true,
		NoticeDollarQuotes: true,
	})
}

// Tokenize breaks up SQL strings into Token objects.  No attempt is made
// to break successive punctuation.
func Tokenize(s string, config Config) Tokens {
	if len(s) == 0 {
		return []Token{}
	}
	tokens := make([]Token, 0, len(s)/5)
	tokenStart := 0
	var i int
	var firstDollarEnd int

	// Why is this written with Goto you might ask?  It's written
	// with goto because RE2 can't handle complex regex and PCRE
	// has external dependencies and thus isn't friendly for libraries.
	// So, it could have had a switch with a state variable, but that's
	// just a way to do goto that's lower performance.  Might as
	// well do goto the natural way.

	token := func(t TokenType) {
		fmt.Printf("> %s: {%s}\n", t, s[tokenStart:i])
		if i-tokenStart == 0 {
			return
		}
		if len(tokens) > 0 && tokens[len(tokens)-1].Type == t && combineOkay(t) {
			tokens[len(tokens)-1].Text += s[tokenStart:i]
		} else {
			tokens = append(tokens, Token{
				Type: t,
				Text: s[tokenStart:i],
			})
		}
		tokenStart = i
	}

BaseState:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '/':
			if i < len(s) && s[i] == '*' {
				goto CStyleComment
			}
			token(Punctuation)
		case '\'':
			goto SingleQuoteString
		case '"':
			goto DoubleQuoteString
		case '-':
			if i < len(s) && s[i] == '-' {
				goto SkipToEOL
			}
			token(Punctuation)
		case '#':
			if config.NoticeHashComment {
				goto SkipToEOL
			}
			token(Punctuation)
		case ';':
			token(Semicolon)
		case '?':
			if config.NoticeQuestionMark {
				token(QuestionMark)
			} else {
				token(Punctuation)
			}
		case ' ', '\n', '\r', '\t', '\b', '\v', '\f':
			goto Whitespace
		case '.':
			goto PossibleNumber
		case '~', '`', '!', '%', '^', '&', '*', '(', ')', '+', '=', '{', '}', '[', ']',
			'|', '\\', ':', '<', '>', ',':
			token(Punctuation)
		case '$':
			// $1
			// $seq$ stuff $seq$
			// $$stuff$$
			if config.NoticeDollarQuotes || config.NoticeDollarNumber {
				goto Dollar
			}
			token(Punctuation)
		case 'U':
			// U&'d\0061t\+000061'
			if i+1 < len(s) && s[i] == '&' && s[i+1] == '\'' {
				i += 2
				goto SingleQuoteString
			}
			goto Word
		case 'x', 'X':
			// X'1f' x'1f'
			if config.NoticeHexNumbers && i < len(s) && s[i] == '\'' {
				goto QuotedHexNumber
			}
			goto Word
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w' /*x*/, 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T' /*U*/, 'V', 'W' /*X*/, 'Y', 'Z',
			'_':
			goto Word
		case '0':
			if config.NoticeHexNumbers && i < len(s) && s[i] == 'x' {
				i++
				goto HexNumber
			}
			goto Number
		case /*0*/ '1', '2', '3', '4', '5', '6', '7', '8', '9':
			goto Number
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			switch {
			case unicode.IsDigit(r):
				goto Number
			case unicode.IsPunct(r) || unicode.IsSymbol(r) || unicode.IsMark(r):
				i += w - 1
				token(Punctuation)
			case unicode.IsLetter(r):
				goto Word
			case unicode.IsControl(r) || unicode.IsSpace(r):
				goto Whitespace
			default:
				i += w - 1
				token(Other)
			}
		}
	}
	goto Done

CStyleComment:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '*':
			if i < len(s) && s[i] == '/' {
				i++
				token(Comment)
				goto BaseState
			}
		}
	}
	token(Comment)
	goto Done

SingleQuoteString:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '\'':
			token(Literal)
			goto BaseState
		case '\\':
			if i < len(s) {
				i++
			} else {
				token(Literal)
				goto Done
			}
		}
	}
	token(Literal)
	goto Done

DoubleQuoteString:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '"':
			token(Literal)
			goto BaseState
		case '\\':
			if i < len(s) {
				i++
			} else {
				token(Literal)
				goto Done
			}
		}
	}
	token(Literal)
	goto Done

SkipToEOL:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '\n':
			token(Comment)
			goto BaseState
		}
	}
	token(Comment)
	goto Done

Word:
	for i < len(s) {
		c := s[i]
		switch c {
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'_',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			i++
			continue
		}
		r, w := utf8.DecodeRuneInString(s[i:])
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			i += w
			continue
		}
		token(Word)
		goto BaseState
	}
	token(Word)
	goto Done

PossibleNumber:
	if i < len(s) {
		c := s[i]
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			i++
			goto NumberNoDot
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			i += w - 1
			if unicode.IsDigit(r) {
				i++
				goto NumberNoDot
			}
			token(Punctuation)
			goto BaseState
		}
	}
	token(Punctuation)
	goto Done

Number:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// okay
		case '.':
			goto NumberNoDot
		case 'e', 'E':
			if i < len(s) {
				switch s[i] {
				case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
					i++
					goto Exponent
				}
			}
			token(Number)
			goto Word
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if !unicode.IsDigit(r) {
				token(Number)
				goto BaseState
			}
			i += w - 1
		}
	}
	token(Number)
	goto Done

NumberNoDot:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// okay
		case 'e', 'E':
			if i < len(s) {
				switch s[i] {
				case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
					i++
					goto Exponent
				}
			}
			token(Number)
			goto Word
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if !unicode.IsDigit(r) {
				token(Number)
				goto BaseState
			}
			i += w - 1
		}
	}
	token(Number)
	goto Done

Exponent:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// okay
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if !unicode.IsDigit(r) {
				token(Number)
				goto BaseState
			}
			i += w - 1
		}
	}
	token(Number)
	goto Done

HexNumber:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'a', 'b', 'c', 'd', 'e', 'f',
			'A', 'B', 'C', 'D', 'E', 'F':
			// okay
		default:
			token(Number)
			goto BaseState
		}
	}
	token(Number)
	goto Done

Whitespace:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case ' ', '\n', '\r', '\t', '\b', '\v', '\f':
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if !unicode.IsSpace(r) && !unicode.IsControl(r) {
				i--
				token(Whitespace)
				goto BaseState
			}
			i += w - 1
		}
	}
	token(Whitespace)
	goto Done

QuotedHexNumber:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'a', 'b', 'c', 'd', 'e', 'f',
			'A', 'B', 'C', 'D', 'E', 'F':
			// okay
		case '\'':
			i++
			token(Number)
			goto BaseState
		default:
			token(Number)
			goto BaseState
		}
	}
	token(Number)
	goto Done

	// $1
	// $seq$ stuff $seq$
	// $$stuff$$
Dollar:
	firstDollarEnd = i
	if i < len(s) {
		c := s[i]
		if config.NoticeDollarQuotes {
			if c == '$' {
				e := strings.Index(s[i+1:], "$$")
				if e == -1 {
					i = firstDollarEnd
					token(Punctuation)
					goto BaseState
				}
				i += 3 + e
				token(Literal)
				goto BaseState
			}
			r, w := utf8.DecodeRuneInString(s[i:])
			if unicode.IsLetter(r) {
				i += w
				for i < len(s) {
					c := s[i]
					r, w := utf8.DecodeRuneInString(s[i:])
					i++
					if c == '$' {
						endToken := s[tokenStart:i]
						e := strings.Index(s[i:], endToken)
						if e == -1 {
							i = firstDollarEnd
							token(Punctuation)
							goto BaseState
						}
						i += e + len(endToken) + 1
						token(Literal)
						goto BaseState
					} else if unicode.IsLetter(r) {
						i += w - 1
						continue
					} else {
						i = firstDollarEnd
						token(Punctuation)
						goto BaseState
					}
				}
			}
		}
		if config.NoticeDollarNumber {
			switch c {
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				i++
				for i < len(s) {
					c := s[i]
					i++
					switch c {
					case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
						continue
					}
					token(DollarNumber)
					goto BaseState
				}
			}
		}
		token(Punctuation)
		goto BaseState
	}
	token(Punctuation)
	goto Done

Done:
	return tokens
}

func (ts Tokens) String() string {
	strs := make([]string, len(ts))
	for i, t := range ts {
		strs[i] = t.Text
	}
	return strings.Join(strs, "")
}