package lsmysql

// SplitCommands breaks up a SQL command string on the semi-colons that
// split commands.  It is not fooled by semi-colons that are inside
// quoted strings nor is it fooled by semi-colons that are inside comments.
// Mysql supports --, #, and /* */ style comments.
func SplitCommands(s string) []string {
	if len(s) == 0 {
		return []string{""}
	}
	var splits []string
	splitStart := 0
	var i int

	// Why is this written with Goto you might ask?  It's written
	// with goto because RE2 can't handle complex regex and PCRE
	// has external dependencies and thus isn't friendly for libraries.
	// So, it could have had a switch with a state variable, but that's
	// just a way to do goto that's lower performance.  Might as
	// well do goto the natural way.

BaseState:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '/':
			goto PossibleCSytleComment
		case '\'':
			goto SingleQuoteString
		case '"':
			goto DoubleQuoteString
		case '-':
			goto PossibleDashComment
		case '#':
			goto SkipToEOL
		case ';':
			splits = append(splits, s[splitStart:i])
			splitStart = i
		}
	}
	goto Done

PossibleCSytleComment:
	if i < len(s) {
		c := s[i]
		switch c {
		case '*':
			i++
			goto CStyleComment
		default:
			goto BaseState
		}
	}
	goto Done

CStyleComment:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '*':
			goto PossibleEndCStyleComment
		}
	}
	goto Done

PossibleEndCStyleComment:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '*':
			continue
		case '/':
			goto BaseState
		default:
			goto CStyleComment
		}
	}
	goto Done

SingleQuoteString:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '\'':
			goto BaseState
		case '\\':
			if i < len(s) {
				i++
			} else {
				goto Done
			}
		}
	}
	goto Done

DoubleQuoteString:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '"':
			goto BaseState
		case '\\':
			if i < len(s) {
				i++
			} else {
				goto Done
			}
		}
	}
	goto Done

PossibleDashComment:
	if i < len(s) {
		switch s[i] {
		case '-':
			i++
			goto SkipToEOL
		default:
			goto BaseState
		}
	}
	goto Done

SkipToEOL:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '\n':
			goto BaseState
		}
	}
	goto Done

Done:
	if splitStart < len(s) {
		splits = append(splits, s[splitStart:])
	}
	return splits
}
