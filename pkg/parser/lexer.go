package parser

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Token struct {
	Type    TokenType
	Literal string
}

type Lexer struct {
	input string
	start int
	pos   int
	width int
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

func (l *Lexer) nextToken() Token {
	l.skipWhitespace()

	// If at EOF
	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF, Literal: ""}
	}

	r, size := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = size

	// Check punctuation/symbols quickly
	switch r {
	case '.':
		l.advance()
		return Token{Type: TokenDot, Literal: "."}
	case '{':
		l.advance()
		return Token{Type: TokenLCurly, Literal: "{"}
	case '}':
		l.advance()
		return Token{Type: TokenRCurly, Literal: "}"}
	case '(':
		l.advance()
		return Token{Type: TokenLParen, Literal: "("}
	case ')':
		l.advance()
		return Token{Type: TokenRParen, Literal: ")"}
	case '[':
		l.advance()
		return Token{Type: TokenLBracket, Literal: "["}
	case ']':
		l.advance()
		return Token{Type: TokenRBracket, Literal: "]"}
	case ':':
		l.advance()
		return Token{Type: TokenColon, Literal: ":"}
	case ';':
		l.advance()
		return Token{Type: TokenSemicolon, Literal: ";"}
	case ',':
		l.advance()
		return Token{Type: TokenComma, Literal: ","}
	case '=':
		l.advance()
		return Token{Type: TokenEqual, Literal: "="}
	case '"':
		return l.lexString()
	}

	// Identifiers, keywords, numbers, booleans
	if isAlpha(r) || r == '_' {
		return l.lexIdentOrKeyword()
	}
	if isDigit(r) || r == '+' || r == '-' {
		return l.lexNumber()
	}

	// Could handle comments, etc. here
	// ...
	return Token{Type: TokenError, Literal: fmt.Sprintf("unexpected character %q", r)}
}

// lexString collects everything until the next unescaped `"`.
func (l *Lexer) lexString() Token {
	// consume the leading quote
	l.advance()
	startPos := l.pos

	for {
		if l.pos >= len(l.input) {
			// Unexpected EOF
			return Token{Type: TokenError, Literal: "unterminated string literal"}
		}
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if r == '"' {
			// found the closing quote
			str := l.input[startPos:l.pos]
			// consume the closing quote
			l.advance()
			return Token{Type: TokenStringLit, Literal: str}
		}
		l.pos += size
	}
}

func (l *Lexer) lexIdentOrKeyword() Token {
	start := l.pos
	for {
		if l.pos >= len(l.input) {
			break
		}
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if !(isAlphaNumeric(r) || r == '_') {
			break
		}
		l.pos += size
	}
	literal := l.input[start:l.pos]
	switch literal {
	case "table":
		return Token{Type: TokenTable, Literal: literal}
	case "struct":
		return Token{Type: TokenStruct, Literal: literal}
	case "include":
		return Token{Type: TokenInclude, Literal: literal}
	case "namespace":
		return Token{Type: TokenNamespace, Literal: literal}
	case "attribute":
		return Token{Type: TokenAttribute, Literal: literal}
	case "enum":
		return Token{Type: TokenEnum, Literal: literal}
	case "union":
		return Token{Type: TokenUnion, Literal: literal}
	case "root_type":
		return Token{Type: TokenRootType, Literal: literal}
	case "rpc_service":
		return Token{Type: TokenRPCService, Literal: literal}
	case "file_extension":
		return Token{Type: TokenFileExtension, Literal: literal}
	case "file_identifier":
		return Token{Type: TokenFileIdentifier, Literal: literal}
	case "true", "false":
		return Token{Type: TokenBoolConst, Literal: literal}
	}
	return Token{Type: TokenIdent, Literal: literal}
}

func (l *Lexer) lexNumber() Token {
	start := l.pos
	// simple parse until non-digit, non-dot, non-+-, or exponent marker
	hasDot := false
	for {
		if l.pos >= len(l.input) {
			break
		}
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if r == '.' {
			hasDot = true
		}
		if !(isDigit(r) || r == '.' || r == 'x' || r == 'X' ||
			r == 'e' || r == 'E' || r == '+' || r == '-') {
			break
		}
		l.pos += size
	}
	literal := l.input[start:l.pos]
	// Heuristics to decide int vs float
	if hasDot || strings.ContainsAny(literal, "eEpP") {
		return Token{Type: TokenFloatConst, Literal: literal}
	}
	return Token{Type: TokenIntegerConst, Literal: literal}
}

func (l *Lexer) advance() {
	l.pos += l.width
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if !unicode.IsSpace(r) {
			break
		}
		l.pos += size
	}
}

func isAlpha(r rune) bool {
	return unicode.IsLetter(r)
}

func isDigit(r rune) bool {
	return unicode.IsDigit(r)
}

func isAlphaNumeric(r rune) bool {
	return isAlpha(r) || isDigit(r)
}

type Table struct {
	Name   string
	Fields []Field
	Meta   []string // to hold metadata at the table level
}

type Field struct {
	Name         string
	Type         string
	DefaultValue string   // optional
	Meta         []string // to hold metadata e.g. "(deprecated)"
}
