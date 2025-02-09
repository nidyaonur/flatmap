package parser

import (
	"fmt"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type Parser struct {
	lexer     *Lexer
	lookahead Token
	done      bool

	tables []Table
	Errors []string

	enumSet map[string]bool
}

var titleCaser = cases.Title(language.Und)

func NewParser(input string) *Parser {
	p := &Parser{
		lexer:   NewLexer(input),
		enumSet: make(map[string]bool),
	}
	p.consume()
	return p
}

func (p *Parser) Parse() []Table {
	// We loop until EOF
	for !p.match(TokenEOF) && !p.match(TokenError) {
		switch p.lookahead.Type {
		case TokenInclude:
			p.parseInclude()
		case TokenNamespace:
			p.parseNamespace()
		case TokenAttribute:
			p.parseAttribute()
		case TokenTable:
			p.parseTable()
		case TokenEnum:
			p.parseEnumDecl()
		// skipping struct, enum, union, rpc_service, root_type etc. for brevity
		default:
			// If it's something else, just consume to avoid infinite loop
			p.consume()
		}
	}
	return p.tables
}

func (p *Parser) parseInclude() {
	// `include` string_constant `;`
	p.consume() // consume 'include'
	if p.match(TokenStringLit) {
		p.consume() // skip the string
	}
	p.expect(TokenSemicolon)
}
func (p *Parser) parseNamespace() {
	// `namespace` ident ( '.' ident )* ';'
	p.consume() // consume 'namespace'

	// we expect at least one identifier
	if p.match(TokenIdent) {
		p.consume() // first ident
		// then possibly more pairs of (TokenDot, TokenIdent)
		for p.match(TokenDot) {
			p.consume() // consume the '.'
			if p.match(TokenIdent) {
				p.consume() // consume the ident after '.'
			} else {
				p.Errors = append(p.Errors, "expected identifier after '.' in namespace")
				return
			}
		}
	} else {
		p.Errors = append(p.Errors, "expected identifier after 'namespace'")
		return
	}

	p.expect(TokenSemicolon)
}

func (p *Parser) parseAttribute() {
	// `attribute` ident|`"..."` ';'
	p.consume() // consume 'attribute'
	if p.match(TokenIdent) || p.match(TokenStringLit) {
		p.consume()
	}
	p.expect(TokenSemicolon)
}

func (p *Parser) parseTable() {
	// `table` ident metadata `{` field_decl+ `}`
	p.consume() // consume 'table'
	tableName := ""
	if p.match(TokenIdent) {
		tableName = p.lookahead.Literal
		p.consume()
	}
	tableMeta := p.parseMetadata()

	p.expect(TokenLCurly)

	var fields []Field
	for !p.match(TokenRCurly) && !p.match(TokenEOF) && !p.match(TokenError) {
		f := p.parseFieldDecl()
		if f.Name != "" {
			fields = append(fields, f)
		}
	}

	p.expect(TokenRCurly)

	p.tables = append(p.tables, Table{
		Name:   tableName,
		Fields: fields,
		Meta:   tableMeta,
	})
}

func (p *Parser) parseFieldDecl() Field {
	// field_decl = ident ':' type [ '=' scalar ] metadata ';'
	field := Field{}
	if p.match(TokenIdent) {
		field.Name = snakeToPascal(p.lookahead.Literal)
		p.consume()
	} else {
		// error or skip
		return field
	}

	p.expect(TokenColon)
	field.Type = p.parseType()

	// optional default
	if p.match(TokenEqual) {
		p.consume()
		if p.match(TokenBoolConst) || p.match(TokenIntegerConst) ||
			p.match(TokenFloatConst) {
			field.DefaultValue = p.lookahead.Literal
			p.consume()
		} else {
			// error or skip
		}
	}

	field.Meta = p.parseMetadata()

	p.expect(TokenSemicolon)
	return field
}

func (p *Parser) parseType() string {
	// type = `bool` | `byte` | `ubyte` | ... | `string` | `[` type `]` | ident
	// For simplicity, treat the next token(s) as a single string for type
	// A real parser would parse nested bracket types carefully.
	if p.match(TokenIdent) ||
		p.matchKeywordType(p.lookahead.Literal) {
		t := p.lookahead.Literal
		p.consume()
		return t
	} else if p.match(TokenLBracket) {
		p.consume()
		inner := p.parseType()
		p.expect(TokenRBracket)
		return "[" + inner + "]"
	}
	// error or default
	return ""
}

// parseMetadata = [ '(' commasep( ident [ ':' single_value ] ) ')' ]
func (p *Parser) parseMetadata() []string {
	var results []string
	if p.match(TokenLParen) {
		p.consume() // skip '('
		// Naively: gather all tokens until ')'
		metaBuilder := strings.Builder{}
		for !p.match(TokenRParen) && !p.match(TokenEOF) && !p.match(TokenError) {
			metaBuilder.WriteString(p.lookahead.Literal)
			if p.lookahead.Type != TokenComma {
				metaBuilder.WriteRune(' ')
			}
			p.consume()
		}
		results = append(results, strings.TrimSpace(metaBuilder.String()))
		p.expect(TokenRParen)
	}
	return results
}

// Utility methods
func (p *Parser) match(t TokenType) bool {
	return p.lookahead.Type == t
}

func (p *Parser) matchKeywordType(lit string) bool {
	// recognized scalar types
	switch lit {
	case "bool", "byte", "ubyte", "short", "ushort", "int", "uint",
		"float", "long", "ulong", "double", "int8", "uint8",
		"int16", "uint16", "int32", "uint32", "int64", "uint64",
		"float32", "float64", "string":
		return true
	}
	return false
}

func (p *Parser) expect(t TokenType) {
	if !p.match(t) {
		// record error or panic
		p.Errors = append(p.Errors, fmt.Sprintf("expected %v, got %v (%q)", t, p.lookahead.Type, p.lookahead.Literal))
	}
	p.consume()
}

func (p *Parser) consume() {
	if p.done {
		return
	}
	tok := p.lexer.nextToken()
	p.lookahead = tok
	if tok.Type == TokenEOF {
		p.done = true
	}
	if tok.Type == TokenError {
		p.Errors = append(p.Errors, tok.Literal)
		p.done = true
	}
}

func (p *Parser) parseEnumDecl() {
	// We already know the current token is TokenEnum, so consume it
	p.consume() // consume 'enum'

	if !p.match(TokenIdent) {
		p.Errors = append(p.Errors, "expected enum name after 'enum'")
		return
	}
	enumName := p.lookahead.Literal
	p.consume() // consume the enum name

	// skip the ':' type portion for simplicity (or parse it if needed)
	if p.match(TokenColon) {
		p.consume() // consume ':'
		// parse the underlying base type (e.g. int, byte, etc.)
		// but we are ignoring it because we force them all to int8
		p.parseType()
	}

	// optional metadata
	_ = p.parseMetadata()

	// now expect '{ ... }'
	p.expect(TokenLCurly)
	// skip the contents until we reach '}'
	// real parser would parse each enumval_decl, but we don't need them
	for !p.match(TokenRCurly) && !p.match(TokenEOF) && !p.match(TokenError) {
		p.consume()
	}
	p.expect(TokenRCurly)

	// record in enumSet
	p.enumSet[enumName] = true
}
