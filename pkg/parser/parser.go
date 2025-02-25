package parser

import (
	"fmt"
	"strings"
)

// var titleCaser = cases.Title(language.Und)

// Enum represents an enum definition.
type Enum struct {
	Name     string
	BaseType string
	Values   []EnumValue
	Meta     []string
}

// EnumValue represents a single member of an enum.
type EnumValue struct {
	Name  string
	Value string // literal value (empty string if not explicitly provided)
}

// Parser holds the state of the parser.
type Parser struct {
	lexer     *Lexer
	lookahead Token
	done      bool

	tables []Table
	enums  map[string]Enum
	Errors []string

	enumSet map[string]bool
}

// NewParser creates a new parser instance.
func NewParser(input string) *Parser {
	p := &Parser{
		lexer:   NewLexer(input),
		enumSet: make(map[string]bool),
		enums:   make(map[string]Enum),
	}
	p.consume()
	return p
}

// Parse loops until the end-of-file and returns all parsed tables.
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
		// skipping struct, union, rpc_service, root_type etc. for brevity
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
	// For simplicity, treat the next token(s) as a single string for type.
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

// parseEnumDecl parses an enum definition such as:
//
//	enum Color : byte { Red = 1, Green, Blue }
func (p *Parser) parseEnumDecl() {
	// Consume the 'enum' token.
	p.consume()

	if !p.match(TokenIdent) {
		p.Errors = append(p.Errors, "expected enum name after 'enum'")
		return
	}
	enumName := p.lookahead.Literal
	p.consume() // consume the enum name

	// Parse the optional underlying base type.
	baseType := ""
	if p.match(TokenColon) {
		p.consume()              // consume ':'
		baseType = p.parseType() // e.g., "byte"
	}
	if baseType != "ubyte" {
		p.Errors = append(p.Errors, fmt.Sprintf("unsupported enum base type %q", baseType))
		return
	}

	// Optional metadata (e.g. annotations)
	meta := p.parseMetadata()

	// Expect the opening curly brace.
	p.expect(TokenLCurly)

	var enumValues []EnumValue
	// Parse enum members until the closing brace.
	for !p.match(TokenRCurly) && !p.match(TokenEOF) && !p.match(TokenError) {
		// Skip any commas between enum members.
		if p.match(TokenComma) {
			p.consume()
			continue
		}

		// Expect an identifier for the enum member.
		if !p.match(TokenIdent) {
			p.Errors = append(p.Errors, "expected enum member identifier")
			p.consume()
			continue
		}
		memberName := p.lookahead.Literal
		p.consume()

		// Optionally, an '=' followed by a literal value.
		memberValue := ""
		if p.match(TokenEqual) {
			p.consume() // consume '='
			if p.match(TokenIntegerConst) || p.match(TokenFloatConst) || p.match(TokenIdent) {
				memberValue = p.lookahead.Literal
				p.consume()
			} else {
				p.Errors = append(p.Errors, "expected literal for enum value after '='")
			}
		}
		enumValues = append(enumValues, EnumValue{
			Name:  memberName,
			Value: memberValue,
		})
		// Optionally, if there is a comma after the enum member, consume it.
		if p.match(TokenComma) {
			p.consume()
		}
	}
	p.expect(TokenRCurly)

	// Check for duplicate enum definitions.
	if p.enumSet[enumName] {
		p.Errors = append(p.Errors, fmt.Sprintf("duplicate enum definition for %s", enumName))
	} else {
		p.enumSet[enumName] = true
	}

	// Save the enum definition.
	enumDef := Enum{
		Name:     enumName,
		BaseType: baseType,
		Values:   enumValues,
		Meta:     meta,
	}
	p.enums[enumDef.Name] = enumDef
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
