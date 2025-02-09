package parser

type TokenType int

const (
	// Special tokens
	TokenEOF TokenType = iota
	TokenError

	// Keywords
	TokenTable
	TokenStruct
	TokenInclude
	TokenNamespace
	TokenAttribute
	TokenEnum
	TokenUnion
	TokenRootType
	TokenRPCService
	TokenFileExtension
	TokenFileIdentifier

	// Punctuation / symbols
	TokenDot       // .
	TokenLCurly    // {
	TokenRCurly    // }
	TokenLParen    // (
	TokenRParen    // )
	TokenLBracket  // [
	TokenRBracket  // ]
	TokenColon     // :
	TokenSemicolon // ;
	TokenComma     // ,
	TokenEqual     // =

	// Other lexical items
	TokenIdent        // e.g., MyTable, my_field
	TokenStringLit    // e.g., "my string"
	TokenIntegerConst // e.g., 42, 0xFF
	TokenFloatConst   // e.g., 3.14
	TokenBoolConst    // e.g., true, false
)
