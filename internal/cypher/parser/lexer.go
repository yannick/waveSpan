// Package parser implements a hand-written lexer and recursive-descent parser for WaveSpan's v1
// Cypher subset (design/07 "Cypher subset"): MATCH/OPTIONAL MATCH, WHERE, CREATE, SET, DELETE,
// RETURN, WITH, UNWIND, ORDER BY, SKIP, LIMIT. MERGE/REMOVE/DETACH DELETE are recognized and
// rejected with an explicit "unsupported in v1" error rather than silently ignored.
package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType classifies a lexeme.
type TokenType int

// Token types.
const (
	TokEOF TokenType = iota
	TokKeyword
	TokIdent
	TokInt
	TokFloat
	TokString
	TokParam // $name
	TokPunct // operators and punctuation
)

// Token is a single lexeme.
type Token struct {
	Type TokenType
	Val  string
	Pos  int
}

var keywords = map[string]bool{
	"MATCH": true, "OPTIONAL": true, "WHERE": true, "CREATE": true, "SET": true,
	"DELETE": true, "RETURN": true, "WITH": true, "UNWIND": true, "AS": true,
	"ORDER": true, "BY": true, "SKIP": true, "LIMIT": true, "DISTINCT": true,
	"AND": true, "OR": true, "NOT": true, "NULL": true, "TRUE": true, "FALSE": true,
	"ASC": true, "DESC": true, "IN": true, "CALL": true,
	// recognized-but-unsupported (reject explicitly in the parser)
	"MERGE": true, "REMOVE": true, "DETACH": true, "LOAD": true,
}

// Lex tokenizes a Cypher query. It returns an error on an unterminated string or unknown character.
func Lex(input string) ([]Token, error) {
	var toks []Token
	i, n := 0, len(input)
	for i < n {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'' || c == '"':
			s, ni, err := lexString(input, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, Token{TokString, s, i})
			i = ni
		case c == '$':
			j := i + 1
			for j < n && isIdentChar(rune(input[j])) {
				j++
			}
			toks = append(toks, Token{TokParam, input[i+1 : j], i})
			i = j
		case isDigit(c) || (c == '.' && i+1 < n && isDigit(input[i+1])):
			tok, ni := lexNumber(input, i)
			toks = append(toks, tok)
			i = ni
		case isIdentStart(rune(c)):
			j := i + 1
			for j < n && isIdentChar(rune(input[j])) {
				j++
			}
			word := input[i:j]
			if keywords[strings.ToUpper(word)] {
				toks = append(toks, Token{TokKeyword, strings.ToUpper(word), i})
			} else {
				toks = append(toks, Token{TokIdent, word, i})
			}
			i = j
		default:
			op, ni := lexPunct(input, i)
			if op == "" {
				return nil, fmt.Errorf("cypher: unexpected character %q at %d", c, i)
			}
			toks = append(toks, Token{TokPunct, op, i})
			i = ni
		}
	}
	toks = append(toks, Token{TokEOF, "", i})
	return toks, nil
}

func lexString(input string, i int) (string, int, error) {
	quote := input[i]
	var sb strings.Builder
	j := i + 1
	for j < len(input) {
		c := input[j]
		if c == '\\' && j+1 < len(input) {
			sb.WriteByte(input[j+1])
			j += 2
			continue
		}
		if c == quote {
			return sb.String(), j + 1, nil
		}
		sb.WriteByte(c)
		j++
	}
	return "", 0, fmt.Errorf("cypher: unterminated string at %d", i)
}

func lexNumber(input string, i int) (Token, int) {
	j := i
	isFloat := false
	for j < len(input) && (isDigit(input[j]) || input[j] == '.') {
		if input[j] == '.' {
			isFloat = true
		}
		j++
	}
	if isFloat {
		return Token{TokFloat, input[i:j], i}, j
	}
	return Token{TokInt, input[i:j], i}, j
}

// multi-char operators first, then single-char punctuation.
var multiPunct = []string{"<>", "<=", ">=", "->", "<-", "=~"}

func lexPunct(input string, i int) (string, int) {
	for _, op := range multiPunct {
		if strings.HasPrefix(input[i:], op) {
			return op, i + len(op)
		}
	}
	switch input[i] {
	case '(', ')', '[', ']', '{', '}', ':', ',', '.', '-', '>', '<', '=', '+', '*', '/', '|':
		return string(input[i]), i + 1
	}
	return "", i
}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(r rune) bool { return unicode.IsLetter(r) || r == '_' }
func isIdentChar(r rune) bool  { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }
