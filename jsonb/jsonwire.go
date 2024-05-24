// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Stuff stolen from https://github.com/go-json-experiment/json/blob/af2d5061e6c2/internal/jsonwire/decode.go#L255

package jsonb

import (
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

// appendUnquote appends the unescaped form of a JSON string in src to dst.
// Any invalid UTF-8 within the string will be replaced with utf8.RuneError,
// but the error will be specified as having encountered such an error.
// The input must be an entire JSON string with no surrounding whitespace.
func appendUnquote[Bytes ~[]byte | ~string](dst []byte, src Bytes) (v []byte, err error) {
	dst = slices.Grow(dst, len(src))

	// Consume the leading double quote.
	var i, n int

	// Consume every character in the string.
	for uint(len(src)) > uint(n) {
		// Optimize for long sequences of unescaped characters.
		noEscape := func(c byte) bool {
			return c < utf8.RuneSelf && ' ' <= c && c != '\\' && c != '"'
		}
		for uint(len(src)) > uint(n) && noEscape(src[n]) {
			n++
		}
		if uint(len(src)) <= uint(n) {
			dst = append(dst, src[i:n]...)
			return dst, nil
		}

		switch r, rn := utf8.DecodeRuneInString(string(truncateMaxUTF8(src[n:]))); {
		// Handle UTF-8 encoded byte sequence.
		// Due to specialized handling of ASCII above, we know that
		// all normal sequences at this point must be 2 bytes or larger.
		case rn > 1:
			n += rn
		// Handle escape sequence.
		case r == '\\':
			dst = append(dst, src[i:n]...)

			// Handle escape sequence.
			if uint(len(src)) < uint(n+2) {
				return dst, io.ErrUnexpectedEOF
			}
			switch r := src[n+1]; r {
			case '"', '\\', '/':
				dst = append(dst, r)
				n += 2
			case 'b':
				dst = append(dst, '\b')
				n += 2
			case 'f':
				dst = append(dst, '\f')
				n += 2
			case 'n':
				dst = append(dst, '\n')
				n += 2
			case 'r':
				dst = append(dst, '\r')
				n += 2
			case 't':
				dst = append(dst, '\t')
				n += 2
			case '0':
				dst = append(dst, '\x00')
				n += 2
			case 'u':
				if uint(len(src)) < uint(n+6) {
					if hasEscapedUTF16Prefix(src[n:], false) {
						return dst, io.ErrUnexpectedEOF
					}
					return dst, newInvalidEscapeSequenceError(src[n:])
				}
				v1, ok := parseHexUint16(src[n+2 : n+6])
				if !ok {
					return dst, newInvalidEscapeSequenceError(src[n : n+6])
				}
				n += 6

				// Check whether this is a surrogate half.
				r := rune(v1)
				if utf16.IsSurrogate(r) {
					r = utf8.RuneError // assume failure unless the following succeeds
					if uint(len(src)) < uint(n+6) {
						if hasEscapedUTF16Prefix(src[n:], true) {
							return utf8.AppendRune(dst, r), io.ErrUnexpectedEOF
						}
						err = newInvalidEscapeSequenceError(src[n-6:])
					} else if v2, ok := parseHexUint16(src[n+2 : n+6]); src[n] != '\\' || src[n+1] != 'u' || !ok {
						err = newInvalidEscapeSequenceError(src[n-6 : n+6])
					} else if r = utf16.DecodeRune(rune(v1), rune(v2)); r == utf8.RuneError {
						err = newInvalidEscapeSequenceError(src[n-6 : n+6])
					} else {
						n += 6
					}
				}

				dst = utf8.AppendRune(dst, r)
			default:
				return dst, newInvalidEscapeSequenceError(src[n : n+2])
			}
			i = n
		// Handle invalid UTF-8.
		case r == utf8.RuneError:
			dst = append(dst, src[i:n]...)
			if !utf8.FullRuneInString(string(truncateMaxUTF8(src[n:]))) {
				return dst, io.ErrUnexpectedEOF
			}
			// NOTE: An unescaped string may be longer than the escaped string
			// because invalid UTF-8 bytes are being replaced.
			dst = append(dst, "\uFFFD"...)
			n += rn
			i = n
			err = errInvalidUTF8
		// Handle invalid control characters.
		case r < ' ':
			dst = append(dst, src[i:n]...)
			return dst, newInvalidCharacterError(src[n:], "within string (expecting non-control character)")
		default:
			panic("BUG: unhandled character " + quoteRune(src[n:]))
		}
	}
	dst = append(dst, src[i:n]...)
	return dst, io.ErrUnexpectedEOF
}

func truncateMaxUTF8[Bytes ~[]byte | ~string](b Bytes) Bytes {
	// TODO(https://go.dev/issue/56948): Remove this function and
	// instead directly call generic utf8 functions wherever used.
	if len(b) > utf8.UTFMax {
		return b[:utf8.UTFMax]
	}
	return b
}

// newError and errInvalidUTF8 are injected by the "jsontext" package,
// so that these error types use the jsontext.SyntacticError type.
var (
	newError       = errors.New
	errInvalidUTF8 = errors.New("invalid UTF-8 within string")
)

// quoteRune quotes the first rune in the input.
func quoteRune[Bytes ~[]byte | ~string](b Bytes) string {
	r, n := utf8.DecodeRuneInString(string(truncateMaxUTF8(b)))
	if r == utf8.RuneError && n == 1 {
		return `'\x` + strconv.FormatUint(uint64(b[0]), 16) + `'`
	}
	return strconv.QuoteRune(r)
}

func newInvalidCharacterError[Bytes ~[]byte | ~string](prefix Bytes, where string) error {
	what := quoteRune(prefix)
	return newError("invalid character " + what + " " + where)
}

func newInvalidEscapeSequenceError[Bytes ~[]byte | ~string](what Bytes) error {
	label := "escape sequence"
	if len(what) > 6 {
		label = "surrogate pair"
	}
	needEscape := strings.IndexFunc(string(what), func(r rune) bool {
		return r == '`' || r == utf8.RuneError || unicode.IsSpace(r) || !unicode.IsPrint(r)
	}) >= 0
	if needEscape {
		return newError("invalid " + label + " " + strconv.Quote(string(what)) + " within string")
	} else {
		return newError("invalid " + label + " `" + string(what) + "` within string")
	}
}

// hasEscapedUTF16Prefix reports whether b is possibly
// the truncated prefix of a \uFFFF escape sequence.
func hasEscapedUTF16Prefix[Bytes ~[]byte | ~string](b Bytes, lowerSurrogateHalf bool) bool {
	for i := 0; i < len(b); i++ {
		switch c := b[i]; {
		case i == 0 && c != '\\':
			return false
		case i == 1 && c != 'u':
			return false
		case i == 2 && lowerSurrogateHalf && c != 'd' && c != 'D':
			return false // not within ['\uDC00':'\uDFFF']
		case i == 3 && lowerSurrogateHalf && !('c' <= c && c <= 'f') && !('C' <= c && c <= 'F'):
			return false // not within ['\uDC00':'\uDFFF']
		case i >= 2 && i < 6 && !('0' <= c && c <= '9') && !('a' <= c && c <= 'f') && !('A' <= c && c <= 'F'):
			return false
		}
	}
	return true
}

// parseHexUint16 is similar to strconv.ParseUint,
// but operates directly on []byte and is optimized for base-16.
// See https://go.dev/issue/42429.
func parseHexUint16[Bytes ~[]byte | ~string](b Bytes) (v uint16, ok bool) {
	if len(b) != 4 {
		return 0, false
	}
	for i := 0; i < 4; i++ {
		c := b[i]
		switch {
		case '0' <= c && c <= '9':
			c = c - '0'
		case 'a' <= c && c <= 'f':
			c = 10 + c - 'a'
		case 'A' <= c && c <= 'F':
			c = 10 + c - 'A'
		default:
			return 0, false
		}
		v = v*16 + uint16(c)
	}
	return v, true
}
