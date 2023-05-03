package lineprotocol

import (
	"strconv"
	"strings"
)

// escaper represents a set of characters that can be escaped.
type escaper struct {
	// table maps from byte value to the byte used to escape that value.
	// If an entry is zero, it doesn't need to be escaped.
	table [256]byte

	// revTable holds the inverse of table - it maps
	// from escaped value to the unescaped value.
	revTable [256]byte

	// escapes holds all the characters that need to be escaped.
	escapes string
}

// newEscaper returns an escaper that escapes the
// given characters.
func newEscaper(escapes string) *escaper {
	var e escaper
	for _, b := range escapes {
		// Note that this works because the Go escaping rules
		// for white space are the same as line-protocol's.
		q := strconv.QuoteRune(b)
		q = q[1 : len(q)-1]             // strip single quotes.
		q = strings.TrimPrefix(q, "\\") // remove backslash if present.
		e.table[byte(b)] = q[0]         // use single remaining character.
		e.revTable[q[0]] = byte(b)
	}
	e.escapes = escapes
	return &e
}

// appendEscaped returns the escaped form of s appended to buf.
func (e *escaper) appendEscaped(buf []byte, s string) []byte {
	newLen, startIndex := e.escapedLen(s)
	if newLen == len(s) {
		return append(buf, s...)
	}
	if cap(buf)-len(buf) < newLen {
		nBuf := make([]byte, len(buf), len(buf)+newLen)
		copy(nBuf, buf)
		buf = nBuf
	}
	e._escape(buf[len(buf):len(buf)+newLen], s, startIndex)
	return buf[:len(buf)+newLen]
}

// escaped returns the length that s will be after escaping
// and the index of the first character in s that needs escaping.
func (e *escaper) escapedLen(s string) (escLen, startIndex int) {
	startIndex = len(s)
	n := len(s)
	for i := 0; i < len(e.escapes); i++ {
		k := strings.IndexByte(s, e.escapes[i])
		if k == -1 {
			continue
		}
		if k < startIndex {
			startIndex = k
		}
		n += 1 + strings.Count(s[k+1:], e.escapes[i:i+1])
	}
	return n, startIndex
}

// _escape writes the escaped form of s into buf. It
// assumes buf is the correct length (as determined
// by escapedLen).
// This method should be treated as private to escaper.
func (e *escaper) _escape(buf []byte, s string, escIndex int) {
	copy(buf, s[:escIndex])
	j := escIndex
	for i := escIndex; i < len(s); i++ {
		b := s[i]
		if r := e.table[b]; r != 0 {
			buf[j] = '\\'
			buf[j+1] = r
			j += 2
		} else {
			buf[j] = b
			j++
		}
	}
}
