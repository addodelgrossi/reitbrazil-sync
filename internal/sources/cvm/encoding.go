package cvm

import (
	"bufio"
	"io"
	"unicode/utf8"
)

// newUTF8Reader wraps r so the caller can read bytes as UTF-8 even if
// the source is Latin-1 encoded (CVM CSVs were Latin-1 until 2020).
// Detection is BOM-first with a utf8.Valid fallback.
func newUTF8Reader(r io.Reader) io.Reader {
	br := bufio.NewReaderSize(r, 64*1024)
	// Strip UTF-8 BOM if present.
	if peek, err := br.Peek(3); err == nil && len(peek) == 3 &&
		peek[0] == 0xEF && peek[1] == 0xBB && peek[2] == 0xBF {
		_, _ = br.Discard(3)
		return br
	}
	// If the first 4KB parses as valid UTF-8, assume UTF-8. Short reads
	// (err == io.EOF) are fine — bufio returns the partial buffer, and a
	// valid UTF-8 prefix is strong evidence the rest is UTF-8 too.
	peek, _ := br.Peek(4096)
	if utf8.Valid(peek) {
		return br
	}
	return &latin1Reader{r: br}
}

// latin1Reader transcodes Latin-1 bytes to UTF-8 on the fly. It only
// needs to handle bytes 0x80-0xFF (single-byte code points in Latin-1
// mapping to U+0080..U+00FF).
type latin1Reader struct {
	r   io.Reader
	buf []byte
}

func (l *latin1Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	// Worst case: every byte expands to two UTF-8 bytes. Read at most
	// half of p at a time to keep the output within bounds.
	readLen := len(p) / 2
	if readLen < 1 {
		readLen = 1
	}
	if cap(l.buf) < readLen {
		l.buf = make([]byte, readLen)
	}
	l.buf = l.buf[:readLen]
	n, err := l.r.Read(l.buf)
	out := 0
	for i := 0; i < n; i++ {
		b := l.buf[i]
		if b < 0x80 {
			p[out] = b
			out++
		} else {
			// U+0080..U+00FF encodes as 110xxxxx 10xxxxxx in UTF-8.
			p[out] = 0xC0 | (b >> 6)
			p[out+1] = 0x80 | (b & 0x3F)
			out += 2
		}
	}
	return out, err
}
