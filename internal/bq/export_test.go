package bq

import "io/fs"

// EmbeddedSQLForTest exposes the embedded SQL fs to external tests.
func EmbeddedSQLForTest() fs.FS { return sqlFS }
