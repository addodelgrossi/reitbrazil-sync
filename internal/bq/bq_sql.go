package bq

import (
	"embed"
	"io/fs"
)

//go:embed sql/*.sql
var embeddedSQL embed.FS

func init() {
	sub, err := fs.Sub(embeddedSQL, "sql")
	if err != nil {
		panic(err)
	}
	RegisterEmbedFS(sub)
}
