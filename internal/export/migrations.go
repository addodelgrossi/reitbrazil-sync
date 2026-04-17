package export

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed migrations_sqlite/*.sql
var migrationsFS embed.FS

// Migrations exposes the embedded MCP migrations so tests in other
// packages (e.g. the contract test in the MCP repo) can assert parity.
func Migrations() fs.FS {
	sub, err := fs.Sub(migrationsFS, "migrations_sqlite")
	if err != nil {
		panic(err) // unreachable: go:embed roots it at that path.
	}
	return sub
}

// ApplyMigrations runs every migrations_sqlite/*.sql file in
// lexicographic order against db.
func ApplyMigrations(db *sql.DB) error {
	fsys := Migrations()
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		if _, err := db.Exec(string(body)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
	}
	return nil
}
