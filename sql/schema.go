package sql

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

const static_sql = `
	CREATE SCHEMA IF NOT EXISTS "%s";
	
CREATE TABLE IF NOT EXISTS "%s".cursor (
		name TEXT PRIMARY KEY,
		cursor TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS "%s".block (
		id SERIAL PRIMARY KEY,
		number INTEGER NOT NULL,
		hash TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL
	);

	CREATE TABLE IF NOT EXISTS "%s".transaction (
		id SERIAL PRIMARY KEY,
		block_id INTEGER NOT NULL,
		hash TEXT NOT NULL UNIQUE,
		CONSTRAINT fk_block FOREIGN KEY (block_id) REFERENCES %s.block(id)
	);
`

type Schema struct {
	Name                  string
	Version               int
	tableCreateStatements []string
}

func (s *Schema) Hash() string {
	h := sha256.New()

	// Add the Name to the hash
	h.Write([]byte(s.Name))

	// Add the Version to the hash
	h.Write([]byte(fmt.Sprintf("%d", s.Version)))

	// Add the tableCreateStatements to the hash in a sorted order for consistent hashing
	sortedStatements := make([]string, len(s.tableCreateStatements))
	copy(sortedStatements, s.tableCreateStatements)
	sort.Strings(sortedStatements)
	for _, stmt := range sortedStatements {
		h.Write([]byte(stmt))
	}

	return hex.EncodeToString(h.Sum(nil))
}

func (s *Schema) String() string {
	return fmt.Sprintf("%s_%d", s.Name, s.Version)
}
