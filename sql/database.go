package sql

import (
	"database/sql"
	"fmt"
	"strings"
	"substreams-sink-map-sql/proto"
	"time"

	"github.com/lib/pq"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

type Database struct {
	context          *Context
	schema           *Schema
	db               *sql.DB
	tx               *sql.Tx
	logger           *zap.Logger
	mapOutputType    string
	descriptor       *desc.FileDescriptor
	insertStatements map[string]*sql.Stmt
}

// todo: handle schema change with version and start block
func NewDatabase(schema *Schema, db *sql.DB, moduleOutputType string, descriptor *desc.FileDescriptor, logger *zap.Logger) (*Database, error) {
	_, err := db.Exec(fmt.Sprintf(static_sql, schema.String(), schema.String(), schema.String()))
	if err != nil {
		return nil, fmt.Errorf("executing static sql: %w", err)
	}
	fmt.Println("static sql executed")

	for _, statement := range schema.tableCreateStatements {
		_, err := db.Exec(statement)
		if err != nil {
			return nil, fmt.Errorf("executing create statement: %w %s", err, statement)
		}

	}
	fmt.Println("table create statements executed")

	for _, constraint := range schema.constraintStatements {
		fmt.Println("executing constraint statement: ", constraint.sql)
		_, err = db.Exec(constraint.sql)
		if err != nil {
			if e, ok := err.(*pq.Error); ok {
				if e.Code == "42710" {
					continue
				}
			}
			return nil, fmt.Errorf("executing constraint statement: %w %s", err, constraint.sql)
		}
	}

	inserts, err := generateInsertStatements(schema, db)
	if err != nil {
		return nil, fmt.Errorf("generating insertSql: %w", err)
	}

	return &Database{
		schema:           schema,
		db:               db,
		logger:           logger,
		mapOutputType:    moduleOutputType,
		descriptor:       descriptor,
		insertStatements: inserts,
	}, nil
}

func (d *Database) BeginTransaction() error {
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	d.tx = tx
	return nil
}

func (d *Database) CommitTransaction() error {
	err := d.tx.Commit()
	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	d.tx = nil
	return nil
}

func (d *Database) RollbackTransaction() error {
	err := d.tx.Rollback()
	if err != nil {
		return fmt.Errorf("rolling back transaction: %w", err)
	}

	d.tx = nil
	return nil
}

func (d *Database) ProcessEntity(data []byte, blockNum uint64, blockHash string, blockTimestamp time.Time, cursor *sink.Cursor) (err error) {
	defer func() {
		if err != nil {
			if d.tx != nil {
				e := d.tx.Rollback()
				if e != nil {
					err = fmt.Errorf("rolling back transaction: %w", e)
				}
				err = fmt.Errorf("processing entity: %w", err)
			}
			return
		}
		if d.tx != nil {
			err = d.tx.Commit()
		}

		d.tx = nil

	}()

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	d.tx = tx

	// Find the message descriptor in the file descriptor
	md := d.descriptor.FindMessage(d.mapOutputType) //output
	if md == nil {
		return fmt.Errorf("message descriptor not found for %s", d.mapOutputType)
	}

	msg := dynamic.NewMessage(md)
	err = msg.Unmarshal(data)
	if err != nil {
		return fmt.Errorf("unmarshaling message: %w", err)
	}

	err = d.processMessage(msg, blockNum, blockHash, blockTimestamp)
	if err != nil {
		return fmt.Errorf("processing message: %w", err)
	}

	err = d.insertCursor(cursor)
	if err != nil {
		return fmt.Errorf("inserting cursor: %w", err)
	}

	return nil
}

func (d *Database) processMessage(md *dynamic.Message, blockNum uint64, blockHash string, blockTimestamp time.Time) error {
	err := d.insertBlock(blockNum, blockHash, blockTimestamp)
	if err != nil {
		return fmt.Errorf("inserting block: %w", err)
	}
	_, err = d.walkMessageDescriptorAndInsert(md, nil)
	if err != nil {
		return fmt.Errorf("walking message descriptor %q: %w", md.GetMessageDescriptor().GetFullyQualifiedName(), err)
	}
	return nil
}

func (d *Database) walkMessageDescriptorAndInsert(dm *dynamic.Message, parent *Parent) (id int, err error) {
	var fieldValues []any
	fieldValues = append(fieldValues, d.context.blockNumber)
	var childs [][]interface{}
	for _, fd := range dm.GetKnownFields() {
		fv := dm.GetField(fd)
		if v, ok := fv.([]interface{}); ok {
			childs = append(childs, v)
		} else if fm, ok := fv.(*dynamic.Message); ok {
			id, err = d.walkMessageDescriptorAndInsert(fm, nil)
			if err != nil {
				return 0, fmt.Errorf("walking nested message descriptor %q: %w", fd.GetName(), err)
			}
			fieldValues = append(fieldValues, id)
		} else {
			fieldValues = append(fieldValues, fv)
		}
	}

	if parent != nil {
		fieldValues = append(fieldValues, parent.id)
	}

	md := dm.GetMessageDescriptor()
	id = -1
	var p *Parent
	if proto.IsTable(md) {
		key := md.GetFullyQualifiedName()
		stmt, found := d.insertStatements[key]
		if !found {
			return 0, fmt.Errorf("insert statement not found for key %q", key)
		}

		row := d.tx.Stmt(stmt).QueryRow(fieldValues...)
		err = row.Err()
		if err != nil {
			return 0, fmt.Errorf("inserting %s: %w", stmt, err)
		}

		err = row.Scan(&id)

		p = &Parent{
			field: strings.ToLower(md.GetName()),
			id:    id,
		}
	}

	for _, child := range childs {
		for _, c := range child {
			fm, ok := c.(*dynamic.Message)
			if !ok {
				panic("expected *dynamic.Message")
			}
			_, err = d.walkMessageDescriptorAndInsert(fm, p)
			if err != nil {
				return 0, fmt.Errorf("processing child %q: %w", fm.GetMessageDescriptor().GetFullyQualifiedName(), err)
			}
		}
	}

	return id, err
}

type Parent struct {
	field string
	id    int
}

func (d *Database) insertBlock(num uint64, hash string, timestamp time.Time) error {
	stmt := d.insertStatements["block"]
	row := d.tx.Stmt(stmt).QueryRow(num, hash, timestamp)

	err := row.Err()
	if err != nil {
		return fmt.Errorf("inserting %q block: %w", stmt, err)
	}

	var id int
	err = row.Scan(&id)

	d.context = NewContext() //also act as a reset
	d.context.SetNumber(id)
	return err
}

func (d *Database) insertCursor(cursor *sink.Cursor) error {
	stmt := d.insertStatements["cursor"]
	_, err := d.tx.Stmt(stmt).Exec("map-sinker", cursor.String())

	if err != nil {
		return fmt.Errorf("inserting cursor: %w", err)
	}

	return err
}

func (d *Database) FetchCursor() (*sink.Cursor, error) {
	rows, err := d.db.Query(fmt.Sprintf("SELECT cursor FROM %s WHERE name = $1", TableName(d.schema, "cursor")), "hivemapper")
	if err != nil {
		return nil, fmt.Errorf("selecting cursor: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var cursor string
		err = rows.Scan(&cursor)

		return sink.NewCursor(cursor)
	}
	return nil, nil
}

func generateInsertStatements(schema *Schema, db *sql.DB) (map[string]*sql.Stmt, error) {
	statements := make(map[string]*sql.Stmt)
	for n, s := range schema.insertSql {
		stmt, err := db.Prepare(s)
		if err != nil {
			return nil, fmt.Errorf("preparing statement %q: %w", s, err)
		}
		statements[n] = stmt
	}

	return statements, nil
}
