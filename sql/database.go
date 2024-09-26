package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
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
	insertStatements map[string]*sql.Stmt
	mapOutputType    string
	descriptor       *desc.FileDescriptor
}

//todo: handle schema change with version and start block

func NewDatabase(schema *Schema, db *sql.DB, mapOutputType string, descriptor *desc.FileDescriptor, logger *zap.Logger) (*Database, error) {
	err := generateTablesCreate(schema, descriptor)
	if err != nil {
		return nil, fmt.Errorf("generating create queries: %w", err)
	}

	_, err = db.Exec(fmt.Sprintf(static_sql, schema.String(), schema.String(), schema.String(), schema.String(), schema.String()))
	if err != nil {
		return nil, fmt.Errorf("executing static sql: %w", err)
	}
	fmt.Println("static sql executed")

	for _, statement := range schema.tableCreateStatements {
		_, err = db.Exec(statement)
		if err != nil {
			return nil, fmt.Errorf("executing create statement: %w %s", err, statement)
		}
	}
	fmt.Println("table create statements executed")

	statements, err := generateStatements(schema, db, descriptor)
	if err != nil {
		return nil, fmt.Errorf("generating insertStatements: %w", err)
	}

	return &Database{
		schema:           schema,
		db:               db,
		logger:           logger,
		insertStatements: statements,
		mapOutputType:    mapOutputType,
		descriptor:       descriptor,
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
	//todo: add transaction id to all entity tables

	err := d.insertBlock(blockNum, blockHash, blockTimestamp)
	if err != nil {
		return fmt.Errorf("inserting block: %w", err)
	}

	interfaces := md.GetFieldByName("transactions").([]interface{})

	for _, i := range interfaces {
		transaction, ok := i.(*dynamic.Message)
		trxHash, ok := transaction.GetFieldByName("trx_hash").(string)
		if !ok {
			return fmt.Errorf("field trx_hash is not a string")
		}

		err := d.insertTransaction(trxHash)
		if err != nil {
			return fmt.Errorf("inserting transaction: %w", err)
		}

		interfaces := transaction.GetFieldByName("entities").([]interface{})
		for _, i := range interfaces {
			entity, ok := i.(*dynamic.Message)
			if !ok {
				return fmt.Errorf("field entities is not a message")
			}
			for _, fd := range entity.GetKnownFields() {
				if fd.GetType() != descriptor.FieldDescriptorProto_TYPE_MESSAGE {
					return fmt.Errorf("field %s is not a message", fd.GetName())
				}
				fv := entity.GetField(fd)

				if reflect.ValueOf(fv).IsNil() {
					continue
				}

				fm, ok := fv.(*dynamic.Message)
				if !ok {
					return fmt.Errorf("field %s is not a message but %t", fd.GetName(), fv)
				}

				_ = fm
				_, err := d.walkMessageDescriptorAndInsert(fd.GetType().String(), fm)
				if err != nil {
					return fmt.Errorf("walking message descriptor %q: %w", fd.GetName(), err)
				}
			}
		}
	}

	return nil
}

func (d *Database) walkMessageDescriptorAndInsert(protoType string, md *dynamic.Message) (int, error) {
	key := strings.ToLower(md.GetMessageDescriptor().GetName())
	stmt, found := d.insertStatements[key]
	if !found {
		return 0, fmt.Errorf("statement not found for %s", protoType)
	}

	var fieldValues []any
	fieldValues = append(fieldValues, d.context.dbTransactionID)
	for _, fd := range md.GetKnownFields() {
		fv := md.GetField(fd)
		if fm, ok := fv.(*dynamic.Message); ok {
			id, err := d.walkMessageDescriptorAndInsert(fd.GetType().String(), fm)
			if err != nil {
				return 0, fmt.Errorf("walking nested message descriptor %q: %w", fd.GetName(), err)
			}
			fieldValues = append(fieldValues, id)
		} else {
			fieldValues = append(fieldValues, fv)
		}
	}

	row := d.tx.Stmt(stmt).QueryRow(fieldValues...)
	err := row.Err()
	if err != nil {
		return 0, fmt.Errorf("inserting %s: %w", stmt, err)
	}

	var id int
	err = row.Scan(&id)

	return id, err
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
	d.context.SetDBBlockID(id)
	return err
}

func (d *Database) insertTransaction(trxHash string) error {
	stmt := d.insertStatements["transaction"]
	row := d.tx.Stmt(stmt).QueryRow(d.context.dbBlockID, trxHash)

	err := row.Err()
	if err != nil {
		return fmt.Errorf("db inserting transaction: %w", err)
	}
	var id int
	err = row.Scan(&id)
	d.context.SetDBTransactionID(id)
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

func generateTablesCreate(schema *Schema, fileDescriptor *desc.FileDescriptor) error {
	foundOutputs := false

	for _, messageDescriptor := range fileDescriptor.GetMessageTypes() {
		name := messageDescriptor.GetName()
		if name == "Entity" {
			err := walkMessageDescriptor(schema, messageDescriptor)
			if err != nil {
				return fmt.Errorf("walking message descriptor %q: %w", messageDescriptor.GetName(), err)
			}
			foundOutputs = true
		}
	}
	if !foundOutputs {
		return fmt.Errorf("no outputs message found")
	}

	return nil
}

func walkMessageDescriptor(schema *Schema, messageDescriptor *desc.MessageDescriptor) error {
	for _, field := range messageDescriptor.GetFields() {
		if field.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			msgDesc := field.GetMessageType()
			err := walkMessageDescriptor(schema, msgDesc)
			if err != nil {
				return fmt.Errorf("walking field %q message descriptor: %w", field.GetName(), err)
			}
		}
	}

	if messageDescriptor.GetName() == "Entity" {
		return nil
	}

	create, err := createTableFromMessageDescriptor(schema, messageDescriptor)
	if err != nil {
		return fmt.Errorf("creating table from message %q descriptor: %w", messageDescriptor.GetName(), err)
	}

	schema.tableCreateStatements = append(schema.tableCreateStatements, create)
	return nil
}

type foreignKey struct {
	name         string
	table        string
	field        string
	foreignTable string
	foreignField string
}

func (f *foreignKey) String() string {
	return fmt.Sprintf("CONSTRAINT %s  FOREIGN KEY (%s) REFERENCES %s(%s)", f.name, f.field, f.foreignTable, f.foreignField)
}

func createTableFromMessageDescriptor(schema *Schema, messageDescriptor *desc.MessageDescriptor) (string, error) {
	var sb strings.Builder

	table := tableNameFromDescriptor(schema, messageDescriptor)
	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (\n", table))
	sb.WriteString("    id SERIAL PRIMARY KEY,\n")
	sb.WriteString("    transaction_id INTEGER NOT NULL,\n")

	var foreignKeys []*foreignKey
	for _, f := range messageDescriptor.GetFields() {
		field := fieldQuotedName(f)
		fieldType := mapFieldType(f)

		sb.WriteString(fmt.Sprintf("    %s %s", field, fieldType))

		if f.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			foreignKeys = append(foreignKeys, &foreignKey{
				name:         "fk_" + fieldName(f),
				table:        table,
				field:        field,
				foreignTable: tableNameFromDescriptor(schema, f.GetMessageType()),
				foreignField: "id",
			})
		}

		sb.WriteString(",\n")
	}
	sb.WriteString(fmt.Sprintf("    CONSTRAINT fk_transaction FOREIGN KEY (transaction_id) REFERENCES %s.block(id)", schema.String()))
	for i, key := range foreignKeys {
		sb.WriteString(",\n")
		sb.WriteString("    " + key.String())
		if i < len(foreignKeys)-1 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}

	sb.WriteString(");")

	return sb.String(), nil

}

func mapFieldType(field *desc.FieldDescriptor) string {
	switch field.GetType() {
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		return "INTEGER"
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return "BOOLEAN"
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		return "INTEGER"
	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return "BIGINT"
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return "DECIMAL"
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return "DOUBLE PRECISION"
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return "VARCHAR(255)" // Example, could be changed based on requirements
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return "BLOB"
	default:
		return "TEXT" // Default case
	}
}

func generateStatements(schema *Schema, db *sql.DB, fileDescriptor *desc.FileDescriptor) (map[string]*sql.Stmt, error) {
	statements := map[string]*sql.Stmt{}

	insertBlock, err := db.Prepare(fmt.Sprintf("INSERT INTO %s (number, hash, timestamp) VALUES ($1, $2, $3) RETURNING id", TableName(schema, "block")))
	if err != nil {
		panic(err)
	}

	statements["block"] = insertBlock

	insertTransaction, err := db.Prepare(fmt.Sprintf("INSERT INTO %s (block_id, hash) VALUES ($1, $2) RETURNING id", TableName(schema, "transaction")))
	if err != nil {
		panic(err)
	}

	statements["transaction"] = insertTransaction

	insertCursor, err := db.Prepare(fmt.Sprintf("INSERT INTO %s (name, cursor) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET cursor = $2", TableName(schema, "cursor")))
	if err != nil {
		panic(err)
	}

	statements["cursor"] = insertCursor

	for _, messageDescriptor := range fileDescriptor.GetMessageTypes() {
		if messageDescriptor.GetName() == "Entity" {

			for _, fieldDescriptor := range messageDescriptor.GetFields() {
				if fieldDescriptor.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
					msgDesc := fieldDescriptor.GetMessageType()
					stmt, err := statementFromMessageDescriptor(schema, db, msgDesc)
					if err != nil {
						return nil, fmt.Errorf("creating statement from message fileDescriptor: %w", err)
					}
					key := strings.ToLower(msgDesc.GetName())
					fmt.Println("mapping statement with name ", key)
					statements[key] = stmt
				}
			}
		}
	}
	return statements, nil
}

func statementFromMessageDescriptor(schema *Schema, db *sql.DB, descriptor *desc.MessageDescriptor) (*sql.Stmt, error) {
	insert := createInsertSQL(schema, descriptor)
	stmt, err := db.Prepare(insert)
	if err != nil {
		return nil, fmt.Errorf("preparing insert statement %q: %w", insert, err)
	}
	return stmt, nil
}

func createInsertSQL(schema *Schema, d *desc.MessageDescriptor) string {
	tableName := schema.String() + "." + strings.ToLower(d.GetName())
	fields := d.GetFields()
	var fieldNames = make([]string, len(fields)+1)
	placeholders := make([]string, len(fields)+1)

	fieldNames[0] = "transaction_id"
	placeholders[0] = "$1"

	for i, field := range fields {
		fieldNames[i+1] = fieldQuotedName(field)
		placeholders[i+1] = fmt.Sprintf("$%d", i+2)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING id",
		tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "))

	return insertSQL
}

func tableNameFromDescriptor(schema *Schema, d *desc.MessageDescriptor) string {
	return TableName(schema, d.GetName())
}

func TableName(schema *Schema, name string) string {
	return schema.String() + "." + strings.ToLower(name)
}

func fieldName(f *desc.FieldDescriptor) string {
	fieldNameSuffix := ""
	if f.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
		fieldNameSuffix = "_id"
	}

	return fmt.Sprintf("%s%s", strings.ToLower(f.GetName()), fieldNameSuffix)
}

func fieldQuotedName(f *desc.FieldDescriptor) string {
	return fmt.Sprintf("\"%s\"", fieldName(f))
}
