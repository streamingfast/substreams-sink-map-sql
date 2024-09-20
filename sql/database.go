package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"go.uber.org/zap"
)

// todo: database create schema
const static_sql = `
	CREATE TABLE IF NOT EXISTS hivemapper.cursor (
		name TEXT PRIMARY KEY,
		cursor TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS hivemapper.blocks (
		id SERIAL PRIMARY KEY,
		number INTEGER NOT NULL,
		hash TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL
	);

	CREATE TABLE IF NOT EXISTS hivemapper.transactions (
		id SERIAL PRIMARY KEY,
		block_id INTEGER NOT NULL,
		hash TEXT NOT NULL UNIQUE,
		CONSTRAINT fk_block FOREIGN KEY (block_id) REFERENCES hivemapper.blocks(id)
	);
`

type Schema struct {
	Name                  string
	Version               int
	tableCreateStatements []string
}

func (s *Schema) String() string {
	return fmt.Sprintf("%s_%d", s.Name, s.Version)
}

type Database struct {
	schema           *Schema
	db               *sql.DB
	tx               *sql.Tx
	logger           *zap.Logger
	insertStatements map[string]*sql.Stmt
	mapOutputType    string
	descriptor       *desc.FileDescriptor
}

func NewDatabase(schema *Schema, db *sql.DB, mapOutputType string, descriptor *desc.FileDescriptor, logger *zap.Logger) (*Database, error) {
	err := generateTablesCreate(schema, descriptor)
	if err != nil {
		return nil, fmt.Errorf("generating create queries: %w", err)
	}

	_, err = db.Exec(static_sql)
	if err != nil {
		return nil, fmt.Errorf("executing static sql: %w", err)
	}

	for _, statement := range schema.tableCreateStatements {
		_, err = db.Exec(statement)
		if err != nil {
			return nil, fmt.Errorf("executing create statement: %w %s", err, statement)
		}
	}

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

func (d *Database) ProcessEntity(data []byte) (err error) {
	defer func() {
		if err != nil {
			if d.tx != nil {
				e := d.tx.Rollback()
				err = fmt.Errorf("rolling back transaction: %w", e)
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

	return nil
}

func (d *Database) processMessage(md *dynamic.Message) error {
	//todo: create some kind context to keep block id trx id for future use.
	//todo: add transaction id to all entity tables

	blockNum := md.GetFieldByName("block_number").(int64)
	blockHash := md.GetFieldByName("block_hash").(string)

	//todo: insert block and get the ID back

	transactions := md.GetFieldByName("transactions").([]*dynamic.Message)

	for _, transaction := range transactions {
		trxHash := transaction.GetFieldByName("trx_hash").(string)
		//todo: insert transaction and get the ID back

		entities := transaction.GetFieldByName("entities").([]*dynamic.Message)
		for _, entity := range entities {
			for _, fd := range entity.GetKnownFields() {
				if fd.GetType() != descriptor.FieldDescriptorProto_TYPE_MESSAGE {
					return fmt.Errorf("field %s is not a message", fd.GetName())
				}
				fv := md.GetField(fd)
				fm, ok := fv.(*dynamic.Message)
				if !ok {
					return fmt.Errorf("field %s is not a message", fd.GetName())
				}
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
	stmt := d.insertStatements[protoType]
	var fieldValues []any
	for _, fd := range md.GetKnownFields() {
		fv := md.GetField(fd)
		if fm, ok := fv.(*dynamic.Message); ok {
			id, err := d.walkMessageDescriptorAndInsert(fd.GetType().String(), fm)
			if err != nil {
				return 0, fmt.Errorf("walking nested message descriptor %q: %w", fd.GetName(), err)
			}
			fieldValues = append(fieldValues, id)
		}
	}

	row := d.tx.Stmt(stmt).QueryRow(fieldValues...)
	err := row.Err()
	if err != nil {
		return 0, fmt.Errorf("inserting %q: %w", protoType, err)
	}

	var id int
	err = row.Scan(&id)

	return id, err
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

	table := tableName(schema, messageDescriptor)
	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (\n", table))
	sb.WriteString("    id SERIAL PRIMARY KEY,\n")

	var foreignKeys []*foreignKey
	for i, f := range messageDescriptor.GetFields() {
		field := fieldQuotedName(f)
		fieldType := mapFieldType(f)

		sb.WriteString(fmt.Sprintf("    %s %s", field, fieldType))

		if f.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			foreignKeys = append(foreignKeys, &foreignKey{
				name:         "fk_" + fieldName(f),
				table:        table,
				field:        field,
				foreignTable: tableName(schema, f.GetMessageType()),
				foreignField: "id",
			})
		}

		if i < len(messageDescriptor.GetFields())-1 || foreignKeys != nil {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}

	for i, key := range foreignKeys {
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
	for _, messageDescriptor := range fileDescriptor.GetMessageTypes() {
		if messageDescriptor.GetName() == "Entity" {

			for _, fieldDescriptor := range messageDescriptor.GetFields() {
				fmt.Println("fieldDescriptor: ", fieldDescriptor.GetName(), fieldDescriptor.GetType(), fieldDescriptor.GetMessageType())
				if fieldDescriptor.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
					msgDesc := fieldDescriptor.GetMessageType()
					stmt, err := statementFromMessageDescriptor(schema, db, msgDesc)
					if err != nil {
						return nil, fmt.Errorf("creating statement from message fileDescriptor: %w", err)
					}
					statements[messageDescriptor.GetName()] = stmt
				}
			}
		}
	}
	return statements, nil
}

func statementFromMessageDescriptor(schema *Schema, db *sql.DB, descriptor *desc.MessageDescriptor) (*sql.Stmt, error) {
	insert := createInsertSQL(schema, descriptor)
	fmt.Println("insert: ", insert)
	stmt, err := db.Prepare(insert)
	if err != nil {
		return nil, fmt.Errorf("preparing insert statement: %w", err)
	}
	return stmt, nil
}

func createInsertSQL(schema *Schema, d *desc.MessageDescriptor) string {
	tableName := schema.String() + "." + strings.ToLower(d.GetName())
	fields := d.GetFields()
	fieldNames := make([]string, len(fields))
	placeholders := make([]string, len(fields))

	for i, field := range fields {
		fieldNames[i] = fieldQuotedName(field)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING id",
		tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "))

	return insertSQL
}

func tableName(schema *Schema, d *desc.MessageDescriptor) string {
	return schema.String() + "." + strings.ToLower(d.GetName())
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
