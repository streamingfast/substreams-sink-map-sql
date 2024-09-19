package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"go.uber.org/zap"
)

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
}

func NewDatabase(schema *Schema, db *sql.DB, descriptor *desc.FileDescriptor, logger *zap.Logger) (*Database, error) {

	err := generateTablesCreate(schema, descriptor)
	if err != nil {
		return nil, fmt.Errorf("generating create queries: %w", err)
	}

	for _, statement := range schema.tableCreateStatements {
		_, err = db.Exec(statement)
		if err != nil {
			return nil, fmt.Errorf("executing create statement: %w %s", err, statement)
		}
	}

	fmt.Println("-------------------------------------- ")
	fmt.Println("Create statements:")
	fmt.Println("-------------------------------------- ")

	for _, query := range schema.tableCreateStatements {
		fmt.Println(query)
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
	}, nil
}

func (p *Database) BeginTransaction() error {
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	p.tx = tx
	return nil
}

func (p *Database) CommitTransaction() error {
	err := p.tx.Commit()
	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	p.tx = nil
	return nil
}

func (p *Database) RollbackTransaction() error {
	err := p.tx.Rollback()
	if err != nil {
		return fmt.Errorf("rolling back transaction: %w", err)
	}

	p.tx = nil
	return nil
}

func generateTablesCreate(schema *Schema, fileDescriptor *desc.FileDescriptor) error {
	foundOutputs := false

	for _, messageDescriptor := range fileDescriptor.GetMessageTypes() {
		name := messageDescriptor.GetName()
		if name == "Output" {
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
	create, err := createTableFromMessageDescriptor(schema, messageDescriptor)
	if err != nil {
		return fmt.Errorf("creating table from message %q descriptor: %w", messageDescriptor.GetName(), err)
	}

	schema.tableCreateStatements = append(schema.tableCreateStatements, create)
	return nil
}

func createTableFromMessageDescriptor(schema *Schema, messageDescriptor *desc.MessageDescriptor) (string, error) {
	var sb strings.Builder

	tableName := schema.String() + "." + messageDescriptor.GetName()
	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (\n", tableName))
	sb.WriteString("    id SERIAL PRIMARY KEY,\n")
	for i, field := range messageDescriptor.GetFields() {
		fieldNameSuffix := ""
		if field.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			fieldNameSuffix = "_id"
		}

		fieldName := field.GetName() + fieldNameSuffix
		fieldType := mapFieldType(field)
		//todo: handle relation to an other table
		//mint_id INTEGER NOT NULL,
		//CONSTRAINT fk_mint FOREIGN KEY (mint_id) REFERENCES hivemapper.mints(id)
		//todo: if type is an enum generate a index for that field
		sb.WriteString(fmt.Sprintf("    %q %s", fieldName, fieldType))

		if i < len(messageDescriptor.GetFields())-1 {
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
		fieldNameSuffix := ""
		if field.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			fieldNameSuffix = "_id"
		}
		fieldNames[i] = fmt.Sprintf("\"%s%s\"", field.GetName(), fieldNameSuffix)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING id",
		tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "))

	return insertSQL
}
