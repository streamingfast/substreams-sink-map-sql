package sql

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"go.uber.org/zap"
)

const static_sql = `
	CREATE SCHEMA IF NOT EXISTS "%s";
	
CREATE TABLE IF NOT EXISTS "%s".cursor (
		name TEXT PRIMARY KEY,
		cursor TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS "%s".block (
		number integer PRIMARY KEY,
		hash TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL
	);
`

type Schema struct {
	Name                  string
	Version               int
	tableCreateStatements []string
	insertSql             map[string]string
	manyToOneRelations    map[string][]string
	moduleOutputType      string
	fileDescriptor        *desc.FileDescriptor
}

func NewSchema(name string, version int, moduleOutputType string, descriptor *desc.FileDescriptor, logger *zap.Logger) (*Schema, error) {
	s := &Schema{
		Name:               name,
		Version:            version,
		moduleOutputType:   moduleOutputType,
		fileDescriptor:     descriptor,
		insertSql:          make(map[string]string),
		manyToOneRelations: make(map[string][]string),
	}

	err := s.init()
	if err != nil {
		return nil, fmt.Errorf("initializing schema: %w", err)
	}
	return s, nil
}

func (s *Schema) init() error {
	foundOutputs := false

	s.insertSql["block"] =
		fmt.Sprintf("INSERT INTO %s (number, hash, timestamp) VALUES ($1, $2, $3) RETURNING number", TableName(s, "block"))

	s.insertSql["cursor"] =
		fmt.Sprintf("INSERT INTO %s (name, cursor) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET cursor = $2", TableName(s, "cursor"))

	for _, messageDescriptor := range s.fileDescriptor.GetMessageTypes() {
		name := messageDescriptor.GetFullyQualifiedName()
		if name == s.moduleOutputType {

			err := s.walkMessageDescriptor(messageDescriptor, func(messageDescriptor *desc.MessageDescriptor) error {
				//extract relation
				for _, f := range messageDescriptor.GetFields() {
					if f.IsRepeated() {
						s.AddManyToOneRelation(f.GetFullyQualifiedName(), messageDescriptor.GetName())
					}
				}
				return nil
			})

			if err != nil {
				return fmt.Errorf("extracting table relations  %q: %w", messageDescriptor.GetName(), err)
			}

			err = s.walkMessageDescriptor(messageDescriptor, func(messageDescriptor *desc.MessageDescriptor) error {
				err := s.createTableFromMessageDescriptor(messageDescriptor)
				if err != nil {
					return fmt.Errorf("walking and creating create statement: %q: %w", messageDescriptor.GetName(), err)
				}
				return nil
			})

			if err != nil {
				return err
			}

			err = s.walkMessageDescriptor(messageDescriptor, func(messageDescriptor *desc.MessageDescriptor) error {
				err := s.createInsertFromDescriptor(messageDescriptor)
				if err != nil {
					return fmt.Errorf("walking and creating insert statement: %q: %w", messageDescriptor.GetName(), err)
				}
				return nil
			})
			if err != nil {
				return err
			}

			foundOutputs = true
		}
	}
	if !foundOutputs {
		return fmt.Errorf("no outputs message found")
	}

	return nil
}

func (s *Schema) walkMessageDescriptor(messageDescriptor *desc.MessageDescriptor, doWork func(messageDescriptor *desc.MessageDescriptor) error) error {
	for _, field := range messageDescriptor.GetFields() {
		if field.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			msgDesc := field.GetMessageType()
			err := s.walkMessageDescriptor(msgDesc, doWork)
			if err != nil {
				return fmt.Errorf("walking field %q message descriptor: %w", field.GetName(), err)
			}
		}
	}

	err := doWork(messageDescriptor)
	if err != nil {
		return fmt.Errorf("doing work on message descriptor %q: %w", messageDescriptor.GetName(), err)
	}

	return nil
}

func (s *Schema) createInsertFromDescriptor(d *desc.MessageDescriptor) error {
	tableName := s.String() + "." + strings.ToLower(d.GetName())
	fields := d.GetFields()
	var fieldNames []string
	var placeholders []string

	fieldNames = append(fieldNames, "block_number")
	placeholders = append(placeholders, "$1")

	fieldCount := 1
	for _, field := range fields {
		if field.IsRepeated() { //not a direct child
			continue
		}
		fieldCount++
		fieldNames = append(fieldNames, fieldQuotedName(field))
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	}

	ones := s.manyToOneRelations[d.GetFullyQualifiedName()]
	for _, one := range ones {
		fieldCount++
		field := fmt.Sprintf("%s_id", one)
		fieldNames = append(fieldNames, field)
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING id",
		tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "))

	s.insertSql[d.GetFullyQualifiedName()] = insertSQL

	return nil
}

func (s *Schema) createTableFromMessageDescriptor(messageDescriptor *desc.MessageDescriptor) error {
	var sb strings.Builder

	table := tableNameFromDescriptor(s, messageDescriptor)
	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (\n", table))
	sb.WriteString("    id SERIAL PRIMARY KEY,\n")
	sb.WriteString("    block_number INTEGER NOT NULL,\n")

	var foreignKeys []*foreignKey
	for _, f := range messageDescriptor.GetFields() {
		field := fieldQuotedName(f)
		fieldType := mapFieldType(f)

		sb.WriteString(fmt.Sprintf("    %s %s", field, fieldType))

		switch {
		case f.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE:
			foreignKeys = append(foreignKeys, &foreignKey{
				name:         "fk_" + fieldName(f),
				table:        table,
				field:        field,
				foreignTable: tableNameFromDescriptor(s, f.GetMessageType()),
				foreignField: "id",
			})
		case f.IsRepeated():
			continue
		}

		sb.WriteString(",\n")
	}

	ones := s.manyToOneRelations[messageDescriptor.GetFullyQualifiedName()]
	for _, one := range ones {
		field := fmt.Sprintf("%s_id", one)
		fieldType := "INTEGER"
		sb.WriteString(fmt.Sprintf("    %s %s", field, fieldType))
		foreignKeys = append(foreignKeys, &foreignKey{
			name:         "fk_" + field,
			table:        table,
			field:        field,
			foreignTable: TableName(s, one),
			foreignField: "id",
		})
	}

	sb.WriteString(fmt.Sprintf("    CONSTRAINT fk_block FOREIGN KEY (block_number) REFERENCES %s.block(number)", s.String()))
	if len(foreignKeys) > 0 {
		sb.WriteString(",\n")
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

	s.tableCreateStatements = append(s.tableCreateStatements, sb.String())
	return nil

}

func (s *Schema) AddManyToOneRelation(many string, one string) {
	ones, found := s.manyToOneRelations[many]
	if !found {
		s.manyToOneRelations[many] = []string{one}
	}
	ones = append(ones, one)
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
