package sql

import (
	"fmt"
	"strings"
	"substreams-sink-map-sql/proto"

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

type constraint struct {
	table string
	sql   string
}

type Schema struct {
	Name                  string
	Version               int
	tableCreateStatements map[string]string
	constraintStatements  []*constraint
	insertSql             map[string]string
	manyToOneRelations    map[string][]string
	moduleOutputType      string
	fileDescriptor        *desc.FileDescriptor
}

func NewSchema(name string, version int, moduleOutputType string, descriptor *desc.FileDescriptor, logger *zap.Logger) (*Schema, error) {
	s := &Schema{
		Name:                  name,
		Version:               version,
		moduleOutputType:      moduleOutputType,
		fileDescriptor:        descriptor,
		insertSql:             make(map[string]string),
		manyToOneRelations:    make(map[string][]string),
		tableCreateStatements: make(map[string]string),
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
					if f.IsRepeated() && proto.IsTable(messageDescriptor) {
						s.AddManyToOneRelation(f.GetMessageType().GetFullyQualifiedName(), messageDescriptor.GetName())
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
	if !proto.IsTable(d) {
		return nil
	}

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
	if !proto.IsTable(messageDescriptor) {
		return nil
	}

	if _, found := s.tableCreateStatements[messageDescriptor.GetFullyQualifiedName()]; found {
		return nil
	}

	var sb strings.Builder

	table := tableNameFromDescriptor(s, messageDescriptor)
	fmt.Println("creating create sql for:", table)

	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (\n", table))
	sb.WriteString("    id SERIAL PRIMARY KEY,\n")
	sb.WriteString("    block_number INTEGER NOT NULL,\n")

	for _, f := range messageDescriptor.GetFields() {
		field := fieldQuotedName(f)
		fieldType := mapFieldType(f)

		switch {
		case f.IsRepeated():
			continue
		case f.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE:
			if !proto.IsTable(f.GetMessageType()) {
				continue
			}
			foreignKey := &foreignKey{
				name:         "fk_" + fieldName(f),
				table:        table,
				field:        field,
				foreignTable: tableNameFromDescriptor(s, f.GetMessageType()),
				foreignField: "id",
			}
			c := &constraint{
				table: table,
				sql:   foreignKey.String(),
			}
			s.constraintStatements = append(s.constraintStatements, c)
		}
		sb.WriteString(fmt.Sprintf("    %s %s", field, fieldType))
		sb.WriteString(",\n")
	}

	temp := sb.String()
	temp = temp[:len(temp)-2]
	sb = strings.Builder{}
	sb.WriteString(temp)

	ones := s.manyToOneRelations[messageDescriptor.GetFullyQualifiedName()]
	for i, one := range ones {
		sb.WriteString(",\n")
		field := fmt.Sprintf("%s_id", strings.ToLower(one))
		fieldType := "INTEGER"
		sb.WriteString(fmt.Sprintf("    %s %s", field, fieldType))

		if i < len(ones)-1 {
			sb.WriteString(",\n")
		}

		foreignKey := &foreignKey{
			name:         "fk_" + field,
			table:        table,
			field:        field,
			foreignTable: TableName(s, one),
			foreignField: "id",
		}
		c := &constraint{
			table: table,
			sql:   foreignKey.String(),
		}

		s.constraintStatements = append(s.constraintStatements, c)
	}
	sb.WriteString("\n);\n")

	c := &constraint{
		table: table,
		sql:   fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT fk_block FOREIGN KEY (block_number) REFERENCES %s.block(number)", table, s.String()),
	}

	s.constraintStatements = append(s.constraintStatements, c)

	s.tableCreateStatements[messageDescriptor.GetFullyQualifiedName()] = sb.String()
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
	panic("fix me")
	//h := sha256.New()
	//
	//// Add the Name to the hash
	//h.Write([]byte(s.Name))
	//
	//// Add the Version to the hash
	//h.Write([]byte(fmt.Sprintf("%d", s.Version)))
	//
	//// Add the tableCreateStatements to the hash in a sorted order for consistent hashing
	//sortedStatements := make([]string, len(s.tableCreateStatements))
	//copy(sortedStatements, s.tableCreateStatements)
	//sort.Strings(sortedStatements)
	//for _, stmt := range sortedStatements {
	//	h.Write([]byte(stmt))
	//}
	//
	//return hex.EncodeToString(h.Sum(nil))
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
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s  FOREIGN KEY (%s) REFERENCES %s(%s)", f.table, f.name, f.field, f.foreignTable, f.foreignField)
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
