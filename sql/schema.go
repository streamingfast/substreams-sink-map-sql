package sql

import (
	"fmt"
	"hash/fnv"
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

type Schema struct {
	Name                  string
	Version               int
	tableCreateStatements map[string]string
	constraintStatements  []*Constraint
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

			err := s.walkMessageDescriptor(messageDescriptor, func(md *desc.MessageDescriptor) error {
				for _, f := range md.GetFields() {
					if f.IsRepeated() && proto.IsTable(md) {
						s.AddManyToOneRelation(f.GetMessageType().GetFullyQualifiedName(), md.GetName())
					}
				}
				return nil
			})

			if err != nil {
				return fmt.Errorf("extracting table relations  %q: %w", messageDescriptor.GetName(), err)
			}

			err = s.walkMessageDescriptor(messageDescriptor, func(md *desc.MessageDescriptor) error {
				err := s.createTableFromMessageDescriptor(md)
				if err != nil {
					return fmt.Errorf("walking and creating create statement: %q: %w", md.GetName(), err)
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
			break
		}
	}
	if !foundOutputs {
		return fmt.Errorf("no outputs message found")
	}

	return nil
}

func (s *Schema) walkMessageDescriptor(md *desc.MessageDescriptor, task func(md *desc.MessageDescriptor) error) error {
	for _, field := range md.GetFields() {
		if field.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			err := s.walkMessageDescriptor(field.GetMessageType(), task)
			if err != nil {
				return fmt.Errorf("walking field %q message descriptor: %w", field.GetName(), err)
			}
		}
	}

	err := task(md)
	if err != nil {
		return fmt.Errorf("running task on message descriptor %q: %w", md.GetName(), err)
	}

	return nil
}

func (s *Schema) createTableFromMessageDescriptor(md *desc.MessageDescriptor) error {
	if !proto.IsTable(md) {
		return nil
	}

	if _, found := s.tableCreateStatements[md.GetFullyQualifiedName()]; found {
		return nil
	}

	var sb strings.Builder

	tableName := tableNameFromDescriptor(s, md)

	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (\n", tableName))
	sb.WriteString("    id SERIAL PRIMARY KEY,\n")
	sb.WriteString("    block_number INTEGER NOT NULL,\n")

	for _, f := range md.GetFields() {
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
				table:        tableName,
				field:        field,
				foreignTable: tableNameFromDescriptor(s, f.GetMessageType()),
				foreignField: "id",
			}
			c := &Constraint{
				table: tableName,
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

	ones := s.manyToOneRelations[md.GetFullyQualifiedName()]
	for i, one := range ones {
		sb.WriteString(",\n")
		field := fmt.Sprintf("%s_id", strings.ToLower(one))
		sb.WriteString(fmt.Sprintf("    %s %s", field, "INTEGER"))

		if i < len(ones)-1 {
			sb.WriteString(",\n")
		}

		foreignKey := &foreignKey{
			name:         "fk_" + field,
			table:        tableName,
			field:        field,
			foreignTable: TableName(s, one),
			foreignField: "id",
		}
		c := &Constraint{
			table: tableName,
			sql:   foreignKey.String(),
		}

		s.constraintStatements = append(s.constraintStatements, c)
	}
	sb.WriteString("\n);\n")

	c := &Constraint{
		table: tableName,
		sql:   fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT fk_block FOREIGN KEY (block_number) REFERENCES %s.block(number)", tableName, s.String()),
	}

	s.constraintStatements = append(s.constraintStatements, c)
	s.tableCreateStatements[md.GetFullyQualifiedName()] = sb.String()

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

func (s *Schema) AddManyToOneRelation(many string, one string) {
	ones, found := s.manyToOneRelations[many]
	if !found {
		s.manyToOneRelations[many] = []string{one}
	}
	ones = append(ones, one)
}

func (s *Schema) Hash() uint64 {
	h := fnv.New64a()

	var buf []byte
	// Hash Name
	buf = append(buf, []byte(s.String())...)
	buf = append(buf, []byte(s.moduleOutputType)...)

	// Hash tableCreateStatements
	for _, sql := range s.tableCreateStatements {
		buf = append(buf, []byte(sql)...)
	}

	for _, constraint := range s.constraintStatements {
		buf = append(buf, []byte(constraint.sql)...)
	}

	for _, sql := range s.insertSql {
		buf = append(buf, []byte(sql)...)
	}

	_, err := h.Write(buf)
	if err != nil {
		panic("unable to write to hash")
	}

	return h.Sum64()
}

func (s *Schema) String() string {
	return fmt.Sprintf("%s_%d", s.Name, s.Version)
}
