package sql

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-map-sql/proto"
)

type PrimaryKey struct {
	Name     string
	DataType DataType
}

type ChildOf struct {
	ParentTable      string
	ParentTableField string
}

func NewChildOf(childOf string) (*ChildOf, error) {
	parts := strings.Split(childOf, " on ")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid child of format %q. expecting 'table_name on field_name' format", childOf)
	}

	return &ChildOf{
		ParentTable:      strings.TrimSpace(parts[0]),
		ParentTableField: strings.TrimSpace(parts[1]),
	}, nil
}

type ForeignKey struct {
	Table      string
	TableField string
}

func NewForeignKey(foreignKey string) (*ForeignKey, error) {
	parts := strings.Split(foreignKey, " on ")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid foreign key format %q. expecting 'table_name on field_name' format", foreignKey)
	}
	return &ForeignKey{
		Table:      strings.TrimSpace(parts[0]),
		TableField: strings.TrimSpace(parts[1]),
	}, nil
}

type Table struct {
	Name       string
	PrimaryKey *PrimaryKey
	ChildOf    *ChildOf
	Fields     []*Field
}

func NewTable(descriptor *desc.MessageDescriptor) (*Table, error) {
	tableInfo := proto.TableInfo(descriptor)
	if tableInfo == nil {
		return nil, nil
	}

	table := &Table{
		Name: descriptor.GetName(),
	}
	table.Name = tableInfo.Name

	if tableInfo.ChildOf != nil {
		co, err := NewChildOf(*tableInfo.ChildOf)
		if err != nil {
			return nil, fmt.Errorf("error parsing child of: %w", err)
		}
		table.ChildOf = co
	}

	err := table.processFields(descriptor)
	if err != nil {
		return nil, fmt.Errorf("error processing fields for table %q: %w", descriptor.GetName(), err)
	}

	return table, nil
}

func (t *Table) processFields(descriptor *desc.MessageDescriptor) error {
	for _, fieldDescriptor := range descriptor.GetFields() {
		field, err := NewField(fieldDescriptor)
		if err != nil {
			return fmt.Errorf("error processing field %q: %w", fieldDescriptor.GetName(), err)
		}
		if field.IsPrimaryKey {
			if t.PrimaryKey != nil {
				return fmt.Errorf("multiple primary keys are not supported in message")
			}

			t.PrimaryKey = &PrimaryKey{
				Name:     field.Name,
				DataType: field.DataType,
			}
		}
		t.Fields = append(t.Fields, field)
	}

	return nil
}

func (t *Table) FullName(schema *Schema) string {
	return TableName(schema, t.Name)
}

func TableName(schema *Schema, tableName string) string {
	return fmt.Sprintf("%s.%s", schema.Name, tableName)
}

type Field struct {
	Name         string
	ForeignKey   *ForeignKey
	DataType     DataType
	IsPrimaryKey bool
	IsUnique     bool
	IsRepeated   bool
	IsExtension  bool
	//todo: naming ...
	IsMessage bool
	Message   string
}

func NewField(d *desc.FieldDescriptor) (*Field, error) {

	out := &Field{
		Name:        d.GetName(),
		DataType:    mapFieldType(d),
		IsRepeated:  d.IsRepeated(),
		IsMessage:   d.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE,
		IsExtension: d.IsExtension(),
	}

	fieldInfo := proto.FieldInfo(d)
	if fieldInfo != nil {
		if fieldInfo.Name != nil {
			out.Name = *fieldInfo.Name
		}
		if fieldInfo.ForeignKey != nil {
			fk, err := NewForeignKey(*fieldInfo.ForeignKey)
			if err != nil {
				return nil, fmt.Errorf("error parsing foreign key %s: %w", *fieldInfo.ForeignKey, err)
			}
			out.ForeignKey = fk
		}
		out.IsPrimaryKey = fieldInfo.PrimaryKey
		out.IsUnique = fieldInfo.Unique
	}

	if out.IsMessage {
		out.Message = d.GetMessageType().GetName()
	}

	return out, nil
}
