package sql

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
)

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
