package proto

import (
	"errors"

	proto "github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"google.golang.org/protobuf/types/descriptorpb"
)

func IsTable(d *desc.MessageDescriptor) bool {
	msgOptions := d.GetOptions().(*descriptorpb.MessageOptions)

	var E_IsTable = &proto.ExtensionDesc{
		ExtendedType:  (*descriptorpb.MessageOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         77701,
		Name:          "is_table",
		Tag:           "varint,77701,opt,name=is_table",
	}

	ext, err := proto.GetExtension(msgOptions, E_IsTable)

	if errors.Is(err, proto.ErrMissingExtension) {
		return false
	} else if err != nil {
		return false
	} else {
		isTable, ok := ext.(*bool)
		if ok && *isTable {
			return true
		} else {
			return false
		}
	}
}
