package schema

func (t *Table) ManyToOnName() string {
	n := t.ManyToOneRelationFieldName
	if n != "" {
		return n
	}
	return t.Name + "_id"
}
