package sql

type Context struct {
	dbBlockID       int
	dbTransactionID int
}

func NewContext() *Context {
	return &Context{}
}

func (c *Context) SetDBBlockID(id int) {
	c.dbBlockID = id
}

func (c *Context) SetDBTransactionID(id int) {
	c.dbTransactionID = id
}

func (c *Context) DBBlockID() int {
	return c.dbBlockID
}

func (c *Context) DBTransactionID() int {
	return c.dbTransactionID
}
