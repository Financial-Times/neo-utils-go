package neoutils

func UnderlyingDB(con NeoConnection) Database {
	//App is using neoism connection directly. Please update when possible to avoid this.

	switch c := con.(type) {
	case *DefaultNeoConnection:
		return c.db
	default:
		panic("unhandled NeoConnection type")
	}

}
