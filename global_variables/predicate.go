package global_variables

type Column struct {
	ColumnId   string
	ColumnType string
}

type Predicate struct {
	PredicateStr  string
	LeftColumn    Column
	RightColumn   Column
	ConstantValue interface{}
	SymbolType    string
	PredicateType int // 常数谓词是0,非常数谓词是1
	Support       float64
}
