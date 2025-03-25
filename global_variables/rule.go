package global_variables

type Rule struct {
	Ree           string
	LhsPredicates []Predicate
	LhsColumns    []Column
	Rhs           Predicate
	RhsColumn     Column
	CR            float64
	FTR           float64
}
