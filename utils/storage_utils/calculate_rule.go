package storage_utils

import (
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
)

func ForeignKeyPredicate(p *rds.Predicate) bool {
	return p.PredicateType == foreignKeyPredicateType
}

func ForeignKeyPredicatesUnique(foreignKeyPredicates []rds.Predicate) ([]rds.Predicate, bool) {
	sz := len(foreignKeyPredicates)
	if sz == 0 {
		return nil, false
	}
	if sz%2 != 0 {
		return foreignKeyPredicates, false
	}

	var r []rds.Predicate
	for i := 0; i < sz-1; i++ {
		cur := foreignKeyPredicates[i]
		nex := foreignKeyPredicates[i+1]
		if sameLeftRight(cur, nex) {
			r = append(r, cur)
			i += 2
		} else {
			return foreignKeyPredicates, false
		}
	}
	return r, true
}

type rowId = int32
