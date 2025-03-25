package task_tree

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"strings"
)

type TaskTree struct {
	Rhs                        rds.Predicate   //根节点
	GiniIndex                  float64         //该节点的gini系数
	Lhs                        []rds.Predicate //0层为空、1层1个、2层2个
	LhsCandidate               []rds.Predicate //候选lhs不一定能用上
	LhsCrossCandidate          []rds.Predicate //跨表候选lhs
	LhsCandidateGiniIndex      []float64       //普通谓词候选集的gini系数
	LhsCrossCandidateGiniIndex []float64       //跨表谓词候选集的gini系数
	TableId2index              map[string]int  //表id和其对应的索引，比如tableA的index是0，那他对应的索引就是t0和t1
}

func (tree TaskTree) Tostring() string {
	lhsArr := make([]string, len(tree.Lhs))
	for i, p := range tree.Lhs {
		lhsArr[i] = p.PredicateStr
	}
	str := fmt.Sprintf("rhs:%s, lhs:%s, gini:%f", tree.Rhs.PredicateStr, strings.Join(lhsArr, "^"), tree.GiniIndex)
	return str
}
