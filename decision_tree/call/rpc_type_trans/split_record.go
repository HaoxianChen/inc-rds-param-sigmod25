package rpc_type_trans

import (
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/gen-go/rpc"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"
)

func SplitRecordToThrift(attr int32, split *tree.InnerSplitInfo) *rpc.SplitRecord {
	return &rpc.SplitRecord{
		Feature:          attr,
		SplitValue:       (*split).SplitValue,
		LeftWeight:       (*split).LeftWeight,
		RightWeight:      (*split).RightWeight,
		LeftImpurity:     (*split).LeftImpurity,
		RightImpurity:    (*split).RightImpurity,
		ImprovementProxy: (*split).ImprovementProxy,
	}
}

func SplitRecordFromThrift(split *rpc.SplitRecord) *tree.SplitRecord {
	return &tree.SplitRecord{
		Feature:       tree.FeatureId((*split).Feature),
		SplitValue:    (*split).SplitValue,
		LeftWeight:    (*split).LeftWeight,
		RightWeight:   (*split).RightWeight,
		LeftImpurity:  (*split).LeftImpurity,
		RightImpurity: (*split).RightImpurity,
		Improvement:   (*split).ImprovementProxy,
	}
}
