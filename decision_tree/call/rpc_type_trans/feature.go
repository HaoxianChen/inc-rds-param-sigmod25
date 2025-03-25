package rpc_type_trans

import "gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"

func FeatureListToThrift(features []tree.FeatureId) []int32 {
	num := len(features)
	to := make([]int32, num)
	for i := 0; i < num; i++ {
		to[i] = int32(features[i])
	}
	return to
}
