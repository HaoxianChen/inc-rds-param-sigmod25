package rpc_type_trans

import (
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/gen-go/rpc"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"
	"unsafe"
)

func AVCtoThrift(avc *tree.AVC) *rpc.AVC {
	return &rpc.AVC{
		int8(avc.AttrType()),
		int32(avc.Attr()),
		avc.NaNCount(),
		avc.AttrVs(),
		avc.LabelCount(),
	}
}

func AVCfromThrift(avc *rpc.AVC) *tree.AVC {
	return tree.ReConstructAVC(tree.AttributeType(avc.AttrT), tree.FeatureId(avc.Attr), avc.NanCount, avc.AttrVs, avc.LabelCount)
}

func GetAVCSize(avc *rpc.AVC) float64 {
	// 只是返回一个近似的大小
	structSize := uint64(unsafe.Sizeof(*avc))
	sliceSize := uint64(len(avc.AttrVs) * 8)
	mapSize := uint64(0)
	for _, m := range avc.LabelCount {
		mapSize += uint64(unsafe.Sizeof(m)) + uint64(len(m)*16)
	}
	totalSize := structSize + sliceSize + mapSize
	return float64(totalSize) / (1024 * 1024)
}
