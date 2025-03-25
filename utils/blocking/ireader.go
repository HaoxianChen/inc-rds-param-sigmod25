package blocking

import "gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"

/**
blocking 结果读取器
*/

type IReader interface {
	ReadMostFreqTokenId(rowId int32) (tokenId blocking_conf.TokenId, err error)
	ReadTopKTokenIds(rowId int32) (tokenIds []blocking_conf.TokenId, err error)
	ReadTopKFreqTokenIds(start, end int) (topKTokenIdsList [][]blocking_conf.TokenId, err error)
	ReadMostFreqTokenIds(start, end int) (tokenIds []interface{}, err error)
}
