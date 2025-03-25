package token1_block_reader

import (
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
)

type reader struct{}

var instance blocking.IReader = reader{}

func Instance() blocking.IReader {
	return instance
}

const tokenIdOne blocking_conf.TokenId = 1

var topKTokenId = []blocking_conf.TokenId{tokenIdOne}

func (r reader) ReadMostFreqTokenId(rowId int32) (tokenId blocking_conf.TokenId, err error) {
	return tokenIdOne, nil
}

func (r reader) ReadTopKTokenIds(rowId int32) (tokenIds []blocking_conf.TokenId, err error) {
	return topKTokenId, nil
}

func (r reader) ReadTopKFreqTokenIds(start, end int) (topKTokenIdsList [][]blocking_conf.TokenId, err error) {
	topKTokenIdsList = make([][]blocking_conf.TokenId, 0, end-start)
	for i := start; i < end; i++ {
		topKTokenIdsList = append(topKTokenIdsList, topKTokenId)
	}
	return topKTokenIdsList, nil
}

func (r reader) ReadMostFreqTokenIds(start, end int) (tokenIds []interface{}, err error) {
	tokenIds = make([]any, 0, end-start)
	for i := start; i < end; i++ {
		tokenIds = append(tokenIds, tokenIdOne)
	}
	return tokenIds, nil
}
