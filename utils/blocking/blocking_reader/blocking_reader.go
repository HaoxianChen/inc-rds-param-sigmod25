package blocking_reader

import (
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"math"
	"sync"
)

type Reader struct {
	tableName   string
	columnNames []string
	fileSuffix  string
	dataMap     sync.Map // map[string][][]int32
	threshold   float64
}

func New(tableName string, columnNames []string, fileSuffix string, threshold float64) *Reader {
	return &Reader{
		tableName:   tableName,
		columnNames: columnNames,
		fileSuffix:  fileSuffix,
		dataMap:     sync.Map{},
		threshold:   threshold,
	}
}

func (n *Reader) ReadMostFreqTokenId(rowId int32) (tokenId blocking_conf.TokenId, err error) {
	tokensList, err := n.getTokensList(rowId)
	if err != nil {
		return blocking_conf.InvalidTokenId, err
	}
	tokenIds := tokensList[rowId%blocking_conf.FileRowSize]
	if len(tokenIds) == 0 {
		return blocking_conf.InvalidTokenId, nil
	} else {
		return tokenIds[0], nil
	}
}

func (n *Reader) ReadMostFreqTokenIds(start, end int) (tokenIds []interface{}, err error) {
	tokenIds = make([]interface{}, 0, end-start)
	var tokensList [][]blocking_conf.TokenId
	for start < end {
		tokensList, err = n.getTokensList(int32(start))
		if err != nil {
			return
		}
		for _, ts := range tokensList {
			if len(ts) == 0 {
				tokenIds = append(tokenIds, int64(blocking_conf.InvalidTokenId))
			} else {
				tokenIds = append(tokenIds, int64(ts[0]))
			}
			if cap(tokenIds) == len(tokenIds) {
				return
			}
		}
		start += blocking_conf.FileRowSize
	}
	return
}

func (n *Reader) ReadTopKFreqTokenIds(start, end int) (topKTokenIdsList [][]blocking_conf.TokenId, err error) {
	topKTokenIdsList = make([][]blocking_conf.TokenId, 0, end-start)
	var tokensList [][]blocking_conf.TokenId
	for start < end {
		tokensList, err = n.getTokensList(int32(start))
		if err != nil {
			return
		}
		for _, ts := range tokensList {
			if len(ts) > 1 {
				ids := ts[1:]
				limit := int(math.Ceil(float64(len(ids)) * (1 - n.threshold)))
				if limit == 0 {
					limit = 1
				}
				if len(ids) > limit {
					ids = ids[:limit]
				}
				topKTokenIdsList = append(topKTokenIdsList, ids)
			} else {
				topKTokenIdsList = append(topKTokenIdsList, nil)
			}
			if cap(topKTokenIdsList) == len(topKTokenIdsList) {
				return
			}
		}
		start += blocking_conf.FileRowSize
	}
	return
}

func (n *Reader) ReadTopKTokenIds(rowId int32) (tokenIds []blocking_conf.TokenId, err error) {
	tokensList, err := n.getTokensList(rowId)
	if err != nil {
		return nil, err
	}
	tokenIds = tokensList[rowId%blocking_conf.FileRowSize]
	if len(tokenIds) < 2 {
		return nil, nil
	} else {
		ids := tokenIds[1:]
		limit := int(math.Ceil(float64(len(ids)) * (1 - n.threshold)))
		if limit == 0 && len(ids) > 0 {
			limit = 1
		}
		if len(ids) > limit {
			ids = ids[:limit]
		}
		return ids, nil
	}
}

var dataMapErrValue any = 1

func (n *Reader) getTokensList(rowId int32) (tokensList [][]blocking_conf.TokenId, err error) {
	shardId := int64(rowId) / blocking_conf.FileRowSize
	path := blocking_conf.FilePath(n.tableName, n.columnNames, shardId, n.fileSuffix)
	tokensListI, ok := n.dataMap.Load(path)
	if !ok {
		tokensList, err = blocking_conf.ReadDerivedColumn(path)
		if err != nil {
			n.dataMap.Store(path, dataMapErrValue)
			return nil, err
		}
		n.dataMap.Store(path, tokensList)
		tokensListI = tokensList
	}
	if tokensListI == dataMapErrValue {
		return nil, err
	} else {
		return tokensListI.([][]blocking_conf.TokenId), err
	}
}
