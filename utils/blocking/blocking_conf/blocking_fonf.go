package blocking_conf

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/enum"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"strings"
)

type TokenId = int32

const InvalidTokenId TokenId = -1

const FileRowSize = 100 * 100 * 100

const NormalBlockingFileSuffix = "nb"
const LocalDistanceBlockingFileSuffix = "ldb"
const LSHBlockingFileSuffix = "lsh"

//func FilePath(tableName, columnName string, shardId int64, similarTypeAbbr string) string {
//	return fmt.Sprintf("blocking/%s/%s/%d.%s", tableName, columnName, shardId, similarTypeAbbr)
//}

func FilePath(tableName string, columnNames []string, shardId int64, similarTypeAbbr string) string {
	// {tableName}-{columnNames.join-}/{shardId}.{type}
	return fmt.Sprintf("blocking/%s-%s/%d.%s", tableName, strings.Join(columnNames, "-"), shardId, similarTypeAbbr)
}

func ModelName2Suffix(modelName string) string {
	modelName = strings.ToLower(modelName)
	if modelName == "jaccard" {
		return NormalBlockingFileSuffix
	} else if modelName == "lhs" {
		return LSHBlockingFileSuffix
	} else {
		panic("未知相似度ML名称，只支持 jaccard 和 lhs")
	}
}

func ModelName2Type(modelName string) string {
	modelName = strings.ToLower(modelName)
	if modelName == "jaccard" {
		return enum.Similar
	} else if modelName == "lhs" {
		return enum.ML
	} else {
		panic("未知相似度ML名称，只支持 similar 和 ML")
	}
}

func ReadDerivedColumn(filePath string) ([][]TokenId, error) {
	stat, err := rock_db.DFS.Stat(filePath)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	data, err := rock_db.DFS.Read(filePath, 0, stat.Size())
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	var tokensList [][]TokenId
	err = gob.NewDecoder(bytes.NewReader(data)).Decode(&tokensList)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return tokensList, nil
}

func WriteDerivedColumn(filePath string, tokenIdsList [][]TokenId) (err error) {
	_ = rock_db.DFS.Delete(filePath)

	buffer := bytes.Buffer{}
	err = gob.NewEncoder(&buffer).Encode(tokenIdsList)
	if err != nil {
		logger.Error(err)
		return err
	}
	err = rock_db.DFS.CreateFile(filePath, int64(buffer.Len()))
	if err != nil {
		logger.Error(err)
		return err
	}
	err = rock_db.DFS.Write(filePath, 0, buffer.Bytes())
	if err != nil {
		logger.Error(err)
		return err
	}
	return nil
}
