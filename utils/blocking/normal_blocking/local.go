package normal_blocking

import (
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"golang.org/x/exp/slices"
	"path"
	"time"
)

func LocalMemoryBlocking(tableName, columnName string, tableLength int64) error {
	columnNames := []string{columnName}
	filePath := blocking_conf.FilePath(tableName, columnNames, 0, blocking_conf.NormalBlockingFileSuffix)
	exist, err := rock_db.DFS.Exist(filePath)
	if err != nil {
		logger.Error(err)
		return err
	}
	if exist {
		logger.Info("blocking ", "表 ", tableName, " 列 ", columnName, " 已存在，跳过")
		return nil
	}

	{
		logger.Info("开始本地 blocking", " 表 ", tableName, " 列 ", columnName)
		go func(time time.Time) {
			logger.Info("blocking ", "表 ", tableName, " 列完成，耗时", utils.DurationString(time))
		}(time.Now())

	}
	if tableLength == 0 {
		lenW, err := rock_db.DB.Query(sql.SingleTable.SELECT(sql.CountAsterisk).FROM(tableName))
		if err != nil {
			logger.Error(err)
			return err
		}
		tableLength = lenW[0][0].(int64)
	}

	var tokenCntMap = map[Token]Frequency{}
	for batchId := int64(0); true; batchId++ {
		start := batchId * blocking_conf.FileRowSize
		end := start + blocking_conf.FileRowSize
		if start >= tableLength {
			break
		}
		if end > tableLength {
			end = tableLength
		}
		logger.Info("表 ", tableName, " 列 ", columnName, " 范围 ", start, " ~ ", end, "的数据计算 token")
		batchTokenCntMap, err := Tokenize(tableName, columnNames, start, end-start) // 内部并行
		if err != nil {
			logger.Error(err)
			return err
		}
		TokenFreqCombine(tokenCntMap, batchTokenCntMap)
		logger.Info("blocking 合并，当前 token 数目 ", len(tokenCntMap))

		if end == tableLength {
			break
		}
	}
	logger.Info("token 计算完成，开始排序和编号")
	for token, frequency := range tokenCntMap {
		if int64(frequency) > tableLength/2 {
			tokenCntMap[token] = 1
		}
	}
	sortedTokens := TokenFreqSort(tokenCntMap)
	logger.Info("token 排序完成，去除了只出现一次的 token，数目", len(sortedTokens))

	for shardId := int64(0); true; shardId++ {
		startRowId := shardId * blocking_conf.FileRowSize
		if startRowId >= tableLength {
			break
		}
		logger.Info("生成tokenId列，当前范围 ", startRowId, "~", startRowId+blocking_conf.FileRowSize)
		_, err = CreateDerivedColumn(tableName, columnNames, startRowId, sortedTokens)
		if err != nil {
			logger.Error(err)
			return err
		}
	}
	return err
}

func LocalMemoryBlockingCrossColumn(leftTableName string, leftColumnNames []string, rightTableName string, rightColumnNames []string) error {
	{
		filePath := blocking_conf.FilePath(leftTableName, leftColumnNames, 0, blocking_conf.NormalBlockingFileSuffix)
		exist, err := rock_db.DFS.Exist(filePath)
		if err != nil {
			logger.Error(err)
			return err
		}
		filePath2 := blocking_conf.FilePath(rightTableName, rightColumnNames, 0, blocking_conf.NormalBlockingFileSuffix)
		exist2, err := rock_db.DFS.Exist(filePath2)
		if err != nil {
			logger.Error(err)
			return err
		}
		if exist && exist2 {
			logger.Info("blocking ", leftTableName, "-", leftColumnNames, " ", rightTableName, "-", rightColumnNames, " 已存在，跳过")
			return nil
		}
		_ = rock_db.DFS.Delete(path.Dir(filePath))
		_ = rock_db.DFS.Delete(path.Dir(filePath2))
	}

	{
		logger.Info("开始 blocking ", leftTableName, "-", leftColumnNames, " ", rightTableName, "-", rightColumnNames)
		go func(time time.Time) {
			logger.Info("开始 blocking ", leftTableName, "-", leftColumnNames, " ", rightTableName, "-", rightColumnNames, " 完成，耗时", utils.DurationString(time))
		}(time.Now())
	}
	var leftTableLength int64 = 0
	var rightTableLength int64 = 0
	{
		lenW, err := rock_db.DB.Query(sql.SingleTable.SELECT(sql.CountAsterisk).FROM(leftTableName))
		if err != nil {
			logger.Error(err)
			return err
		}
		leftTableLength = lenW[0][0].(int64)
	}
	{
		lenW, err := rock_db.DB.Query(sql.SingleTable.SELECT(sql.CountAsterisk).FROM(rightTableName))
		if err != nil {
			logger.Error(err)
			return err
		}
		rightTableLength = lenW[0][0].(int64)
	}

	var tokenCntMap = map[Token]Frequency{}
	for batchId := int64(0); true; batchId++ {
		start := batchId * blocking_conf.FileRowSize
		end := start + blocking_conf.FileRowSize
		if start >= leftTableLength {
			break
		}
		if end > leftTableLength {
			end = leftTableLength
		}
		logger.Info("表 ", leftTableName, " 列 ", leftColumnNames, " 范围 ", start, " ~ ", end, "的数据计算 token")
		batchTokenCntMap, err := Tokenize(leftTableName, leftColumnNames, start, end-start) // 内部并行
		if err != nil {
			logger.Error(err)
			return err
		}
		TokenFreqCombine(tokenCntMap, batchTokenCntMap)
		logger.Info("blocking 合并，当前 token 数目 ", len(tokenCntMap))

		if end == leftTableLength {
			break
		}
	}
	for batchId := int64(0); true; batchId++ {
		start := batchId * blocking_conf.FileRowSize
		end := start + blocking_conf.FileRowSize
		if start >= rightTableLength {
			break
		}
		if end > rightTableLength {
			end = rightTableLength
		}
		logger.Info("表 ", rightTableName, " 列 ", rightColumnNames, " 范围 ", start, " ~ ", end, "的数据计算 token")
		batchTokenCntMap, err := Tokenize(rightTableName, rightColumnNames, start, end-start) // 内部并行
		if err != nil {
			logger.Error(err)
			return err
		}
		TokenFreqCombine(tokenCntMap, batchTokenCntMap)
		logger.Info("blocking 合并，当前 token 数目 ", len(tokenCntMap))

		if end == rightTableLength {
			break
		}
	}
	logger.Info("token 计算完成，开始排序和编号")
	for token, frequency := range tokenCntMap {
		if int64(frequency) > (leftTableLength+rightTableLength)/4 {
			tokenCntMap[token] = 1
		}
	}
	sortedTokens := TokenFreqSort(tokenCntMap)
	logger.Info("token 排序完成，去除了只出现一次的 token，数目", len(sortedTokens))

	for shardId := int64(0); true; shardId++ {
		startRowId := shardId * blocking_conf.FileRowSize
		if startRowId >= leftTableLength {
			break
		}
		logger.Info("生成tokenId列，当前范围 ", startRowId, "~", startRowId+blocking_conf.FileRowSize)
		_, err := CreateDerivedColumn(leftTableName, leftColumnNames, startRowId, sortedTokens)
		if err != nil {
			logger.Error(err)
			return err
		}
	}
	if leftTableName != rightTableName || !slices.Equal(leftColumnNames, rightColumnNames) {
		for shardId := int64(0); true; shardId++ {
			startRowId := shardId * blocking_conf.FileRowSize
			if startRowId >= rightTableLength {
				break
			}
			logger.Info("生成tokenId列，当前范围 ", startRowId, "~", startRowId+blocking_conf.FileRowSize)
			_, err := CreateDerivedColumn(rightTableName, rightColumnNames, startRowId, sortedTokens)
			if err != nil {
				logger.Error(err)
				return err
			}
		}
	}
	return nil
}
