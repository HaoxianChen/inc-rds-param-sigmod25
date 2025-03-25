package normal_blocking

import (
	"bytes"
	"encoding/gob"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql/condition"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/line"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/table_impl/row_id"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"runtime"
	"sort"
	"strconv"
	"sync"
)

/**
1. 每个分片进行分词，将词频发送到主节点
2. 主节点合并词频，按照从小到大排序，丢弃频率为 1 的
3. 排序的词发送到分片，分片将单元格转为词序号集合，写入单独文件 {tab}-{col}-{s}.nb
   s 表示 起始行号/100w、，因此起始行号必须是 100w 的整数倍，例如 0, 100w, 200w, 300w
*/

// Tokenize 对一列分词，计算词频
// limit 为 0 时 startRowId, limit 不生效。全部扫描
func Tokenize(tableName string, columnNames []string, startRowId, limit int64) (map[Token]Frequency, error) {
	SQL := sql.SingleTable.SELECT(columnNames...).FROM(tableName).WHERE(condition.GreaterEq(row_id.ColumnName, startRowId))
	if limit > 0 {
		SQL = SQL.WHERE(condition.Less(row_id.ColumnName, startRowId+limit))
	}
	lines, err := rock_db.DB.Query(SQL)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	result := parallelize(lines, func(low, high int) (batchResult map[Token]Frequency) {
		batchResult = map[Token]Frequency{}
		for i := low; i < high; i++ {
			values := lines[i]
			tokens := tokenizer(values2sentence(values))
			//tokens := tokenizerString(utils.ToString(value))
			for _, token := range tokens {
				batchResult[token] = batchResult[token] + 1
			}
		}
		return batchResult
	}, TokenFreqCombine)

	return result, nil
}

// TokenFreqCombine 词频合并。在主节点调用
func TokenFreqCombine(result map[Token]Frequency, tr map[Token]Frequency) (toResult map[Token]Frequency) {
	for token, frequency := range tr {
		result[token] = result[token] + frequency
	}
	return result
}

// TokenFreqSort 对词频排序
func TokenFreqSort(tokenFreqMap map[Token]Frequency) (sortedTokens []Token) {
	type tokeFreq struct {
		token Token
		freq  Frequency
	}

	var tokenFreqSlice []tokeFreq

	for token, frequency := range tokenFreqMap {
		if frequency <= 1 {
			continue
		}
		tokenFreqSlice = append(tokenFreqSlice, tokeFreq{
			token: token,
			freq:  frequency,
		})
	}

	sort.Slice(tokenFreqSlice, func(i, j int) bool {
		return tokenFreqSlice[i].freq < tokenFreqSlice[j].freq
	})

	for _, tf := range tokenFreqSlice {
		sortedTokens = append(sortedTokens, tf.token)
	}
	return
}

// CreateDerivedColumn 每个节点构建衍生列，是一个单独的文件 {tab}-{col}-{s}.nb
// 文件内容是 [][]TokenId，每个单元格生成 ids []TableId，其中 ids[0] 是频次最高的 token 的 id，用于规则发现粗过滤，ids[1:] 是频次最低的几个 tokenId 用于查错纠错
func CreateDerivedColumn(tableName string, columnNames []string, startRowId int64, sortedTokens []Token) (tokenWords [][]blocking_conf.TokenId, err error) {
	if startRowId%blocking_conf.FileRowSize != 0 {
		panic("startRowId 必须是 FileRowSize 整数倍")
	}
	shardId := startRowId / blocking_conf.FileRowSize

	filePath := blocking_conf.FilePath(tableName, columnNames, shardId, blocking_conf.NormalBlockingFileSuffix) // {taskId}-{index}-{tableName}-{columnName}[-{columnName}].sfx
	//_ = rock_db.DFS.Delete(filePath)

	SQL := sql.SingleTable.SELECT(columnNames...).FROM(tableName)
	SQL = SQL.WHERE(condition.GreaterEq(row_id.ColumnName, startRowId), condition.Less(row_id.ColumnName, startRowId+blocking_conf.FileRowSize))
	lines, err := rock_db.DB.Query(SQL)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	// 索引编号
	var tokenIndexMap = map[Token]blocking_conf.TokenId{}
	for i, token := range sortedTokens {
		tokenIndexMap[token] = int32(i)
	}

	// 输出
	tokenWords = make([][]blocking_conf.TokenId, len(lines))
	parallelizeNoRet(lines, func(low, high int) {
		for i := low; i < high; i++ {
			// token 转为 tokenId
			values := lines[i]
			tokens := tokenizer(values2sentence(values))
			//tokens := tokenizerString(word)
			if len(tokens) == 0 {
				continue // 空值的衍生为为空，即 len(tokenWords[i]) == 0
			}
			var tokenIdSlice = make([]blocking_conf.TokenId, 0, len(tokens))
			for _, token := range tokens {
				if tokenId, ok := tokenIndexMap[token]; ok {
					tokenIdSlice = append(tokenIdSlice, tokenId)
				}
			}
			if len(tokenIdSlice) == 0 { // 确实有可能，因为 tokenIndexMap 中没有出现一次的 token
				continue
			}
			// 小的在前，即频数少的在前
			sort.Slice(tokenIdSlice, func(i, j int) bool {
				return tokenIdSlice[i] < tokenIdSlice[j]
			})

			tokenWords[i] = append(tokenWords[i], tokenIdSlice[len(tokenIdSlice)-1]) // 其中 ids[0] 是频次最高的 token 的 id，用于规则发现粗过滤

			for ti := 0; ti < utils.Min(TokenIdNumber, len(tokenIdSlice)); ti++ {
				tokenWords[i] = append(tokenWords[i], tokenIdSlice[ti])
			}
		}
	})

	// 输出文件
	buf := bytes.Buffer{}
	err = gob.NewEncoder(&buf).Encode(tokenWords)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	err = rock_db.DFS.CreateFile(filePath, int64(buf.Len()))
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	err = rock_db.DFS.Write(filePath, 0, buf.Bytes())
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	logger.Info("blocking 生成文件 ", filePath)

	return tokenWords, nil
}

type Token = string
type Frequency = int

var wordPreSuffix = []rune("##")
var wordPreSuffixString = "##"

const sliceLength = 3
const TokenIdNumber = 10 // 每个单元格值 tokens 化后，取词频最小的 10 个

func tokenizer(word []rune) (tokens []Token) {
	if len(word) == 0 {
		return
	}

	str := append(append(wordPreSuffix, word...), wordPreSuffix...)
	size := len(str) - sliceLength + 1

	tokenCntMap := make(map[Token]int, size)
	tokens = make([]Token, size)
	for i := 0; i < size; i++ {
		token := Token(str[i : i+sliceLength])
		cnt := tokenCntMap[token]
		if cnt > 0 {
			tokens[i] = token + "_" + strconv.Itoa(cnt) // 混入位置信息
		} else {
			tokens[i] = token
		}
		tokenCntMap[token] = tokenCntMap[token] + 1
	}
	return
}

func tokenizerString(word string) (tokens []Token) {
	if len(word) == 0 {
		return
	}

	str := wordPreSuffixString + word + wordPreSuffixString
	size := len(str) - sliceLength + 1

	tokenCntMap := make(map[Token]int, size)
	tokens = make([]Token, size)
	for i := 0; i < size; i++ {
		token := str[i : i+sliceLength]
		cnt := tokenCntMap[token]
		if cnt > 0 {
			tokens[i] = token + "_" + strconv.Itoa(cnt) // 混入位置信息
		} else {
			tokens[i] = token
		}
		tokenCntMap[token] = tokenCntMap[token] + 1
	}
	return
}

func values2sentence(values line.Line) (sentence []rune) {
	var allNil = true
	for i, value := range values {
		sentence = append(sentence, []rune(utils.ToString(value))...)
		if i < len(values)-1 {
			sentence = append(sentence, '#')
		}
		if value != nil {
			allNil = false
		}
	}
	if allNil {
		sentence = nil
	}
	return
}

func parallelize[E, R interface{}](sources []E, f func(low, high int) (batchResult R), combiner func(r1, r2 R) R) (allResult R) {
	totalElementNumber := len(sources)
	parallelism := runtime.NumCPU() + 1
	if totalElementNumber < 1000 {
		return f(0, totalElementNumber)
	}

	var locker sync.Mutex
	var batchResultSlot = make([]R, 0)
	batchSize := totalElementNumber/parallelism + 1
	wg := sync.WaitGroup{}
	for batchId := 0; batchId < parallelism; batchId++ {
		low, high := batchId*batchSize, (batchId+1)*batchSize
		if low >= totalElementNumber {
			continue
		}
		if high > totalElementNumber {
			high = totalElementNumber
		}
		wg.Add(1)
		go func(low, high int, batchId int) {
			r := f(low, high)
			{
				locker.Lock()
				if len(batchResultSlot) == 0 {
					batchResultSlot = append(batchResultSlot, r)
				} else {
					batchResultSlot[0] = combiner(batchResultSlot[0], r)
				}
				locker.Unlock()
			}
			wg.Done()
		}(low, high, batchId)
	}
	wg.Wait()
	return batchResultSlot[0]
}

func parallelizeNoRet[E any](sources []E, f func(low, high int)) {
	totalElementNumber := len(sources)
	parallelism := runtime.NumCPU() + 1
	if totalElementNumber < 1 {
		f(0, totalElementNumber)
	}

	batchSize := totalElementNumber/parallelism + 1
	wg := sync.WaitGroup{}
	for batchId := 0; batchId < parallelism; batchId++ {
		low, high := batchId*batchSize, (batchId+1)*batchSize
		if low >= totalElementNumber {
			continue
		}
		if high > totalElementNumber {
			high = totalElementNumber
		}
		wg.Add(1)
		go func(low, high int, batchId int) {
			f(low, high)
			wg.Done()
		}(low, high, batchId)
	}
	wg.Wait()
	return
}
