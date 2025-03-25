package local_jaccard_similar

import (
	"gitlab.grandhoo.com/rock/rock-share/global/utils/similarity/similarity_impl/jaccard_similarity"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"math"
	"sort"
	"strconv"
)

type Token string
type TokenId int32
type Count int32

type IndexCnt struct {
	index int32
	cnt   Count
}

func Calculate(left, right []any, threshold float64) (pairs [][2]int32) {
	sLeft := make([]string, len(left))
	sRight := make([]string, len(right))

	for i, s := range left {
		sLeft[i] = utils.ToString(s)
	}
	for i, s := range right {
		sRight[i] = utils.ToString(s)
	}
	left = nil
	right = nil
	return calculate0(sLeft, sRight, threshold)
}

func calculate0(left, right []string, threshold float64) (pairs [][2]int32) {
	var reverse = false
	if len(left) > len(right) {
		left, right = right, left
		reverse = true
	}

	// token
	var tokenIdProvider = make(map[Token]TokenId, len(right)/10+1024)
	var curTokenId TokenId = 1
	var tokenCntMap = make(map[TokenId]Count, len(right)/10+1024)

	var leftTokens = make([][]TokenId, len(left))
	var rightTokens = make([][]TokenId, len(right))

	for i, s := range left {
		tokens := tokenizer(s)
		var tokenIds = make([]TokenId, len(tokens))
		for k, token := range tokens {
			tokenId := tokenIdProvider[token]
			if tokenId == 0 {
				tokenId = curTokenId
				tokenIdProvider[token] = tokenId
				curTokenId++
			}
			tokenCntMap[tokenId] = tokenCntMap[tokenId] + 1
			tokenIds[k] = tokenId
		}
		leftTokens[i] = tokenIds
	}

	for i, s := range right {
		tokens := tokenizer(s)
		var tokenIds = make([]TokenId, len(tokens))
		for k, token := range tokens {
			tokenId := tokenIdProvider[token]
			tokenCntMap[tokenId] = tokenCntMap[tokenId] + 1
			tokenIds[k] = tokenId
		}
		rightTokens[i] = tokenIds
	}

	// create right map[token][]*IndexCnt. pointer here for sharing
	var token2rightWords = make(map[TokenId][]*IndexCnt, len(right)/10+1024)
	for index, tokenIds := range rightTokens {
		var indexCnt = IndexCnt{
			index: int32(index),
			cnt:   0,
		}
		for _, tokenId := range tokenIds {
			token2rightWords[tokenId] = append(token2rightWords[tokenId], &indexCnt)
		}
	}

	// scan left and output
	var rightIndexSet = map[int32]struct{}{}
	for leftIndex, leftTokenIds := range leftTokens {
		// sort leftTokenIds by cnt. 升序
		sort.Slice(leftTokenIds, func(i, j int) bool {
			return tokenCntMap[leftTokenIds[i]] < tokenCntMap[leftTokenIds[j]]
		})
		// leftLimit
		var leftLimit = int(math.Ceil(float64(len(leftTokenIds)) * (1 - threshold)))
		if leftLimit > len(leftTokenIds) {
			leftLimit = len(leftTokenIds)
		}
		// counting
		for i := 0; i < leftLimit; i++ {
			leftTokenId := leftTokenIds[i]
			rightWords := token2rightWords[leftTokenId]
			for rightIndex := range rightWords {
				rightWords[rightIndex].cnt++ // 交集 S∩T
			}
		}
		// re-iter for output and zero
		for i := 0; i < leftLimit; i++ {
			leftTokenId := leftTokenIds[i]
			rightWords := token2rightWords[leftTokenId]
			for _, rightIndexCnt := range rightWords {
				if rightIndexCnt.cnt > 0 {
					rightIndexSet[rightIndexCnt.index] = struct{}{}
				}
				//rightIndex := rightIndexCnt.index                        // index to right, rightTokens and output
				//sum := float64(leftLimit + len(rightTokens[rightIndex])) // |S| + |T|
				//if intercept >= threshold*(sum)/(1+threshold) {          // jaccard
				//	if reverse {
				//		pairs = append(pairs, [2]int32{rightIndex, int32(leftIndex)})
				//	} else {
				//		pairs = append(pairs, [2]int32{int32(leftIndex), rightIndex})
				//	}
				//}

				// anyway, zero it
				rightIndexCnt.cnt = 0
			}
		}

		// 验证
		if len(rightIndexSet) > 0 {
			leftString := left[leftIndex]
			for rightIndex := range rightIndexSet {
				rightString := right[rightIndex]
				if jaccard_similarity.INSTANCE.Compare(leftString, rightString) >= threshold {
					if reverse {
						pairs = append(pairs, [2]int32{rightIndex, int32(leftIndex)})
					} else {
						pairs = append(pairs, [2]int32{int32(leftIndex), rightIndex})
					}
				}
			}
			// clear
			rightIndexSet = map[int32]struct{}{}
		}
	}
	return pairs
}

const sliceLength = 3

var wordPreSuffixString = "##"

func tokenizer(word string) (tokens []Token) {
	if len(word) == 0 {
		return
	}

	//str := wordPreSuffixString + word + wordPreSuffixString
	str := []rune(wordPreSuffixString + word + wordPreSuffixString)
	size := len(str) - sliceLength + 1

	tokenCntMap := make(map[Token]int, size) // 位置信息
	tokens = make([]Token, size)
	for i := 0; i < size; i++ {
		token := Token(str[i : i+sliceLength])
		cnt := tokenCntMap[token]
		if cnt > 0 {
			tokens[i] = Token(string(token) + "_" + strconv.Itoa(cnt)) // 混入位置信息
		} else {
			tokens[i] = token
		}
		tokenCntMap[token] = tokenCntMap[token] + 1
	}
	return
}
