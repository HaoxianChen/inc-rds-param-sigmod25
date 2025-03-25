package local_jaccard_similar

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/global/utils/similarity/similarity_impl/jaccard_similarity"
	"testing"
)

func TestCalculate(t *testing.T) {
	left := []string{
		"apple",
		"apples",
	}
	right := []string{
		"apple",
	}
	pairs := calculate0(left, right, 0.85)
	for _, pair := range pairs {
		fmt.Println(left[pair[0]], right[pair[1]])
	}
}

func TestCalculate2(t *testing.T) {
	left := []string{
		"123457abcdef",
	}
	right := []string{
		"123456abcdef",
	}
	pairs := calculate0(left, right, 0.60)
	for _, pair := range pairs {
		fmt.Println(left[pair[0]], right[pair[1]])
	}

	compare := jaccard_similarity.INSTANCE.Compare("123457abcdef", "123456abcdef")
	fmt.Println(compare)
}

func Test_tokenizer(t *testing.T) {
	tokens := tokenizer("无锡市扬名机械厂")
	fmt.Println(tokens)
}
