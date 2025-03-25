package lsh_blocking

import (
	"bufio"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"strings"
	"testing"
)

func Test_doBlocking(t *testing.T) {
	list := doBlocking([][]int{
		{100, 1, 2, 3, 10, 11, 12},
		{200, 4, 5, 6, 21, 22, 23},
		{300, 1, 5, 3, 32, 33, 34},
	})
	for _, tokenIds := range list {
		logger.Errorf("%v", tokenIds)
	}
}

func TestScanner(t *testing.T) {
	var s = "apple\r\norange\r\npear"
	reader := strings.NewReader(s)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		text := scanner.Text()
		println(text)
	}
}
