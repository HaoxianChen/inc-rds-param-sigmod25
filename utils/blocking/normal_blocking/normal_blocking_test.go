package normal_blocking

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"gitlab.grandhoo.com/rock/storage/config"
	"gitlab.grandhoo.com/rock/storage/storage2/database/database_facade/rich_dababase"
	"gitlab.grandhoo.com/rock/storage/storage2/database/database_facade/rich_dababase/irich_dababase"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/table_impl"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/logic_type"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"gitlab.grandhoo.com/rock/storage/storage2/storage3/database/database_impl/database_impl_s3_base"
	"gitlab.grandhoo.com/rock/storage/storage2/storage3/database/database_impl/database_impl_s3_tx/database_impl_s3_tx_impl"
	"gitlab.grandhoo.com/rock/storage/storage2/storage3/file_system/memory_file_system"
	"gitlab.grandhoo.com/rock/storage/storage2/storage3/page_memory/page_memory"
	"gitlab.grandhoo.com/rock/storage/storage2/utils/test"
	"testing"
)

func TestTokenize(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DFS = rich_dababase.NewLocalDFS(config.RootDir)
		rock_db.DB = richDB
		tableName := prepareTable(richDB)
		tokenFreqMap := test.PanicErr1(Tokenize(tableName, []string{"name"}, 0, 0))
		for token, frequency := range tokenFreqMap {
			fmt.Println(token, "-", frequency)
		}
	})
}

func useDBS3(use func(richDB irich_dababase.IRichDatabase)) {
	mfs := memory_file_system.New()
	pageMemory := test.PanicErr1(page_memory.New(mfs, 16*1024, 5*1024*1024*1024))
	db3base := database_impl_s3_base.New(pageMemory, 10)
	db3tx := database_impl_s3_tx_impl.New(db3base)
	rdb := rich_dababase.New(db3tx)
	test.PanicErr(database_impl_s3_tx_impl.InitLogTables(db3tx))
	use(rdb)
}

func prepareTable(richDB irich_dababase.IRichDatabase) (tableName string) {
	tableName = "stu"
	test.PanicErr(richDB.CreateTable(tableName, 10, []*table_impl.ColumnConf{
		{ColumnName: "name", LogicType: logic_type.STRING, HasIndexing: true},
	}))
	test.PanicErr(richDB.InsertBatch(tableName, [][]interface{}{
		{nil, "apple", "apples", "world", nil, "world", "aa", "ba"},
	}))
	test.PanicErr(richDB.RefreshIndexing(tableName))
	return
}

func TestCreateDerivedColumn(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DFS = rich_dababase.NewLocalDFS(config.RootDir)
		rock_db.DB = richDB
		tableName := prepareTable(richDB)
		columnName := "name"
		lines := test.PanicErr1(richDB.Query(sql.SingleTable.SELECT(columnName).FROM(tableName)))

		tokenFreqMap := test.PanicErr1(Tokenize(tableName, []string{columnName}, 0, 0))
		for token, frequency := range tokenFreqMap {
			fmt.Println(token, "-", frequency)
		}
		sortedTokens := TokenFreqSort(tokenFreqMap)
		tokenWords := test.PanicErr1(CreateDerivedColumn(tableName, []string{columnName}, 0, sortedTokens))
		for i, word := range tokenWords {
			value := lines[i][0]
			tokens := mapping(word[:], func(e blocking_conf.TokenId) Token {
				if e == 0 {
					return "/"
				}
				return sortedTokens[e]
			})
			fmt.Printf("%v %v %v\n", value, word, tokens)
		}
	})
}

func mapping[E, R any](es []E, f func(E) R) []R {
	var rs []R
	for _, e := range es {
		rs = append(rs, f(e))
	}
	return rs
}

func TestReadDerivedColumn(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DFS = rich_dababase.NewLocalDFS(config.RootDir)
		rock_db.DB = richDB
		tableName := prepareTable(richDB)
		columnName := "name"
		lines := test.PanicErr1(richDB.Query(sql.SingleTable.SELECT(columnName).FROM(tableName)))

		tokenFreqMap := test.PanicErr1(Tokenize(tableName, []string{columnName}, 0, 0))
		for token, frequency := range tokenFreqMap {
			fmt.Println(token, "-", frequency)
		}
		sortedTokens := TokenFreqSort(tokenFreqMap)
		tokenWords := test.PanicErr1(CreateDerivedColumn(tableName, []string{columnName}, 0, sortedTokens))
		for i, word := range tokenWords {
			value := lines[i][0]
			tokens := mapping(word[:], func(e blocking_conf.TokenId) Token {
				return sortedTokens[e]
			})
			fmt.Printf("%v %v %v\n", value, word, tokens)
		}

		column, err := blocking_conf.ReadDerivedColumn(blocking_conf.FilePath(tableName, []string{columnName}, 0, blocking_conf.NormalBlockingFileSuffix))
		test.PanicErr(err)
		fmt.Printf("%v", column)
	})
}

func TestLocalBlocking(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DFS = rich_dababase.NewLocalDFS(config.RootDir)
		rock_db.DB = richDB
		tableName := prepareTable(richDB)
		columnName := "name"

		lenW, err := rock_db.DB.Query(sql.SingleTable.SELECT(sql.CountAsterisk).FROM(tableName))
		test.PanicErr(err)

		err = LocalMemoryBlocking(tableName, columnName, lenW[0][0].(int64))
		test.PanicErr(err)

		column, err := blocking_conf.ReadDerivedColumn(blocking_conf.FilePath(tableName, []string{columnName}, 0, blocking_conf.NormalBlockingFileSuffix))
		test.PanicErr(err)
		fmt.Printf("%v", column)
	})
}
