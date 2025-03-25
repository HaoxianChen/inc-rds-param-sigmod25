package storage_utils

import (
	"fmt"
	"github.com/bits-and-blooms/bitset"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock-share/global/utils/similarity"
	"gitlab.grandhoo.com/rock/rock-share/global/utils/similarity/similarity_cluster"
	"gitlab.grandhoo.com/rock/rock-share/global/utils/similarity/similarity_enum"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/request"
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
	"gitlab.grandhoo.com/rock/storage/storage2/utils/managed_memory/memory"
	"gitlab.grandhoo.com/rock/storage/storage2/utils/test"
	"math"
	"os"
	"testing"
	"time"
)

func TestGetTableAllValueIndexes(t *testing.T) {
	tableIndexes, index2Value, _ := GetTableAllValueIndexes(map[string][]interface{}{
		"aaa": {int64(1), int64(4), int64(3), int64(2)},
	}, map[string]string{
		"aaa": rds_config.IntType,
	})
	for col, indexes := range tableIndexes {
		fmt.Println(col, indexes)
	}
	for col, indexes := range index2Value {
		fmt.Println(col, indexes)
	}
}

func TestGetTableAllValueIndexes2(t *testing.T) {
	var values []interface{}
	values = append(values, nil)
	for _, v := range []int{5, 5, 6, 4, 4, 7, 3, 2} {
		values = append(values, int64(v))
	}
	values = append(values, nil)

	tableIndexes, index2Value, _ := GetTableAllValueIndexes(map[string][]interface{}{
		"aaa": values,
	}, map[string]string{
		"aaa": rds_config.IntType,
	})
	for col, indexes := range tableIndexes {
		fmt.Println(col, indexes)
	}
	for col, indexes := range index2Value {
		fmt.Println(col, indexes)
	}
}

func TestGetTableAllValueIndexes3(t *testing.T) {
	tableIndexes, index2Value, _ := GetTableAllValueIndexes(map[string][]interface{}{
		"aaa": {int64(1), int64(4), int64(3), int64(2)},
		"bbb": {"a", "b", "a", "b", "c", nil},
	}, map[string]string{
		"aaa": rds_config.IntType,
		"bbb": rds_config.StringType,
	})
	for col, indexes := range tableIndexes {
		fmt.Println(col, indexes)
	}
	for col, indexes := range index2Value {
		fmt.Println(col, indexes)
	}
}

func TestPairSimilar(t *testing.T) {
	s := similarity.AlgorithmMap[similarity_enum.JaroWinkler]

	f := s.Compare("apple", "apples")

	fmt.Println(f)
}

func TestClusterSimilar(t *testing.T) {
	var cluster [][]int32 = similarity_cluster.CreateCluster[int32]([]string{
		"aaaa",
		"bbb",
		"aaaa",
		"aaaa",
		"bbb",
		"aaaa",
	}, similarity.AlgorithmMap[similarity_enum.JaroWinkler], 0.9)
	for _, group := range cluster {
		fmt.Println(group)
	}
}

func TestForeachParallel(t *testing.T) {
	arr := [100]int{}
	ForeachParallel(20, arr[:], func(i int) {
		time.Sleep(1 * time.Second)
		println(1)
	})
}

func TestForeachParallel2(t *testing.T) {
	var arr []int
	ForeachParallel(20, arr, func(i int) {
		time.Sleep(1 * time.Second)
		println(1)
	})
}

func TestHashBucketInfo(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		tableName := prepareTable1(richDB)
		println(richDB.Show(tableName))
		hashBucketNumber := 2
		for i := 0; i < hashBucketNumber; i++ {
			cellRowIds, err := richDB.CellRowIds(tableName, "age", int64(i), int64(hashBucketNumber))
			test.PanicErr(err)
			println(fmt.Sprintf("%v", cellRowIds))
		}
		rock_db.DB = richDB
		bucketInfo := test.PanicErr1(HashBucketInfo(tableName, "age", hashBucketNumber))
		fmt.Printf("%v", bucketInfo)
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

func prepareTable1(richDB irich_dababase.IRichDatabase) (tableName string) {
	tableName = "stu"
	test.PanicErr(richDB.CreateTable(tableName, 10, []*table_impl.ColumnConf{
		{ColumnName: "name", LogicType: logic_type.STRING, HasIndexing: true},
		{ColumnName: "age", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
		{ColumnName: "score", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
		{ColumnName: "good", LogicType: logic_type.BOOLEAN, HasIndexing: true},
		{ColumnName: "time", LogicType: logic_type.TIME, HasIndexing: true},
		{ColumnName: "money", LogicType: logic_type.MONEY, HasIndexing: true},
	}))
	test.PanicErr(richDB.InsertBatch(tableName, [][]interface{}{
		{"f", "k", "c", "a", "e1011111111111111111111111111111111111111", "a", "g", "", "", nil},
		{1, "2", 3, 4, 5, "5", nil, 8, 9, 10},
		{1, 2.2, 3, "4.4", 5, 6, nil, math.NaN(), math.Inf(1), math.Inf(-1)},
		{true, true, true, "false", false, "false", nil, nil, nil, nil},
		{time.Now().Add(10 * time.Second), time.Now().Add(-10 * time.Second), time.Now(), time.Now(), time.Now(), nil, nil, nil, nil, nil},
		{1.1, nil, nil, nil, 2, 2.2, 2.2, nil, nil, nil},
	}))
	test.PanicErr(richDB.RefreshIndexing(tableName))
	return
}

func prepareTable2(richDB irich_dababase.IRichDatabase) (tableName string) {
	tableName = "stu1"
	test.PanicErr(richDB.CreateTable(tableName, 10, []*table_impl.ColumnConf{
		{ColumnName: "name1", LogicType: logic_type.STRING, HasIndexing: true},
		{ColumnName: "age1", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
		{ColumnName: "score1", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
		{ColumnName: "good1", LogicType: logic_type.BOOLEAN, HasIndexing: true},
		{ColumnName: "time1", LogicType: logic_type.TIME, HasIndexing: true},
		{ColumnName: "money1", LogicType: logic_type.MONEY, HasIndexing: true},
	}))
	test.PanicErr(richDB.InsertBatch(tableName, [][]interface{}{
		{"g", "g", "g", "a", "e1011111111111111111111111111111111111111", "a", "g", "", "", nil},
		{1, "2", 3, 4, 5, "5", nil, 8, 9, 10},
		{1, 2.2, 3, "4.4", 5, 6, nil, math.NaN(), math.Inf(1), math.Inf(-1)},
		{false, false, true, "false", false, "false", nil, nil, true, nil},
		{time.Now().Add(10 * time.Second), time.Now().Add(-10 * time.Second), time.Now(), time.Now(), time.Now(), nil, nil, nil, nil, nil},
		{1.1, nil, nil, nil, 2, 2.2, 2.2, nil, nil, nil},
	}))
	test.PanicErr(richDB.RefreshIndexing(tableName))
	return
}

func prepareTable3(richDB irich_dababase.IRichDatabase) (tableName string) {
	tableName = "stu2"
	test.PanicErr(richDB.CreateTable(tableName, 10, []*table_impl.ColumnConf{
		{ColumnName: "name2", LogicType: logic_type.STRING, HasIndexing: true},
		{ColumnName: "age2", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
		{ColumnName: "score2", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
		{ColumnName: "good2", LogicType: logic_type.BOOLEAN, HasIndexing: true},
		{ColumnName: "time2", LogicType: logic_type.TIME, HasIndexing: true},
		{ColumnName: "money2", LogicType: logic_type.MONEY, HasIndexing: true},
	}))
	test.PanicErr(richDB.InsertBatch(tableName, [][]interface{}{
		{"g", "g", "g", "a", "e1011111111111111111111111111111111111111", "a", "g", "", "", nil},
		{1, "2", 3, 4, 5, "5", nil, 8, 9, 10},
		{1, 2.2, 3, "4.4", 5, 6, nil, math.NaN(), math.Inf(1), math.Inf(-1)},
		{false, false, true, "false", false, "false", nil, nil, true, nil},
		{time.Now().Add(10 * time.Second), time.Now().Add(-10 * time.Second), time.Now(), time.Now(), time.Now(), nil, nil, nil, nil, nil},
		{1.1, nil, nil, nil, 2, 2.2, 2.2, nil, nil, nil},
	}))
	test.PanicErr(richDB.RefreshIndexing(tableName))
	return
}

func prepareTable4(richDB irich_dababase.IRichDatabase) (tableName string) {
	tableName = "stu"
	test.PanicErr(richDB.CreateTable(tableName, 10, []*table_impl.ColumnConf{
		{ColumnName: "name", LogicType: logic_type.STRING, HasIndexing: true},
		{ColumnName: "age", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
		{ColumnName: "score", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
		{ColumnName: "good", LogicType: logic_type.BOOLEAN, HasIndexing: true},
		{ColumnName: "time", LogicType: logic_type.TIME, HasIndexing: true},
		{ColumnName: "money", LogicType: logic_type.MONEY, HasIndexing: true},
	}))
	test.PanicErr(richDB.InsertBatch(tableName, [][]interface{}{
		{"f", "k", "c", "a", "e1011111111111111111111111111111111111111", "a", "g", "", "", nil},
		{1, "2", 3, 4, 5, "5", nil, 8, 9, 10},
		{1, 2.2, 3, "4.4", 5, 6, nil, math.NaN(), math.Inf(1), math.Inf(-1)},
		{true, true, true, "false", false, "false", nil, nil, nil, nil},
		{time.Now().Add(10 * time.Second), time.Now().Add(-10 * time.Second), time.Now(), time.Now(), time.Now(), nil, nil, nil, nil, nil},
		{1.1, nil, nil, nil, 2.2, 2.2, 2.2, nil, nil, nil},
	}))
	test.PanicErr(richDB.RefreshIndexing(tableName))
	return
}

func prepareTable5(richDB irich_dababase.IRichDatabase) (tableName string) {
	tableName = "stu"
	test.PanicErr(richDB.CreateTable(tableName, 10, []*table_impl.ColumnConf{
		{ColumnName: "name", LogicType: logic_type.STRING, HasIndexing: true},
		{ColumnName: "age", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
		{ColumnName: "equal", LogicType: logic_type.BIGDECIMAL, HasIndexing: true},
	}))
	test.PanicErr(richDB.InsertBatch(tableName, [][]interface{}{
		{"apple", "hello", "apple2", "apples", "world", "hello!", "worlds", "", "", nil},
		{1, "2", 3, 4, 5, "5", nil, 8, 9, 10},
		{1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1},
	}))
	test.PanicErr(richDB.RefreshIndexing(tableName))
	return
}

func TestCrossColumnHashBucketSizes(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		tableName := prepareTable1(richDB)
		println(richDB.Show(tableName))
		hashBucketNumber := 2
		for i := 0; i < hashBucketNumber; i++ {
			cellRowIds, err := richDB.CellRowIds(tableName, "age", int64(i), int64(hashBucketNumber))
			test.PanicErr(err)
			println(fmt.Sprintf("%v\n", cellRowIds))
		}
		for i := 0; i < hashBucketNumber; i++ {
			cellRowIds, err := richDB.CellRowIds(tableName, "name", int64(i), int64(hashBucketNumber))
			test.PanicErr(err)
			println(fmt.Sprintf("%v\n", cellRowIds))
		}
		rock_db.DB = richDB
		bucketInfo := test.PanicErr1(HashBucketInfo(tableName, "age", hashBucketNumber))
		fmt.Printf("%v\n", bucketInfo)
		bucketInfo = test.PanicErr1(HashBucketInfo(tableName, "name", hashBucketNumber))
		fmt.Printf("%v\n", bucketInfo)

		sizes := test.PanicErr1(CrossColumnHashBucketSizes(tableName, "name", tableName, "age", hashBucketNumber))
		fmt.Printf("%v\n", sizes)
	})
}

// "t0.name=a"
// cube [a] [{stu [3 5]}]
func TestHyperCubeT0Const(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tableName := prepareTable1(richDB)
		println(richDB.Show(tableName))
		predicates := []rds.Predicate{
			{
				PredicateStr: "t0.name=a",
				LeftColumn: rds.Column{
					TableId:  tableName,
					ColumnId: "name",
				},
				RightColumn:   rds.Column{},
				ConstantValue: "a",
				PredicateType: 0,
			},
		}
		hyperCube, _ := test.PanicErr2(DoHyperCube(predicates, nil))
		for key, cube := range hyperCube {
			fmt.Printf("cube %s %v\n", key.String(), cube)
		}
	})
}

// "t0.good=true"
func TestHyperCubeT01Const(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tableName := prepareTable1(richDB)
		println(richDB.Show(tableName))
		predicates := []rds.Predicate{
			{
				PredicateStr: "t0.good=true",
				LeftColumn: rds.Column{
					TableId:     tableName,
					ColumnId:    "good",
					ColumnIndex: 0,
				},
				RightColumn:   rds.Column{},
				ConstantValue: true,
				PredicateType: 0,
			}, {
				PredicateStr: "t1.good=false",
				LeftColumn: rds.Column{
					TableId:     tableName,
					ColumnId:    "good",
					ColumnIndex: 1,
				},
				RightColumn:   rds.Column{},
				ConstantValue: false,
				PredicateType: 0,
			},
		}
		hyperCube, _ := test.PanicErr2(DoHyperCube(predicates, nil))
		for key, cube := range hyperCube {
			fmt.Printf("cube %s %v\n", key.String(), cube)
		}
	})
}

// t0.name=t2.name1
// cube [e1011111111111111111111111111111111111111] [{stu [4]}, {}, {stu1 [4]}]
// cube [g] [{stu [6]}, {}, {stu1 [0, 1, 2, 6]}]
// cube [a] [{stu [3, 5]}, {}, {stu1 [3, 5]}]
func TestHyperCubeCross(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tableName1 := prepareTable1(richDB)
		tableName2 := prepareTable2(richDB)
		println(richDB.Show(tableName1))
		println(richDB.Show(tableName2))
		predicates := []rds.Predicate{
			{
				PredicateStr: "t0.name=t2.name1",
				LeftColumn: rds.Column{
					TableId:  tableName1,
					ColumnId: "name",
				},
				RightColumn: rds.Column{
					TableId:  tableName2,
					ColumnId: "name1",
				},
				ConstantValue: nil,
				PredicateType: 1,
			},
		}
		hyperCube, _ := test.PanicErr2(DoHyperCube(predicates, nil))
		for key, cube := range hyperCube {
			fmt.Printf("cube %s %s\n", key.String(), slices2string(cube))
		}
	})
}

// t0.name=a ^ t0.good=t1.good
// cube [a false] [{stu [3, 5]}, {stu [3, 4, 5]}]
func TestHyperCubeT0ConstT0NConst(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tableName := prepareTable1(richDB)
		println(richDB.Show(tableName))
		predicates := []rds.Predicate{
			{
				PredicateStr: "t0.name=a",
				LeftColumn: rds.Column{
					TableId:  tableName,
					ColumnId: "name",
				},
				RightColumn:   rds.Column{},
				ConstantValue: "a",
				PredicateType: 0,
			}, {
				PredicateStr: "t0.good=t1.good",
				LeftColumn: rds.Column{
					TableId:  tableName,
					ColumnId: "good",
				},
				RightColumn: rds.Column{
					TableId:  tableName,
					ColumnId: "good",
				},
				ConstantValue: nil,
				PredicateType: 1,
			},
		}
		hyperCube, _ := test.PanicErr2(DoHyperCube(predicates, nil))
		for key, cube := range hyperCube {
			fmt.Printf("cube %s %s\n", key.String(), slices2string(cube))
		}
	})
}

// t0.good=t1.good
// cube [true] [{stu [0, 1, 2]}, {stu [0, 1, 2]}]
// cube [false] [{stu [3, 4, 5]}, {stu [3, 4, 5]}]
func TestHyperCubeNoConstPred(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tableName := prepareTable1(richDB)
		println(richDB.Show(tableName))
		predicates := []rds.Predicate{
			{
				PredicateStr: "t0.good=t1.good",
				LeftColumn: rds.Column{
					TableId:  tableName,
					ColumnId: "good",
				},
				RightColumn: rds.Column{
					TableId:  tableName,
					ColumnId: "good",
				},
				ConstantValue: nil,
				PredicateType: 1,
			},
		}
		hyperCube, _ := test.PanicErr2(DoHyperCube(predicates, nil))
		for key, cube := range hyperCube {
			fmt.Printf("cube %s %s\n", key.String(), slices2string(cube))
		}
	})
}

// t0.name = a ^ t2.name1 = g
// cube [a g] [{stu [3, 5]}, {}, {stu1 [0, 1, 2, 6]}]
func TestHyperCubeCrossTable2(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tableName := prepareTable1(richDB)
		tableName2 := prepareTable2(richDB)
		println(richDB.Show(tableName))
		println(richDB.Show(tableName2))
		predicates := []rds.Predicate{
			{
				PredicateStr: "t0.name=a",
				LeftColumn: rds.Column{
					TableId:  tableName,
					ColumnId: "name",
				},
				RightColumn:   rds.Column{},
				ConstantValue: "a",
				PredicateType: 0,
			},
			{
				PredicateStr: "t2.name=a",
				LeftColumn: rds.Column{
					TableId:  tableName2,
					ColumnId: "name1",
				},
				RightColumn:   rds.Column{},
				ConstantValue: "g",
				PredicateType: 0,
			},
		}
		hyperCube, _ := test.PanicErr2(DoHyperCube(predicates, nil))
		for key, cube := range hyperCube {
			fmt.Printf("cube %s %s\n", key.String(), slices2string(cube))
		}
	})
}

// t0.good = t2.good
// cube [true] [{stu [0, 1, 2]}, {}, {stu1 [2, 8]}]
// cube [false] [{stu [3, 4, 5]}, {}, {stu1 [0, 1, 3, 4, 5]}]
func TestHyperCubeCrossTable3(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tableName := prepareTable1(richDB)
		tableName2 := prepareTable2(richDB)
		println(richDB.Show(tableName))
		println(richDB.Show(tableName2))
		predicates := []rds.Predicate{
			{
				PredicateStr: "t0.good = t2.good",
				LeftColumn: rds.Column{
					TableId:  tableName,
					ColumnId: "good",
				},
				RightColumn: rds.Column{
					TableId:  tableName2,
					ColumnId: "good1",
				},
				ConstantValue: nil,
				PredicateType: 1,
			},
		}
		hyperCube, _ := test.PanicErr2(DoHyperCube(predicates, nil))
		for key, cube := range hyperCube {
			fmt.Printf("cube %s %s\n", key.String(), slices2string(cube))
		}
	})
}

// t0.good = t2.good ^ t2.name1 = t4.name2
// cube [true g] [{stu [0, 1, 2]}, {}, {stu1 [2]}, {}, {stu2 [0, 1, 2, 6]}]
// cube [false g] [{stu [3, 4, 5]}, {}, {stu1 [0, 1]}, {}, {stu2 [0, 1, 2, 6]}]
// cube [false a] [{stu [3, 4, 5]}, {}, {stu1 [3, 5]}, {}, {stu2 [3, 5]}]
// cube [false e1011111111111111111111111111111111111111] [{stu [3, 4, 5]}, {}, {stu1 [4]}, {}, {stu2 [4]}]
func TestHyperCubeCross2Table4(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tableName := prepareTable1(richDB)
		tableName2 := prepareTable2(richDB)
		tableName3 := prepareTable3(richDB)
		println(richDB.Show(tableName))
		println(richDB.Show(tableName2))
		predicates := []rds.Predicate{
			{
				PredicateStr: "t0.good = t2.good",
				LeftColumn: rds.Column{
					TableId:  tableName,
					ColumnId: "good",
				},
				RightColumn: rds.Column{
					TableId:  tableName2,
					ColumnId: "good1",
				},
				ConstantValue: nil,
				PredicateType: 1,
			}, {
				PredicateStr: "t2.name1 = t4.name2",
				LeftColumn: rds.Column{
					TableId:  tableName2,
					ColumnId: "name1",
				},
				RightColumn: rds.Column{
					TableId:  tableName3,
					ColumnId: "name2",
				},
				ConstantValue: nil,
				PredicateType: 1,
			},
		}
		hyperCube, _ := test.PanicErr2(DoHyperCube(predicates, nil))
		for key, cube := range hyperCube {
			fmt.Printf("cube %s %s\n", key.String(), slices2string(cube))
		}
	})
}

func TestSelectSameColumn(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		tableName := prepareTable1(richDB)
		println(richDB.Show(tableName))
		fmt.Printf("%v\n", test.PanicErr1(richDB.Query(sql.SingleTable.SELECT("age", "name", "age").FROM(tableName))))
		fmt.Printf("%v\n", test.PanicErr1(richDB.Query(sql.SingleTable.SELECT("age", "name", "name").FROM(tableName))))
	})
}

func TestBinaryFound(t *testing.T) {
	fmt.Println(BinaryFound([]int32{1, 2, 3}, 2))
}

func TestTableDataQuery(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tabName := prepareTable1(richDB)
		println(richDB.Show(tabName))
		//data, err := TableDataQuery(map[string]map[int32]struct{}{
		//	tabName: {
		//		1: struct{}{},
		//		3: struct{}{},
		//		5: struct{}{},
		//		6: struct{}{},
		//	},
		//})
		bs := bitset.New(0)
		bs.Set(1)
		bs.Set(3)
		bs.Set(5)
		bs.Set(6)
		data, err := TableDataQuery(map[string]*bitset.BitSet{
			tabName: bs,
		}, map[tableName][]columnName{
			tabName: {"age"},
		}, map[string]Pair[int64, int64]{
			tabName: {0, 100},
		})
		test.PanicErr(err)
		for tabName, columnData := range data {
			for colName, data := range columnData {
				fmt.Printf("%v %v %v\n", tabName, colName, data)
			}
		}
	})
}

func TestImportTable(t *testing.T) {
	pageMemory, err := page_memory.New(memory_file_system.New(), 4*memory.KB, 256*memory.MB)
	if err != nil {
		panic(err)
	}
	baseDB := database_impl_s3_base.New(pageMemory, 100)
	rock_db.DB = rich_dababase.New(baseDB)

	fn := "tmp.csv"
	err = os.WriteFile(fn, []byte("name,age\naaa,12\nvvv,13"), 0666)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = os.Remove(fn)
	}()

	tab := ImportTable(&request.DataSource{
		TableName: fn,
	})

	logger.Info(rock_db.DB.Show(tab))
}
