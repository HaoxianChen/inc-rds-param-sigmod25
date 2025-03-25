package local_distance_blocking

import (
	"fmt"
	"testing"

	"gitlab.grandhoo.com/rock/rock-share/global/utils/similarity"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_reader"
	"gitlab.grandhoo.com/rock/storage/storage2/database/database_facade/database_facade_impl"
	"gitlab.grandhoo.com/rock/storage/storage2/database/database_facade/rich_dababase/rich_dababase_impl"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/line"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/table_impl"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/logic_type"
	"gitlab.grandhoo.com/rock/storage/storage2/filesystem/localFS"
	"gitlab.grandhoo.com/rock/storage/storage2/memory/physical_memory"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"gitlab.grandhoo.com/rock/storage/storage2/utils/test"
)

func Test_doGroupBy(t *testing.T) {
	var values = []string{
		"apple, apple, 0",
		"orange, orange, 10",
		"apple, apple, 1",
		"apple, apple, 2",
		"",
		"orange, orange, 11",
		"orange, orange, 12",
		"apple, apple, 3",
	}
	groups := doGroupBy(values, func(s string, s2 string) float64 {
		return similarity.Jaccard(s, s2)
	}, 0.5)
	fmt.Printf("%v\n", values)
	fmt.Printf("%v\n", groups)
}

func Test_parallel(t *testing.T) {
	var bs = make([]byte, 200)
	parallel(0, 200, func(i int) {
		bs[i] = 'a'
	})
	fmt.Println(string(bs))
}

func TestNewReader(t *testing.T) {
	rock_db.DFS = localFS.New("storage")
	database, err := database_facade_impl.New(physical_memory.New())
	test.PanicErr(err)
	rock_db.DB = rich_dababase_impl.New(database)

	err = rock_db.DB.CreateTable("stu", 10, []*table_impl.ColumnConf{
		{
			ColumnName: "name",
			LogicType:  logic_type.STRING,
		},
	})
	test.PanicErr(err)

	test.PanicErr(rock_db.DB.Insert("stu", line.Line{"apple, apple, 0"}))
	test.PanicErr(rock_db.DB.Insert("stu", line.Line{"apple, apple, 0"}))
	test.PanicErr(rock_db.DB.Insert("stu", line.Line{"orange, orange, 11"}))
	test.PanicErr(rock_db.DB.Insert("stu", line.Line{"apple, apple, 0"}))
	test.PanicErr(rock_db.DB.Insert("stu", line.Line{""}))
	test.PanicErr(rock_db.DB.Insert("stu", line.Line{"apple, apple, 0"}))
	test.PanicErr(rock_db.DB.Insert("stu", line.Line{"apple, apple, 0"}))
	test.PanicErr(rock_db.DB.Insert("stu", line.Line{"apple, apple, 0"}))
	test.PanicErr(rock_db.DB.Insert("stu", line.Line{"orange, orange, 12"}))
	test.PanicErr(rock_db.DB.Insert("stu", line.Line{"apple, apple, 0"}))

	err = GroupBy("stu", "name", func(s string, s2 string) float64 {
		return similarity.Jaccard(s, s2)
	}, 0.5)

	test.PanicErr(err)

	reader := blocking_reader.New("stu", []string{"name"}, blocking_conf.LocalDistanceBlockingFileSuffix, 0.6)

	for i := 0; i < 10; i++ {
		tokenId, err := reader.ReadMostFreqTokenId(int32(i))
		test.PanicErr(err)
		fmt.Println(i, tokenId)
	}

}
