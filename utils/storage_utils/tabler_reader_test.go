package storage_utils

import (
	"fmt"
	"gitlab.grandhoo.com/rock/storage/storage2/database/database_facade/rich_dababase/irich_dababase"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"gitlab.grandhoo.com/rock/storage/storage2/utils/test"
	"testing"
)

func Test_loadTable(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		name := prepareTable1(richDB)
		table, err := loadTable(name, 0, 0)
		test.PanicErr(err)
		for name, values := range table {
			fmt.Printf("%v %v\n", name, values)
		}
	})
}
