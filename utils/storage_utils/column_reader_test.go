package storage_utils

import (
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_memery"
	"gitlab.grandhoo.com/rock/rock_v3/common"
	"gitlab.grandhoo.com/rock/storage/storage2/database/database_facade/rich_dababase/irich_dababase"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"gitlab.grandhoo.com/rock/storage/storage2/utils/test"
	"testing"
)

func TestRead(t *testing.T) {
	const mem = "1G"
	rock_memery.InitMemoryManager(&rock_memery.MemoryConfig{StorageMem: mem, ShuffleMem: mem, IntersectionMem: mem})
	storageTaskMemManager := rock_memery.NewTaskMemoryManager(rock_memery.MemoryManagerInstance)
	common.StorageSliceAgent = rock_memery.NewRockSliceAgent(storageTaskMemManager, rock_memery.StorageCalc)

	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		name := prepareTable1(richDB)
		column, err := ColumnReader.GetColumnValue(name, "money")
		test.PanicErr(err)
		for i := 0; i < int(column.Length()); i++ {
			logger.Info(column.Get(ColumnValueRowIdType(i)))
		}

		column, err = ColumnReader.GetColumnValue(name, "money")
		test.PanicErr(err)
		for i := 0; i < int(column.Length()); i++ {
			logger.Info(column.Get(ColumnValueRowIdType(i)))
		}

		column, err = ColumnReader.GetColumnValue(name, "time")
		test.PanicErr(err)
		for i := 0; i < int(column.Length()); i++ {
			logger.Info(column.Get(ColumnValueRowIdType(i)))
		}
	})
}

func TestLength(t *testing.T) {
	const mem = "5G"
	rock_memery.InitMemoryManager(&rock_memery.MemoryConfig{StorageMem: mem, ShuffleMem: mem, IntersectionMem: mem})
	storageTaskMemManager := rock_memery.NewTaskMemoryManager(rock_memery.MemoryManagerInstance)
	common.StorageSliceAgent = rock_memery.NewRockSliceAgent(storageTaskMemManager, rock_memery.StorageCalc)

	length := 150 * 100 * 100
	values, _, err := common.StorageSliceAgent.MakeRockSliceWithCapacity(rock_memery.FLOAT64, length)
	test.PanicErr(err)
	for i := 0; i < length; i++ {
		_, err = common.StorageSliceAgent.Append(values, float64Of(length))
	}

	logger.Info("size", values.Size)
}

func TestLogAny(t *testing.T) {
	logger.Info("example", []any{1, 2, 3})
	logger.Info("example", []any{false, false, false})
}
