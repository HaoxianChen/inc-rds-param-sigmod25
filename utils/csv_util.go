package utils

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/wxnacy/wgo/arrays"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/common"
	"gitlab.grandhoo.com/rock/rock_v3/request"
)

type TableInfo struct {
	TableName   string
	TableId     string
	ColumnsType map[string]string
	Data        map[string][]interface{}
	RowSize     int
}

func GetCsvCls(path string, skipColumn map[string]int) (map[string]int, int, error) {
	cls := make(map[string]int)
	var totalLine = 0
	preData, err := GetCsvData(path)
	if err != nil {
		fmt.Println("read a csv failed, err:", err)
		return nil, 0, common.ErrReadCsv
	}
	index := 0
	for _, columnName := range preData[0] {
		if _, ok := cls[columnName]; !ok {
			if _, okk := skipColumn[columnName]; !okk {
				cls[columnName] = index
			}
		}
		index++
	}
	totalLine = len(preData) - 1
	return cls, totalLine, nil
}

func GetCsvData(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("opens a csv failed, err:", err)
		return nil, common.ErrOpenCsv
	}
	reader := csv.NewReader(f)
	preData, err := reader.ReadAll()
	if err != nil {
		fmt.Println("read a csv failed, err:", err)
		return nil, common.ErrReadCsv
	}
	return preData, nil
}

func CreateCsv(path string, data [][]string) (string, error) {
	outputFolderPath := "output"
	err := os.MkdirAll(outputFolderPath, 0777)
	if err != nil {
		log.Fatal("Error when mkdir: ", err)
	}

	path = filepath.Join(outputFolderPath, path)

	csvFile, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()
	csvFile.WriteString("\xEF\xBB\xBF")
	csvWriter := csv.NewWriter(csvFile)
	err = csvWriter.WriteAll(data)
	if err != nil {
		fmt.Printf("error (%v)", err)
		return "", err
	}
	absPath, _ := filepath.Abs(path)
	return absPath, nil
}

func GetDataFromCsv(csvInfo *request.CsvInfo) TableInfo {
	path := csvInfo.Path
	tableId := strconv.FormatInt(time.Now().UnixMilli(), 10)
	columnsType := csvInfo.ColumnType
	preData, err := GetCsvData(path)
	columns := csvInfo.Columns
	if err != nil {
		logger.Errorf("read a csv failed, err:", err)
	}
	rowSize := len(preData) - 1
	data := make(map[string][]interface{})
	columnSize := len(preData[0])
	for i := 0; i < columnSize; i++ {
		columnName := preData[0][i]
		if columns != nil && arrays.ContainsString(columns, columnName) == -1 {
			continue
		}
		columnType := columnsType[columnName]
		for j := 1; j <= rowSize; j++ {
			convertedData := TryConvertStringData(preData[j][i], columnType)
			data[columnName] = append(data[columnName], convertedData)
		}
	}
	return TableInfo{
		TableName:   csvInfo.TableName,
		TableId:     tableId,
		ColumnsType: columnsType,
		Data:        data,
		RowSize:     rowSize,
	}
}

// TryConvertStringData 根据指定类型尝试转换interface的类型, 如果转换失败则会保留原有类型
// originalData: 原始数据类型
// targetType: 字符串的类型描述, 例如: "float64", "string"
func TryConvertStringData(originalData string, targetType string) interface{} {
	// 根据类型尝试转换
	switch targetType {
	case "bool":
		if boolValue, err := strconv.ParseBool(originalData); err == nil {
			return boolValue
		}
	case "int64":
		if intValue, err := strconv.ParseInt(originalData, 10, 64); err == nil {
			return intValue
		}
	case "float64":
		if floatValue, err := strconv.ParseFloat(originalData, 64); err == nil {
			return floatValue
		}
	case "time":
		if timeValue, err := time.Parse("2006-01-02", originalData); err == nil {
			return timeValue
		}
	}

	// 类型不支持或者转换失败统一返回原数据
	return originalData
}

func WriteCSV(filename string, data map[string][]interface{}) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 确定最长的切片长度
	maxLength := 0
	for _, values := range data {
		if len(values) > maxLength {
			maxLength = len(values)
		}
	}

	// 写入header
	header := make([]string, 0, len(data))
	for k := range data {
		header = append(header, k)
	}
	sort.Strings(header)
	if err := writer.Write(header); err != nil {
		return err
	}

	// 填充并写入数据
	for i := 0; i < maxLength; i++ {
		row := make([]string, len(data))
		for j, columnName := range header {
			values := data[columnName]
			if i < len(values) {
				row[j] = GetInterfaceToString(values[i])
			} else {
				// 填充缺失的值，这里用空字符串作为示例
				row[j] = ""
			}
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}
