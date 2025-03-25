package conf_manager

import (
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/cmd"
	"os"
	"sort"
)

func cmdTablePrint(container *cmd.FlagContainer) {
	//todo 在解析的那边参数顺序打乱了init的顺序，所以在这里要做一下重排序，不太好，后续得改改，在生成那边适配
	newFlagList := make([]*cmd.Flag, len(container.GetFlags()), len(container.GetFlags()))
	for _, flag := range container.GetFlags() {
		index := container.GetPrintPriority()[flag.Name]
		newFlagList[index] = flag
	}
	t := table.NewWriter()
	//todo 这里可以考虑打印在err和文件里面，后续看看怎么设置日志
	t.SetOutputMirror(os.Stderr)
	t.SetColumnConfigs([]table.ColumnConfig{{Name: "First Parameter", Align: text.AlignCenter, AlignHeader: text.AlignCenter, WidthMax: 20, WidthMin: 20},
		{Name: "Second Parameter", Align: text.AlignCenter, AlignHeader: text.AlignCenter, WidthMax: 30, WidthMin: 30},
		{Name: "Value", AlignHeader: text.AlignCenter, WidthMax: 70, WidthMin: 70}})
	t.SetTitle("COMMAND PARAMETER TABLE")
	t.AppendHeader(table.Row{"First Parameter", "Second Parameter", "Value"}, table.RowConfig{AutoMerge: true})
	for _, flag := range newFlagList {
		valueMap := flag.FlagValue.Get()
		if value, ok := valueMap["firstParaValue"]; len(valueMap) == 1 && ok {
			t.AppendRow(table.Row{flag.Name, "/", value})
		} else {
			//输出排个序，每次一致,同时计算一个中位数的位置去放一级参数名
			median := len(valueMap) / 2
			orderedKList := make([]string, 0, len(valueMap))
			for k := range valueMap {
				orderedKList = append(orderedKList, k)
			}
			sort.Strings(orderedKList)
			for i, k := range orderedKList {
				if i == median {
					t.AppendRow(table.Row{flag.Name, k, valueMap[k]})
				} else {
					t.AppendRow(table.Row{"", k, valueMap[k]})
				}
			}
		}
		t.AppendSeparator()
	}
	t.Render()
}
