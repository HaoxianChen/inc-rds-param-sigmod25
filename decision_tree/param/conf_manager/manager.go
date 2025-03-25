package conf_manager

import "gitlab.grandhoo.com/rock/rock_v3/decision_tree/cmd"

// flagContainer 管理所有的参数
var flagContainer = cmd.NewFlagContainer()

// AddCmdArgs 加入一些需要的参数
func AddCmdArgs(flags ...*cmd.Flag) error {
	return flagContainer.AddFlags(flags...)
}

func ParseFlagsWithArgs(args []string) error {
	return flagContainer.Parse(args)
}

func FlagsToString() string {
	return (*flagContainer).String()
}

func FlagsPrintPriority() {
	priorityMap := make(map[string]int, len(flagContainer.GetFlags()))
	for i, flag := range flagContainer.GetFlags() {
		priorityMap[flag.Name] = i
	}
	flagContainer.SetPrintPriority(priorityMap)
}

func FlagsPrint() {
	cmdTablePrint(flagContainer)
}
