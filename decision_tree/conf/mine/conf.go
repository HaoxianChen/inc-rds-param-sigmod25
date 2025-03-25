package mine

// 一些通用的参数设置就放这了，如果是各个算法相关的参数可以放在相应的其他包下
const (
	// DEBUG 日志debug开关
	DEBUG = false
	// ClosedEnabled closed itemSet开关
	ClosedEnabled = false
	// FreqItemNumLimit 有效freq-item数目限制，控制运算量
	FreqItemNumLimit = 500
)

/*配置相关，命令行解析后赋值*/
var (
	GenerateDataFrameCoreNum = 20 // GenerateDataFrameCoreNum 生成dataFrame并行时的核数，暂定为20
)
