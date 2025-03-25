package remote

type State int8 // State worker的一些状态

const (
	INIT    State = iota // INIT 初始状态
	PREPARE              // PREPARE 进行一些准备工作
	WORKING              // WORKING 正在执行任务
	FINISH               // FINISH 任务执行完成
	_
	ABORT   // ABORT 任务异常中断
	TIMEOUT // TIMEOUT 与worker的连接超时，暂时应该还没有任务执行超时的说法，到时候可能会加
)

type Obj struct {
	Name string
	Addr string // 还是用ip:port这样的形式
	//state State // 状态标志，这是针对该addr来说的
}
