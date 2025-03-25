package call

var (
	//WorkerNum                = 4 // WorkerNum worker的数量，对应机器数
	WorkerDuplicateNumOnRing = 4 // WorkerDuplicateNumOnRing 在hash环上，各worker结点虚拟结点数

	IntervalNum                 = 100 // IntervalNum 对于连续属性划分区间时的区间数
	LimitedNumericValueNumLimit = 100 // LimitedNumericValueNumLimit 对于 NumericLimited 类型的数据，value取值能有几项

	FeatureBatchSize     = 16 // FeatureBatchSize 一次同时进行划分的特征数，做完一定数量之后，再起一批新的
	FeatureBatchFillSize = 4  // FeatureBatchFillSize 当还未执行完的特征数减少到这个数目之后，再起一批新的，将数量维持在 FeatureBatchSize，所以要求这个大小一定要小于 FeatureBatchSize
)

// todo:任务结束之后把这些key都delete，如果多个任务共用一个etcd的话，再加个任务名前缀?
// todo:开始之前先以taskId为前缀，看是不是已经存在了，存在的话加点后缀，直到没有重复的为止，但如果同时有多个起的话还是可能不安全

// MASTER_PREFIX_ETCD 在etcd中master结点相关的一些数据存取时要带上这个前缀
const MASTER_PREFIX_ETCD = "taskId/hspawn/master/"

// WORKER_PREFIX_ETCD 向etcd存放worker相关数据时key要带上这个前缀
const WORKER_PREFIX_ETCD = "taskId/hspawn/worker/"

// SUPPORT_PREFIX_ETCD 向etcd中存放supportWorker的信息时，key要带上这个前缀
const SUPPORT_PREFIX_ETCD = "taskId/hspawn/support/" // todo:这个在整个任务结束之后可以清空
