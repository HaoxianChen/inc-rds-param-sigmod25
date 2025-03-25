package call

import (
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"
	"math"
)

// Builder 用于构建树，树结构本身是比较独立的，相关逻辑都放到Builder中
type Builder interface {
	Build(worker *ServerOnWorker, tree *tree.Tree, manager *ManagerOnMaster, stopTask bool)
	Init(options ...BuildOption)
}

type BuilderBasic struct {
	Builder
	// todo:到时候建树的时候传一个TreeBuildOption吧

	maxFeatureNum uint32 // maxFeatureNum 会用到的最大特征数，会对特征随机采样，采样到这个数目
	// 剪枝相关的一些参数
	minImpurityDecrease float64 // minImpurityDecrease 分裂时需要达到的最小impurity提升
	minSupportRate      float64 // minSupportRate 这个support对应水平扩展的support
	minSamplesInSplit   int     // minSamplesInSplit 树内部结点最少需要的实例数
	maxDepth            uint32  // maxDepth 构建的树的最大深度
}

func (bb *BuilderBasic) Init(options ...BuildOption) {
	// todo:看这里要哪些参数吧
	// todo:要先建立criterion和splitter，Init方法后面会调
	setting := &buildOptions{
		// todo:给一些默认设置
		minImpurityDecrease:   0,
		minSamplesRateInSplit: 0,
		maxDepth:              math.MaxUint32,
		maxFeatureNum:         math.MaxUint32,
	}

	for _, option := range options {
		option(setting)
	}

	bb.set(setting)
}

func (bb *BuilderBasic) set(setting *buildOptions) {
	bb.maxFeatureNum = setting.maxFeatureNum
	bb.minImpurityDecrease = setting.minImpurityDecrease
	bb.minSupportRate = setting.minSamplesRateInSplit
	bb.maxDepth = setting.maxDepth
}

type StackRecord struct {
	depth    uint32
	parent   tree.NodeId
	isLeft   bool
	impurity float64
}

type RecordStack struct {
	records []StackRecord
	cur     int // 栈顶，可以取到，初始化为-1表示没有元素
}

func NewRecordStack() *RecordStack {
	return &RecordStack{
		cur: -1,
	}
}

func (stack *RecordStack) Push(record StackRecord) {
	stack.records = append(stack.records, record)
	stack.cur += 1
}

func (stack *RecordStack) Peek() *StackRecord {
	if stack.cur >= 0 {
		return &stack.records[stack.cur]
	}
	return nil
}

func (stack *RecordStack) Pop() *StackRecord {
	top := stack.Peek()
	if top != nil {
		stack.records = stack.records[:stack.cur]
		stack.cur -= 1
	}
	return top
}

func (stack *RecordStack) Clear() {
	stack.cur = -1
	stack.records = stack.records[:0]
}

func (stack *RecordStack) IsEmpty() bool {
	return len(stack.records) == 0
}

type BuildOption func(op *buildOptions)

func MinImpurityDecrease(v float64) BuildOption {
	return func(op *buildOptions) {
		op.minImpurityDecrease = v
	}
}

func MinSupportRateInSplit(v float64) BuildOption {
	return func(op *buildOptions) {
		op.minSamplesRateInSplit = v
	}
}

func MaxDepth(v uint32) BuildOption {
	return func(op *buildOptions) {
		op.maxDepth = v
	}
}

func MaxFeatureNum(v uint32) BuildOption {
	return func(op *buildOptions) {
		op.maxFeatureNum = v
	}
}

type buildOptions struct {
	minImpurityDecrease   float64 // minImpurityDecrease 分裂时需要达到的最小impurity提升
	minSamplesRateInSplit float64 // minSamplesRateInSplit 树内部结点最少需要的实例数，这里只能给一个比例，因为没有全局实例信息
	maxDepth              uint32  // maxDepth 构建的树的最大深度
	maxFeatureNum         uint32  // maxFeatureNum Split时最多会考虑的特征数(从这么多个特征中选一个，如果有的话)
}

func CriterionByName(n string) ClassifierOption {
	return func(op *classifierOptions) {
		switch n {
		case "entropy":
			// 基于信息熵
			op.criterion = tree.Entropy{}
		case "gini":
			// 基于基尼系数
			op.criterion = tree.Gini{}
		default:
			logger.Errorf("unknown criterion:%s", n)
		}
	}
}

func BuilderByName(n string) ClassifierOption {
	return func(op *classifierOptions) {
		switch n {
		case "depth":
			// 根据最大深度来构建
			builder := NewDepthFirstTreeBuilder()
			op.builder = &builder.BuilderBasic
		default:
			logger.Errorf("unknown builder:%s", n)
		}
	}
}

func BuilderDirect(bd *BuilderBasic) ClassifierOption {
	return func(op *classifierOptions) {
		op.builder = bd
	}
}

type ClassifierOption func(op *classifierOptions)

type classifierOptions struct {
	criterion tree.Criterion
	builder   *BuilderBasic
}

func NewDepthFirstTreeBuilder() *DepthFirstTreeBuilder {
	builder := &DepthFirstTreeBuilder{}
	builder.Builder = builder
	return builder
}

type DepthFirstTreeBuilder struct {
	BuilderBasic
}

func (dft *DepthFirstTreeBuilder) Build(worker *ServerOnWorker, t *tree.Tree, manager *ManagerOnMaster, stopTask bool) {
	initCap := 0
	if t.MaxDepth() <= 10 {
		initCap = int(math.Pow(2, float64(t.MaxDepth()+1))) + 1
	} else {
		initCap = 2047
	}
	t.ReSize(initCap)

	maxFeatureNum := dft.maxFeatureNum
	maxDepth := dft.maxDepth
	minSamplesInSplit := dft.minSamplesInSplit
	minImpurityDecrease := dft.minImpurityDecrease

	// 一些初始化值
	impurity := tree.INFINITY
	maxDepthSeen := uint32(0)
	depth := uint32(0)
	parent := tree.NodeId(-1)
	isLeft := false
	isLeaf := false
	splitRecord := tree.SplitRecord{}
	statisticInfo := tree.StatisticInfo{}
	nodeId := tree.NodeId(-1)

	stack := NewRecordStack()
	// 先放root
	stack.Push(StackRecord{
		isLeft:   true, // root的数据放在left
		depth:    0,
		parent:   -1,
		impurity: tree.INFINITY,
	})

	for !stack.IsEmpty() {
		if stopTask {
			return
		}
		record := stack.Pop()

		depth = record.depth
		parent = record.parent
		isLeft = record.isLeft
		impurity = record.impurity // 如果是root的话，是正无穷

		manager.Split(worker, int64(record.parent), record.isLeft, int64(t.NextNodeId()), impurity, maxFeatureNum, minSamplesInSplit, t.ParentFeatures(record.parent), &splitRecord, &statisticInfo, stopTask)
		if record.parent == -1 {
			// 是root，算一个support
			//log.Info().Msgf("pattern support <%d, %d>", statisticInfo.NodeSingleCount, statisticInfo.NodeMultiCount)
			minSamplesInSplit = int(dft.minSupportRate * float64(statisticInfo.NodeMultiCount))
			dft.minSamplesInSplit = minSamplesInSplit
		}
		// 一些提前结束条件
		isLeaf = depth >= maxDepth || !splitRecord.Valid() || splitRecord.Improvement+tree.EPSILON < minImpurityDecrease

		nodeId = t.AddNode(parent, isLeft, isLeaf, splitRecord.Feature, splitRecord.SplitValue, impurity, &statisticInfo)
		if nodeId < 0 {
			break
		}

		if !isLeaf {
			// 分别把右结点和左结点放到stack里
			stack.Push(StackRecord{
				depth:    depth + 1,
				parent:   nodeId,
				isLeft:   false,
				impurity: splitRecord.RightImpurity,
			})

			stack.Push(StackRecord{
				depth:    depth + 1,
				parent:   nodeId,
				isLeft:   true,
				impurity: splitRecord.LeftImpurity,
			})
		}

		if depth > maxDepthSeen {
			maxDepthSeen = depth
		}
	}

	t.ReSize(t.NodeNum()) // 压缩一下空间
	t.SetMaxDepth(maxDepthSeen)
}

type BestFirstTreeBuilder struct {
	BuilderBasic
	maxLeafNum int // maxLeafNum 最大叶子结点数，对应于划分次数
}

func NewBestFirstTreeBuilder(leafNum int) *BestFirstTreeBuilder {
	builder := &BestFirstTreeBuilder{maxLeafNum: leafNum}
	builder.Builder = builder
	return builder
}

// addSplitNode 将点进行split加入树中，虽然进行了划分，但只是把这次划分记录一下(得一个improvement放入堆中)，如果是叶子结点的话，就没必要放进堆里了。之后从堆里取出来之后再对两个子分段做该操作
func (bft *BestFirstTreeBuilder) addSplitNode(
	worker *ServerOnWorker,
	t *tree.Tree, manager *ManagerOnMaster,
	parent tree.NodeId, depth uint32, impurity float64, isLeft bool,
	record *SplitRecordInfo,
	stopTask bool) {

	minImpurityDecrease := bft.minImpurityDecrease

	splitRecord := tree.SplitRecord{}
	statisticInfo := tree.StatisticInfo{}

	manager.Split(worker, int64(parent), isLeft, int64(t.NextNodeId()), impurity, bft.maxFeatureNum, bft.minSamplesInSplit, t.ParentFeatures(parent), &splitRecord, &statisticInfo, stopTask)
	if parent == -1 {
		// 是root，算一个support
		bft.minSamplesInSplit = int(bft.minSupportRate * float64(statisticInfo.NodeMultiCount))
	}
	// 一些提前结束条件
	isLeaf := depth >= bft.maxDepth || !splitRecord.Valid() || splitRecord.Improvement+tree.EPSILON < minImpurityDecrease

	// 这个hasNaN是上次划分得到该子结点时，是否有NaN在这一块里，也就是说是针对上一个划分的key的，所以要外面单独传
	nodeId := t.AddNode(parent, isLeft, isLeaf, splitRecord.Feature, splitRecord.SplitValue, impurity, &statisticInfo)
	if nodeId < 0 {
		return
	}

	record.nodeId = nodeId
	record.depth = depth

	// 外面判断是不是叶子就根据Improvement是不是0来判断了
	if !isLeaf {
		record.improvement = splitRecord.Improvement
		record.leftImpurity = splitRecord.LeftImpurity
		record.rightImpurity = splitRecord.RightImpurity
	} else {
		record.improvement = 0
		record.leftImpurity = impurity
		record.rightImpurity = impurity
	}
}

func (bft *BestFirstTreeBuilder) Build(worker *ServerOnWorker, t *tree.Tree, manager *ManagerOnMaster, stopTask bool) {
	maxLeafNum := bft.maxLeafNum
	maxSplitNum := bft.maxLeafNum - 1
	t.ReSize(maxLeafNum + maxSplitNum)

	// 一些初始化值
	maxDepthSeen := uint32(0)
	splitInfo := SplitRecordInfo{}

	heap := NewRecordHeap(0)

	// 这里的做法和dfs是不一样的，要先split，然后把node加到tree里之后，再放进堆里
	// 先把root加入
	bft.addSplitNode(worker, t, manager, -1, 0, tree.INFINITY, false, &splitInfo, stopTask)
	if splitInfo.improvement != 0 {
		// 非叶子结点
		heap.Push(splitInfo.Copy())
	}
	// 然后开始循环
	for !heap.IsEmpty() {
		if maxSplitNum <= 0 {
			// 结束了
			break
		}
		maxSplitNum -= 1
		record := heap.Pop()
		// 根据这个划分情况做实际的划分
		// 左边
		splitInfo = SplitRecordInfo{} // 清空一下
		bft.addSplitNode(worker, t, manager, record.nodeId, record.depth+1, record.leftImpurity, true, &splitInfo, stopTask)
		if splitInfo.improvement != 0 {
			// 非叶子结点
			heap.Push(splitInfo.Copy())
		}
		// 右边
		splitInfo = SplitRecordInfo{} // 清空一下
		bft.addSplitNode(worker, t, manager, record.nodeId, record.depth+1, record.rightImpurity, false, &splitInfo, stopTask)
		if splitInfo.improvement != 0 {
			// 非叶子结点
			heap.Push(splitInfo.Copy())
		}

		if record.depth > maxDepthSeen {
			maxDepthSeen = record.depth
		}
	}

	t.ReSize(t.NodeNum()) // 压缩一下空间
	t.SetMaxDepth(maxDepthSeen)
}

// SplitRecordInfo 记录一次分裂信息
type SplitRecordInfo struct {
	nodeId tree.NodeId // 当前结点(start~end范围)的id
	depth  uint32      // 该结点(start~end范围)所处的depth

	//impurity      float64 // start~end的impurity，作为后续start~pos，pos~end的parent的impurity
	leftImpurity  float64 // 分裂时能顺便计算出左边和右边impurity就顺便存着吧
	rightImpurity float64
	improvement   float64 // 将该结点进行分裂能得到的improvement，判断好坏的依据
}

func (r *SplitRecordInfo) Copy() *SplitRecordInfo {
	copied := *r
	return &copied
}

type RecordHeap struct {
	records []*SplitRecordInfo // fixme:到时候看要不要用指针
}

func NewRecordHeap(cap int) *RecordHeap {
	return &RecordHeap{
		records: make([]*SplitRecordInfo, 0, cap),
	}
}

func (heap *RecordHeap) IsEmpty() bool {
	return len((*heap).records) == 0
}

// Push 往堆里添加元素
func (heap *RecordHeap) Push(r *SplitRecordInfo) {
	(*heap).records = append((*heap).records, r)
	heap.siftUp(len((*heap).records) - 1)
}

// Pop 获取堆顶元素
func (heap *RecordHeap) Pop() *SplitRecordInfo {
	if len((*heap).records) == 0 {
		return nil
	}
	res := (*heap).records[0]
	(*heap).records[0] = (*heap).records[len((*heap).records)-1]
	(*heap).records = (*heap).records[0 : len((*heap).records)-1]
	heap.siftDown(0)
	return res
}

func (heap *RecordHeap) ReHeap() {
	size := len((*heap).records)
	if size == 0 {
		return
	}
	for cur := (size - 2) / 2; cur >= 0; cur-- {
		heap.siftDown(cur)
	}
}

// siftDown 向下调整
func (heap *RecordHeap) siftDown(cur int) {
	size := len((*heap).records)
	if cur >= size {
		return
	}

	left := 2*cur + 1
	right := left + 1

	curElem := (*heap).records[cur]
	for left < size {
		next := left
		if right < size {
			// 左右都看
			if (*heap).records[right].improvement > (*heap).records[left].improvement {
				next = right
			}
		}
		if (*heap).records[next].improvement > curElem.improvement {
			// 换下去
			(*heap).records[cur] = (*heap).records[next]
			cur = next
			left = 2*cur + 1
			right = left + 1
		} else {
			break
		}
	}
	(*heap).records[cur] = curElem
}

// siftUp 向上调整
func (heap *RecordHeap) siftUp(cur int) {
	size := len((*heap).records)
	if cur >= size {
		return
	}

	curElem := (*heap).records[cur]
	parent := (cur - 1) / 2
	for parent >= 0 {
		if (*curElem).improvement > (*(*heap).records[parent]).improvement {
			// 换上去
			(*heap).records[cur] = (*heap).records[parent]
			cur = parent
			if cur == 0 {
				// -1/2 是0？
				break
			}
			parent = (cur - 1) / 2
		} else {
			break
		}
	}
	(*heap).records[cur] = curElem
}

type Classifier struct {
	criterion tree.Criterion
	builder   *BuilderBasic
}

func NewClassifier(options ...ClassifierOption) *Classifier {
	c := &classifierOptions{
		criterion: tree.Entropy{},
		builder:   &NewDepthFirstTreeBuilder().BuilderBasic,
	}

	for _, option := range options {
		option(c)
	}

	return &Classifier{
		criterion: c.criterion,
		builder:   c.builder,
	}
}

func (c *Classifier) Fit(worker *ServerOnWorker, manager *ManagerOnMaster, stopTask bool, options ...BuildOption) *tree.Tree {
	c.builder.Builder.Init(options...)
	tree := tree.NewTreeWithDepth(c.builder.maxDepth)
	// splitter和criterion的初始化都放到Build里了
	manager.DataFrameReset(worker)
	c.builder.Build(worker, tree, manager, stopTask)

	return tree
}
