/*
	决策树结构，暂时还是按照二叉树的实现来，对于离散值(多叉树)暂时也先当连续值来处理
	树的组织结构，用个数组吧，因为这样的树的结点不会很多，数组开销也不大，到时候要序列化或什么的也方便
	限制:
	1.标签y只考虑单维度，即一次只做一个属性的判定，但对应取值可以有多项(多分类)、
	2.
*/

package tree

import (
	"fmt"
	"github.com/awalterschulze/gographviz"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"os"
)

// NodeId Node的id，root为0，对应树数组的下标
type NodeId int

type DecisionInfo struct {
	Feature    FeatureId // Feature  当前结点用于结点划分的特征
	SplitValue float64   // SplitValue 用于划分的特征值取值

	Impurity float64 // Impurity 衡量指标，criterion取值，如信息熵、基尼系数等
	IsLeft   bool    // IsLeft 该分段是左分段还是右分段

	// 一些support计数等
	StatisticInfo
}

type Node struct {
	parent NodeId
	// 因为root不会出现在child中，如果为0表示没有相应结点
	leftChild  NodeId
	rightChild NodeId

	statistic *DecisionInfo
}

type Tree struct {
	//featureNum uint32 // featureNum 树中涉及的特征数量
	//classNum   uint32 // classNum 分类时的类别数
	maxDepth  uint32 // maxDepth 树的最大深度，根为0
	nodeCount NodeId // nodeCount 进行nodeId分配，最后对应树中node数目(包含root)
	nodes     []Node // nodes 树中所有结点
	//value     [][]float64 // value 各node中各类label的value计数

	capacity int // capacity 容量
}

func (t *Tree) MaxDepth() uint32 {
	return (*t).maxDepth
}

func (t *Tree) SetMaxDepth(depth uint32) {
	(*t).maxDepth = depth
}

func (t *Tree) NodeNum() int {
	return int((*t).nodeCount)
}

func (t *Tree) ToSimpleGraph(outPath string) {
	graphAst, _ := gographviz.Parse([]byte(`digraph G{}`))
	graph := gographviz.NewGraph()
	gographviz.Analyse(graphAst, graph)

	nodeNum := int(t.nodeCount)
	for i := 0; i < nodeNum; i++ {
		nodeI := t.nodes[i]
		if nodeI.leftChild == 0 && nodeI.rightChild == 0 {
			// 叶子结点
			graph.AddNode("G", fmt.Sprintf("%d", i), map[string]string{"label": fmt.Sprintf("< id = %d<br/>impurity = %v<br/>samples = %d<br/>value = %v>",
				i, nodeI.statistic.Impurity, nodeI.statistic.NodeMultiCount, nodeI.statistic.ClassMultiCount)})
		} else {
			graph.AddNode("G", fmt.Sprintf("%d", i), map[string]string{"label": fmt.Sprintf("<id = %d<br/>X[%d] &lt; %v <br/>impurity = %v<br/>samples = %d<br/>value = %v>",
				i, nodeI.statistic.Feature, nodeI.statistic.SplitValue, nodeI.statistic.Impurity, nodeI.statistic.NodeMultiCount, nodeI.statistic.ClassMultiCount)})
		}
	}

	for i := 0; i < nodeNum; i++ {
		nodeI := t.nodes[i]
		if nodeI.leftChild != 0 {
			// 获取子节点的实例
			//child := t.nodes[nodeI.leftChild]
			//values := make([]float64, 0, child.statistic.NodeMultiCount)
			//for _, ins := range child.statistic.SomeInstances {
			//	insV := dataFrame.GetFloat64Element(ins, featureList[nodeI.statistic.Feature])
			//	values = append(values, insV)
			//}
			//sort.Float64s(values)
			//fmt.Printf("in node[%d] split: instances(positive insNum:%d) %v \n left child(positive insNum:%d): %v \n", i, len(nodeI.statistic.SomeInstances), nodeI.statistic.SomeInstances, len(values), values)

			graph.AddEdge(fmt.Sprintf("%d", i), fmt.Sprintf("%d", nodeI.leftChild), true, nil)
		}
		if nodeI.rightChild != 0 {
			//child := t.nodes[nodeI.rightChild]
			//values := make([]float64, 0, child.statistic.NodeMultiCount)
			//for _, ins := range child.statistic.SomeInstances {
			//	insV := dataFrame.GetFloat64Element(ins, featureList[nodeI.statistic.Feature])
			//	values = append(values, insV)
			//}
			//sort.Float64s(values)
			//fmt.Printf(" right child(positive insNum:%d): %v \n", len(values), values)

			graph.AddEdge(fmt.Sprintf("%d", i), fmt.Sprintf("%d", nodeI.rightChild), true, nil)
		}
	}

	out, err := os.Create(outPath)
	if err != nil {
		logger.Errorf("error when open file:%s--%v", outPath, err)
		return
	}
	_, err = out.WriteString(graph.String())
	if err != nil {
		logger.Errorf("error when write to file:%s--%v", outPath, err)
		return
	}
	err = out.Close()
	if err != nil {
		logger.Errorf("error when close file:%s--%v", outPath, err)
		return
	}
}

func NewTreeWithCap(cap int) *Tree {
	t := &Tree{}
	t.ReSize(cap)
	return t
}

func NewTreeWithDepth(depth uint32) *Tree {
	return &Tree{maxDepth: depth}
}

func (t *Tree) ReSize(cap int) {
	if cap == t.capacity && len(t.nodes) != 0 {
		return
	}
	if cap < 0 {
		if t.capacity == 0 {
			cap = 3 // 初始容量
		} else {
			cap = t.capacity * 2
		}
	}
	newNodes := make([]Node, cap)
	//newValue := make([][]float64, cap)
	copy(newNodes, t.nodes[:t.nodeCount])
	//copy(newValue, t.value[:t.nodeCount])

	t.nodes = newNodes
	//t.value = newValue

	// fixme:nodeCount是能这么改的吗，这个逻辑不对吧，sklearn里是这样的，到时候再确认一下
	//if cap < int(t.nodeCount) {
	//	t.nodeCount = NodeId(cap)
	//}
	t.capacity = cap

}

func (t *Tree) NextNodeId() NodeId {
	return t.nodeCount
}

func (t *Tree) AddNode(
	parent NodeId, isLeft bool, isLeaf bool,
	feature FeatureId, splitValue float64, impurity float64,
	statisticInfo *StatisticInfo,
) NodeId {
	nodeId := t.NextNodeId()
	if int(nodeId) >= t.capacity {
		t.ReSize(-1)
	}
	node := &t.nodes[nodeId]
	node.parent = parent
	node.statistic = &DecisionInfo{
		Impurity:      impurity,
		StatisticInfo: *statisticInfo,
		IsLeft:        isLeft,
	}

	if parent >= 0 {
		// 如果小于0的话，表示当前结点是root
		if isLeft {
			t.nodes[parent].leftChild = nodeId
		} else {
			t.nodes[parent].rightChild = nodeId
		}
	}

	if isLeaf {
		// todo:看看要不要对叶子结点做一点设置，好像没必要，反正只要left和right为0就表示没有孩子
	} else {
		node.statistic.Feature = feature
		node.statistic.SplitValue = splitValue
	}
	t.nodeCount += 1
	return nodeId
}

func (t *Tree) Predict(X [][]float64) []float64 {
	// todo:to be implemented
	// 相当于是规则应用？
	return nil
}

// ParentFeatures 获取从root到dest的路径上所有涉及的feature(不重复)
func (t *Tree) ParentFeatures(dest NodeId) []FeatureId {
	if dest == -1 {
		// 在root上面，没有路径
		return nil
	}
	featureMap := make(map[FeatureId]struct{}, t.maxDepth)
	for dest != -1 {
		featureMap[t.nodes[dest].statistic.Feature] = struct{}{}
		dest = t.nodes[dest].parent
	}
	featureNum := len(featureMap)
	featurePath := make([]FeatureId, 0, featureNum)
	for k, _ := range featureMap {
		featurePath = append(featurePath, k)
	}
	return featurePath
}

func (t *Tree) DecisionPaths() [][]*DecisionInfo {
	// todo:这里的实现可以之后再看着调整，可以直接记住所有叶子结点，然后往上找路径就好
	// 深度优先遍历
	if t.nodeCount == 0 {
		// 连root都没有
		return nil
	}
	stack := NewNodeStack()
	stack.Push(NodeVisit{node: 0, hasVisited: false}) // 从root开始
	pathKeep := make([]NodeId, 0, t.maxDepth+1)
	decisions := [][]*DecisionInfo(nil)

	cur := NodeVisit{}
	curNode := Node{}
	for !stack.IsEmpty() {
		cur = stack.Pop()
		if cur.hasVisited {
			// 第二次被pop了
			// path回退
			pathKeep = pathKeep[:len(pathKeep)-1]
			continue
		}

		pathKeep = append(pathKeep, cur.node) // 取到的就算进path里
		curNode = t.nodes[cur.node]
		if curNode.leftChild == 0 && curNode.rightChild == 0 {
			// 是叶子结点
			pathLen := len(pathKeep)
			curDecision := make([]*DecisionInfo, pathLen)
			for i := 0; i < pathLen; i++ {
				curDecision[i] = t.nodes[pathKeep[i]].statistic
			}
			decisions = append(decisions, curDecision)
			// 回退一下
			pathKeep = pathKeep[:pathLen-1]
		} else {
			cur.hasVisited = true
			stack.Push(cur) // 再放回去

			if curNode.rightChild != 0 {
				cur.node = curNode.rightChild
				cur.hasVisited = false
				stack.Push(cur)
			}
			if curNode.leftChild != 0 {
				cur.node = curNode.leftChild
				cur.hasVisited = false
				stack.Push(cur)
			}
		}
	}

	return decisions
}

type NodeVisit struct {
	node       NodeId
	hasVisited bool
}

type NodeStack struct {
	records []NodeVisit
	cur     int // 栈顶，可以取到，初始化为-1表示没有元素
}

func NewNodeStack() *NodeStack {
	return &NodeStack{
		cur: -1,
	}
}

func (stack *NodeStack) Push(record NodeVisit) {
	stack.records = append(stack.records, record)
	stack.cur += 1
}

func (stack *NodeStack) Peek() NodeVisit {
	if stack.cur >= 0 {
		return stack.records[stack.cur]
	}
	return NodeVisit{node: -1}
}

func (stack *NodeStack) Pop() NodeVisit {
	top := stack.Peek()
	if top.node >= 0 {
		stack.records = stack.records[:stack.cur]
		stack.cur -= 1
	}
	return top
}

func (stack *NodeStack) Clear() {
	stack.cur = -1
	stack.records = stack.records[:0]
}

func (stack *NodeStack) IsEmpty() bool {
	return len(stack.records) == 0
}

type StatisticInfo struct {
	NodeSingleCount  int             // NodeSingleCount 该结点实例的singleCount
	NodeMultiCount   int             // NodeMultiCount 该结点实例的multiCount
	ClassSingleCount map[float64]int // ClassSingleCount 该结点实例各个label的singleCount统计
	ClassMultiCount  map[float64]int // ClassMultiCount 该结点实例各个label的multiCount统计

	// 找实例的行为还是后置吧，因为最好让规则相关的属性非NaN
	//SomeInstances []string // SomeInstances 保存部分实例的json串
}

func (s *StatisticInfo) Init() {
	*s = StatisticInfo{} // 清空一下
}

type SplitRecord struct {
	Feature       FeatureId // Feature 用于划分的特征
	SplitValue    float64   // SplitValue 用于划分的值
	LeftWeight    float64   // LeftWeight 划分之后左半部分的权重和
	RightWeight   float64   // RightWeight 划分之后右半部分的权重和
	LeftImpurity  float64   // LeftImpurity 划分之后左半部分impurity
	RightImpurity float64   // RightImpurity 划分之后右半部分impurity
	Improvement   float64   // Improvement 划分之后的提升度
}

func (r *SplitRecord) Init() {
	*r = SplitRecord{}
	(*r).Improvement = NEG_INFINITY
}

// Valid 一次划分结果是否有效
func (r *SplitRecord) Valid() bool {
	// 如果提升度是负无穷，则是无效的
	return (*r).Improvement != NEG_INFINITY
}

// grh
func PrintDecisionPath(paths [][]*DecisionInfo) {
	for i := 0; i < len(paths); i++ {
		fmt.Printf("%s%20s%20s%20s\n", "Feature", "SplitValue", "Impurity", "IsLeft")
		for j := 0; j < len(paths[i]); j++ {
			node := paths[i][j]
			fmt.Printf("%d", node.Feature)
			fmt.Printf("%20f", node.SplitValue)
			fmt.Printf("%20f", node.Impurity)
			fmt.Printf("\t\t%v\n", node.IsLeft)
		}
		fmt.Println()
	}
}
