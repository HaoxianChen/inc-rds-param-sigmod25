package cluster

import (
	"errors"
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
)

var GlobalHashRing = HashRing{make(map[uint32]Node), make([]uint32, 0, 10), sync.Mutex{}}

type Node struct {
	ip       string
	port     string
	vid      int32
	hashcode uint32
}

func (n *Node) Init(ip string, port string, vid int32) {
	n.ip, n.port, n.vid = ip, port, vid
	if n.hashcode != 0 {
		logger.Warn("initial hash code is not 0, reassignment to node is not allowed")
	}
	n.hashcode = toHashCode(ip, port, vid)
}

func (n Node) String() string {
	return fmt.Sprintf("(node:%s/%s/%d, hash code: %d)", n.ip, n.port, n.vid, n.hashcode)
}

type HashRing struct {
	Nodes map[uint32]Node
	// HashSlice is a sorted slice of hash code.
	HashSlice []uint32

	lock sync.Mutex
}

func NewHashRing() *HashRing {
	return &HashRing{
		make(map[uint32]Node),
		make([]uint32, 0, 10),
		sync.Mutex{},
	}
}

func (h HashRing) String() string {
	elements := make([]string, len(h.HashSlice), len(h.HashSlice))
	for i := 0; i < len(h.HashSlice); i++ {
		elements[i] = fmt.Sprintf("%d: hashcode is %d, ip is %s, port is %s, and vid is %d",
			i, h.HashSlice[i], h.Nodes[h.HashSlice[i]].ip, h.Nodes[h.HashSlice[i]].port, h.Nodes[h.HashSlice[i]].vid)
	}
	return strings.Join(elements, "\n")
}

func (n Node) GetIp() string {
	return n.ip
}

func (n Node) GetPort() string {
	return n.port
}

func (n Node) GetVid() int32 {
	return n.vid
}

func (n Node) GetHashCode() uint32 {
	return n.hashcode
}

// AddNodesOfLocalHost 将本机对应的所有节点添加到哈希环当中
func (h *HashRing) AddNodesOfLocalHost(ip string, port string, count int) {
	for i := 0; i < count; i++ {
		h.AddNodeOfLocalHost(ip, port, int32(i))
	}
}

// AddNodeOfLocalHost 在本地哈希环中增加一个新的节点
func (h *HashRing) AddNodeOfLocalHost(ip string, port string, vid int32) uint32 {
	hashcode := toHashCode(ip, port, vid)
	node := Node{ip, port, vid, hashcode}
	h.lock.Lock()
	h.Nodes[hashcode] = node
	h.HashSlice = insert(h.HashSlice, hashcode)
	h.lock.Unlock()
	return hashcode
}

// AddNode adds a virtual node to HashRing.
func (h *HashRing) AddNode(node Node) uint32 {
	h.Nodes[node.hashcode] = node
	h.HashSlice = insert(h.HashSlice, node.hashcode)
	return node.hashcode
}

// AddNodeFromEtcd 将从etcd监听到的节点增加到本地的哈希环中
func (h *HashRing) AddNodeFromEtcd(key string, value string) {
	ip, port, vid, _ := parseKeyFromEtcd(key)
	hashCode, _ := strconv.Atoi(value)
	h.lock.Lock()
	if _, isOk := h.Nodes[uint32(hashCode)]; !isOk {
		h.AddNodeWithHashCode(ip, port, vid, uint32(hashCode))
	}
	h.lock.Unlock()
}

// AddNodeWithHashCode 基于节点的完整信息在哈希环中增加一个新的节点
func (h *HashRing) AddNodeWithHashCode(ip string, port string, vid int32, hashcode uint32) {
	node := Node{ip, port, vid, hashcode}
	h.Nodes[hashcode] = node
	h.HashSlice = insert(h.HashSlice, hashcode)
}

// GetNodeByHashCode 输入一个hash值，返回在hash环中的下一个节点
func (h *HashRing) GetNodeByHashCode(hashcode uint32) Node {
	for i := 0; i < len(h.HashSlice); i++ {
		if h.HashSlice[i] >= hashcode {
			return h.Nodes[h.HashSlice[i]]
		}
	}
	return h.Nodes[h.HashSlice[0]]
}

// GetNextNodeByHashCode is slightly different from GetNextNodeByHashCode.
func (h *HashRing) GetNextNodeByHashCode(hashcode uint32) Node {
	for i := 0; i < len(h.HashSlice); i++ {
		if h.HashSlice[i] > hashcode {
			return h.Nodes[h.HashSlice[i]]
		}
	}
	return h.Nodes[h.HashSlice[0]]
}

// GetClusterAddr 返回哈希环中所有节点的ip和端口号
func (h *HashRing) GetClusterAddr() []string {
	mp := make(map[string]bool)
	for _, node := range h.Nodes {
		str := node.ip + ":" + node.port
		mp[str] = true
	}
	res := make([]string, 0, len(mp))
	for s := range mp {
		res = append(res, s)
	}
	return res
}

// GetLocalVidList 获取本地的vid list
func (h *HashRing) GetLocalVidList(ip string) []int32 {
	var res []int32
	for _, node := range h.Nodes {
		if node.ip == ip {
			res = append(res, node.vid)
		}
	}
	return res
}

// toHashCode calculates hash code for a virtual node.
func toHashCode(ip string, port string, vid int32) uint32 {
	return CalcHash(ip + port + strconv.Itoa(int(vid)))
}

// insert 将一个元素插入到一个升序的uint32切片中
func insert(slice []uint32, num uint32) []uint32 {
	slice = append(slice, num)
	if len(slice) == 1 {
		return slice
	}
	for i := 0; i < len(slice)-1; i++ {
		if slice[i] > num {
			copy(slice[i+1:], slice[i:])
			slice[i] = num
			break
		}
	}
	return slice
}

// parseKeyFromEtcd 将从etcd获得的string解析为节点的具体信息
func parseKeyFromEtcd(str string) (ip string, port string, vid int32, err error) {
	elements := strings.Split(str, "/")
	if len(elements) != 4 {
		err = errors.New(fmt.Sprintf("elements are not expected, with %v", elements))
		return
	}
	tmp, err := strconv.Atoi(elements[3])
	if err != nil {
		return
	}
	ip = elements[1]
	port = elements[2]
	vid = int32(tmp)

	return
}

func CalcHash(input string) uint32 {
	return crc32.ChecksumIEEE([]byte(input))
}
