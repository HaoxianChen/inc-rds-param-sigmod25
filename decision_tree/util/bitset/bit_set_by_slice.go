package bitset

type BitSetBySlice struct {
	data []uint64
}

func NewBitSetBySlice(d []uint64) *BitSetBySlice {
	return &BitSetBySlice{d}
}

// NewBitSetBySliceWithCap 预先分配位串的长度，输入参数为需要的位数
func NewBitSetBySliceWithCap(bitLength int) *BitSetBySlice {
	// fixme:考虑一下，对于bitLength小于0的情况要不要返回nil，这样外面初始化时可能要改
	blockSize := 0
	if bitLength > 0 {
		blockSize = (bitLength + 63) / 64
	}
	data := make([]uint64, blockSize, blockSize)
	return &BitSetBySlice{
		data: data,
	}
}

// FillWithOnes 设置从start这一位开始的num位为1
func (bitSet *BitSetBySlice) FillWithOnes(start, num int) {
	if start < 0 || num < 0 {
		// panic("negative param in fill-ones")
		// fixme:可以加个错误处理
		return
	}
	startBlockIndex := start / 64 // 设置哪一块，从0起
	startPosInBlock := start % 64

	endBlockIndex := (start + num) / 64 // 结束于哪一块，可能会多加一个空块
	endPosInBlock := (start + num) % 64

	if len((*bitSet).data) < endBlockIndex+1 { // 要扩容
		newData := make([]uint64, endBlockIndex+1, endBlockIndex+1)
		copy(newData, (*bitSet).data)
		(*bitSet).data = newData
	}
	if startBlockIndex != endBlockIndex {
		(*bitSet).data[startBlockIndex] = (*bitSet).data[startBlockIndex] | ^((1 << startPosInBlock) - 1) // 这一块从starPos(可以取到)开始全部置1

		for i := startBlockIndex + 1; i < endBlockIndex; i++ {
			(*bitSet).data[i] = ^uint64(0)
		}

		(*bitSet).data[endBlockIndex] = (*bitSet).data[endBlockIndex] | ((1 << endPosInBlock) - 1) // 这一块从0开始到endPos(取不到)全部置1
	} else {
		// 在一块里操作
		(*bitSet).data[startBlockIndex] = (*bitSet).data[startBlockIndex] | ((^((1 << startPosInBlock) - 1)) & ((1 << endPosInBlock) - 1)) // 上面两个交集
	}
}

// SetBit 设置该位为1
func (bitSet *BitSetBySlice) SetBit(pos int) {
	if pos < 0 {
		// panic("negative position in set bit")
		// fixme:可以加个错误处理
		return
	}
	blockIndex := pos / 64 // 设置哪一块，从0起
	posInBlock := pos % 64
	if len((*bitSet).data) < blockIndex+1 { // 要扩容
		newData := make([]uint64, blockIndex+1, blockIndex+1)
		copy(newData, (*bitSet).data)
		(*bitSet).data = newData
	}
	(*bitSet).data[blockIndex] = (*bitSet).data[blockIndex] | (1 << posInBlock)
}

// GetBit 获取该位bit，返回0则该位为0，返回非0则是1
func (bitSet *BitSetBySlice) GetBit(pos int) int {
	if pos < 0 {
		// panic("negative position in set bit")
		// fixme:可以加个错误处理
		return 0
	}
	blockIndex := pos / 64 // 设置哪一块，从0起
	posInBlock := pos % 64
	if len((*bitSet).data) < blockIndex+1 { // 要扩容
		return 0
	} else {
		if (*bitSet).data[blockIndex]&(1<<posInBlock) != 0 {
			return 1
		} else {
			return 0
		}
	}
}

// Shift 移动offset位
func (bitSet *BitSetBySlice) Shift(offset uint64) {
	allZero := true
	for _, data := range (*bitSet).data {
		if data != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return
	}

	// 设offset为k，移动后最多得到两块数据，记为高一块和低一块
	// 一共64位进行移动，只要看第0位和第63位的移动
	// ((63+k) % 64) + 1，有多少位移到高一块
	// (0+k+63) / 64，高一块的地址

	// 分别拿到要移到高一块和低一块的那几位
	highBitsNum := int(((63 + offset) % 64) + 1)
	lowBitsNum := 64 - highBitsNum

	lowBitsMask := uint64((1 << lowBitsNum) - 1)
	highBitsMask := ^lowBitsMask

	highBlockOffset := int((offset + 63) / 64)

	newSlice := make([]uint64, len((*bitSet).data)+highBlockOffset, len((*bitSet).data)+highBlockOffset)
	for blockIndex, blockData := range (*bitSet).data {
		highBits := blockData & highBitsMask
		lowBits := blockData & lowBitsMask

		blockIndex += highBlockOffset
		newSlice[blockIndex] = newSlice[blockIndex] | (highBits >> lowBitsNum)

		if lowBitsNum == 0 {
			// 都在一块里，就是移动了64的整数倍
			// 那就不用再存低位了
		} else {
			// 高位和低位要分开存
			blockIndex -= 1 // 不会为负
			newSlice[blockIndex] = newSlice[blockIndex] | (lowBits << highBitsNum)
		}
	}

	(*bitSet).data = newSlice
}

// Union 与另一个位串进行或操作
func (bitSet *BitSetBySlice) Union(other BitSet) {
	switch other.(type) {
	case *BitSetBySlice:
		dataInOther := (*other.(*BitSetBySlice)).data
		unionLen := len(dataInOther)
		if len((*bitSet).data) < unionLen { // 先扩容
			newData := make([]uint64, unionLen, unionLen)
			copy(newData, (*bitSet).data)
			(*bitSet).data = newData
		}
		for blockIndex, blockDataInOther := range dataInOther {
			(*bitSet).data[blockIndex] = (*bitSet).data[blockIndex] | blockDataInOther
		}
	default:
		for _, data := range other.AllOneBitsInUint64() {
			bitSet.SetBit(int(data))
		}
	}
}

// UnionWithOffset 和Union方法有点不同，另一个BitSet每一位要加上offset后再进行合并，相当于把另一个位串中各位移动offset位之后再合并
func (bitSet *BitSetBySlice) UnionWithOffset(other BitSet, offset uint64) {
	dataInOther := (*other.(*BitSetBySlice)).data // 可能panic
	//isBySlice, dataInOther := other.BySlice()
	//if !isBySlice {
	//	panic("inConsistent")
	//}

	allZero := true
	for _, data := range dataInOther {
		if data != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return
	}
	// 设offset为k，移动后最多得到两块数据，记为高一块和低一块
	// 一共64位进行移动，只要看第0位和第63位的移动
	// ((63+k) % 64) + 1，有多少位移到高一块
	// (0+k+63) / 64，高一块的地址

	// 分别拿到要移到高一块和低一块的那几位
	highBitsNum := int(((63 + offset) % 64) + 1)
	lowBitsNum := 64 - highBitsNum

	lowBitsMask := uint64((1 << lowBitsNum) - 1)
	highBitsMask := ^lowBitsMask

	highBlockOffset := int((offset + 63) / 64)

	unionLen := len(dataInOther) + highBlockOffset
	if len((*bitSet).data) < unionLen { // 先扩容
		newData := make([]uint64, unionLen, unionLen)
		copy(newData, (*bitSet).data)
		(*bitSet).data = newData
	}

	for blockIndex, blockDataInOther := range dataInOther {
		highBits := blockDataInOther & highBitsMask
		lowBits := blockDataInOther & lowBitsMask

		blockIndex += highBlockOffset
		(*bitSet).data[blockIndex] = (*bitSet).data[blockIndex] | (highBits >> lowBitsNum)

		if lowBitsNum == 0 {
			// 都在一块里，就是移动了64的整数倍
			// 那就不用再存低位了
		} else {
			// 高位和低位要分开存
			blockIndex -= 1 // 不会为负
			(*bitSet).data[blockIndex] = (*bitSet).data[blockIndex] | (lowBits << highBitsNum)
		}
	}
}

// Intersect 与另一个位串进行与操作
func (bitSet *BitSetBySlice) Intersect(other BitSet) {
	dataInOther := (*other.(*BitSetBySlice)).data // 可能panic
	//isBySlice, dataInOther := other.BySlice()
	//if !isBySlice {
	//	panic("inConsistent")
	//}
	unionLen := len(dataInOther)
	if len((*bitSet).data) < unionLen { // 先扩容
		newData := make([]uint64, unionLen, unionLen)
		copy(newData, (*bitSet).data)
		(*bitSet).data = newData
	}
	for blockIndex, blockDataInOther := range dataInOther {
		(*bitSet).data[blockIndex] = (*bitSet).data[blockIndex] & blockDataInOther
	}
}

// Count 计数，位串中有几个1
func (bitSet *BitSetBySlice) Count() uint64 {
	var count uint64 = 0
	for _, block := range (*bitSet).data {
		for keep := block; keep != 0; {
			count += 1
			keep = keep & (keep - 1)
		}
	}
	return count
}

// AllOneBitsInUint32 获得具体有哪几位为1
func (bitSet *BitSetBySlice) AllOneBitsInUint32() []uint32 {
	res := make([]uint32, 0, bitSet.Count())
	for blockIndex, block := range (*bitSet).data {
		for onePos := uint32(0); block != 0; onePos++ {
			if block&0x1 == 1 {
				// 最低位是1
				res = append(res, (uint32(blockIndex)<<6)+onePos)
			}
			block >>= 1 // 如果是int的话，是逻辑位移，所以还是改uint64了
		}
	}
	return res
}

// AllOneBitsInUint64 获得具体有哪几位为1
func (bitSet *BitSetBySlice) AllOneBitsInUint64() []uint64 {
	res := make([]uint64, 0, bitSet.Count())
	for blockIndex, block := range (*bitSet).data {
		for onePos := uint64(0); block != 0; onePos++ {
			if block&0x1 == 1 {
				// 最低位是1
				res = append(res, (uint64(blockIndex)<<6)+onePos)
			}
			block >>= 1 // 如果是int的话，是逻辑位移，所以还是改uint64了
		}
	}
	return res
}

// Clear 清一下slice，为了垃圾回收
func (bitSet *BitSetBySlice) Clear() {
	(*bitSet).data = make([]uint64, 0)
}

func (bitSet *BitSetBySlice) ByMap() (bool, map[int]uint64) {
	return false, nil
}

func (bitSet *BitSetBySlice) BySlice() (bool, []uint64) {
	return true, (*bitSet).data
}

func (bitSet *BitSetBySlice) GetSimpleCount() uint64 {
	return 0
}
