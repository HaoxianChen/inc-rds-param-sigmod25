package add

// FloatAdder 用于浮点数累加
type FloatAdder struct {
	sum float64 // 当前累和结果
	c   float64 // 误差
}

func NewFloatAdder() *FloatAdder {
	return new(FloatAdder)
}

func (adder *FloatAdder) Merge(other FloatAdder) *FloatAdder {
	adder.Add(other.sum)
	adder.Add(other.c)
	return adder
}

func (adder *FloatAdder) Add(num float64) {
	// Kahan summation
	y := num + (*adder).c
	t := (*adder).sum + y
	(*adder).c = y - (t - (*adder).sum)
	(*adder).sum = t
}

func (adder *FloatAdder) Clear() {
	(*adder).sum = 0
	(*adder).c = 0
}

func (adder *FloatAdder) Result() float64 {
	return (*adder).sum
}
