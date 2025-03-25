package list_cluster

type LiCluster[T any] struct {
	data []Element[T]
	free Location
}

type Element[T any] struct {
	Value T
	Next  Location
	Tail  Location
}

type Location int32
type Head = Location

const Null Location = -1

func New[T any]() *LiCluster[T] {
	return &LiCluster[T]{}
}

func NewCapacity[T any](sz int32) *LiCluster[T] {
	return &LiCluster[T]{
		data: make([]Element[T], sz),
	}
}

func (li *LiCluster[T]) NewList(value T) (head Head) {
	head = li.free
	e := Element[T]{
		Value: value,
		Next:  Null, // no next
		Tail:  li.free,
	}
	li.addElement(&e)
	return head
}

func (li *LiCluster[T]) AppendList(head Head, value T) {
	headElement := &li.data[head]
	if headElement.Next == -1 {
		headElement.Next = li.free
	} else {
		li.data[headElement.Tail].Next = li.free
	}
	headElement.Tail = li.free

	e := &Element[T]{
		Value: value,
		Next:  -1, // no next
	}
	li.addElement(e)
}

func (li *LiCluster[T]) ForeachValue(head Head, consumer func(value T)) {
	for {
		headElement := &li.data[head]
		consumer(headElement.Value)

		head = headElement.Next
		if head == -1 {
			break
		}
	}
}

func (li *LiCluster[T]) Length(head Head) (length int) {
	for {
		headElement := &li.data[head]
		length++

		head = headElement.Next
		if head == -1 {
			break
		}
	}
	return length
}

func (li *LiCluster[T]) ToSlice(head Head) []T {
	var length int
	{
		head := head
		for {
			headElement := &li.data[head]
			length++

			head = headElement.Next
			if head == -1 {
				break
			}
		}
	}

	s := make([]T, length)
	for i := 0; i < length; i++ {
		headElement := &li.data[head]
		s[i] = headElement.Value
		head = headElement.Next
	}
	return s
}

func (li *LiCluster[T]) Clean() {
	li.free = 0
}

func (li *LiCluster[T]) addElement(e *Element[T]) {
	if li.free == Location(len(li.data)) {
		li.data = append(li.data, *e)
	} else {
		li.data[li.free] = *e
	}
	li.free++
}
