package set

type Set[E comparable] map[E]struct{}

func New[E comparable]() Set[E] {
	return make(map[E]struct{})
}

func (s Set[E]) Put(e E) {
	s[e] = struct{}{}
}

func (s Set[E]) Exist(e E) bool {
	_, ok := s[e]
	return ok
}

func (s Set[E]) Len() int {
	return len(s)
}

func (s Set[E]) Foreach(it func(e E) bool) {
	for e := range s {
		if !it(e) {
			break
		}
	}
}

func (s Set[E]) ToSlice() []E {
	es := make([]E, 0, s.Len())
	for e := range s {
		es = append(es, e)
	}
	return es
}
