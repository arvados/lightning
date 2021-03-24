package lightning

import (
	"sort"
)

type interval struct {
	start int
	end   int
}

type intervalTreeNode struct {
	interval interval
	maxend   int
}

type intervalTree []intervalTreeNode

type mask struct {
	intervals map[string][]interval
	itrees    map[string]intervalTree
	frozen    bool
}

func (m *mask) Add(seqname string, start, end int) {
	if m.intervals == nil {
		m.intervals = map[string][]interval{}
	}
	m.intervals[seqname] = append(m.intervals[seqname], interval{start, end})
}

func (m *mask) Freeze() {
	m.itrees = map[string]intervalTree{}
	for seqname, intervals := range m.intervals {
		m.itrees[seqname] = m.freeze(intervals)
	}
	m.frozen = true
}

func (m *mask) Check(seqname string, start, end int) bool {
	if !m.frozen {
		panic("bug: (*mask)Check() called before Freeze()")
	}
	return m.itrees[seqname].check(0, interval{start, end})
}

func (m *mask) freeze(in []interval) intervalTree {
	if len(in) == 0 {
		return nil
	}
	sort.Slice(in, func(i, j int) bool {
		return in[i].start < in[j].start
	})
	itreesize := 1
	for itreesize < len(in) {
		itreesize = itreesize * 2
	}
	itree := make(intervalTree, itreesize)
	itree.importSlice(0, in)
	for i := len(in); i < itreesize; i++ {
		itree[i].maxend = -1
	}
	return itree
}

func (itree intervalTree) check(root int, q interval) bool {
	return root < len(itree) &&
		itree[root].maxend >= q.start &&
		((itree[root].interval.start <= q.end && itree[root].interval.end >= q.start) ||
			itree.check(root*2+1, q) ||
			itree.check(root*2+2, q))
}

func (itree intervalTree) importSlice(root int, in []interval) int {
	mid := len(in) / 2
	node := intervalTreeNode{interval: in[mid], maxend: in[mid].end}
	if mid > 0 {
		end := itree.importSlice(root*2+1, in[0:mid])
		if end > node.maxend {
			node.maxend = end
		}
	}
	if mid+1 < len(in) {
		end := itree.importSlice(root*2+2, in[mid+1:])
		if end > node.maxend {
			node.maxend = end
		}
	}
	itree[root] = node
	return node.maxend
}
