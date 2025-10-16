package dgorder

import (
	"container/heap"

	"github.com/memsql/errors"
)

type Node struct {
	Blocking    []int // values are the indexes of nodes in the list of nodes
	isBlockedBy map[int]struct{}
}

// Order returns a list of the indexes of the nodes
// in the order in which they can be acted upon so that no blocked
// node comes before a node that blocks it.  Additionally, to
// the extent possible nodes will be returned in-order.
// If this is not possible, an error will be returned and the
// describe function will be used to help generate the error so that
// it is human-readable.
func Order(nodes []Node, describe func(int) string) ([]int, error) {
	for j := range nodes {
		nodes[j].isBlockedBy = nil
	}
	for j, node := range nodes {
		for _, i := range node.Blocking {
			if i < 0 || i > len(nodes) {
				return nil, errors.Errorf("Invalid dependency from %d to %d", j, i)
			}
			if nodes[i].isBlockedBy == nil {
				nodes[i].isBlockedBy = make(map[int]struct{})
			}
			nodes[i].isBlockedBy[j] = struct{}{}
		}
	}
	unblocked := make(IntHeap, 0, len(nodes))
	for j, node := range nodes {
		if node.isBlockedBy == nil {
			unblocked = append(unblocked, j)
		}
	}
	heap.Init(&unblocked)
	order := make([]int, 0, len(nodes))
	for len(unblocked) > 0 {
		i := heap.Pop(&unblocked).(int)
		order = append(order, i)
		for _, j := range nodes[i].Blocking {
			delete(nodes[j].isBlockedBy, i)
			if len(nodes[j].isBlockedBy) == 0 {
				heap.Push(&unblocked, j)
			}
		}
	}
	if len(order) == len(nodes) {
		return order, nil
	}
	for j, node := range nodes {
		for b := range node.isBlockedBy {
			return nil, errors.Errorf("Circular dependency found that includes %s depending upon %s",
				describe(j), describe(b))
		}
	}
	return nil, errors.Errorf("Internal error with dependency checking")
}

// The following is lifted directly from the examples provided with container/heap
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
