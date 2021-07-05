package libschema

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDependenciesOkay(t *testing.T) {
	cases := []struct {
		nodes []Node
		want  []int
		err   string
	}{
		{
			nodes: []Node{
				{
					Blocking: []int{2, 3, 4},
				},
				{
					Blocking: []int{0},
				},
				{},
				{
					Blocking: []int{4},
				},
				{
					Blocking: []int{2},
				},
				{},
			},
			want: []int{1, 0, 3, 4, 2, 5},
		},
	}
	for _, tc := range cases {
		got, err := DependencyOrder(tc.nodes, strconv.Itoa)
		if tc.err != "" {
			assert.Equal(t, fmt.Errorf(tc.err), err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		}
	}
}
