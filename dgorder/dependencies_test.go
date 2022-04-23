package dgorder

import (
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
		{
			nodes: []Node{
				{},
				{
					Blocking: []int{2},
				},
				{
					Blocking: []int{1},
				},
				{},
			},
			want: nil,
			err:  "Circular dependency found that includes 1 depending upon 2",
		},
		{
			nodes: []Node{
				{
					Blocking: []int{22},
				},
			},
			want: nil,
			err:  "Invalid dependency from 0 to 22",
		},
	}
	for _, tc := range cases {
		got, err := Order(tc.nodes, strconv.Itoa)
		if tc.err != "" {
			assert.Equal(t, tc.err, err.Error())
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		}
	}
}
