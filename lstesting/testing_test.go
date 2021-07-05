package lstesting_test

import (
	"testing"

	"github.com/muir/libschema/lstesting"
	"github.com/stretchr/testify/assert"
)

func TestRandomString(t *testing.T) {
	a := lstesting.RandomString(50)
	b := lstesting.RandomString(50)
	assert.NotEqual(t, a, b, "random strings")
	assert.Equal(t, 50, len(a), "length")
	assert.Equal(t, 50, len(b), "length")
}
