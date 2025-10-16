package lstesting_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/muir/libschema/lstesting"
)

func TestRandomString(t *testing.T) {
	a := lstesting.RandomString(50)
	b := lstesting.RandomString(50)
	assert.NotEqual(t, a, b, "random strings")
	assert.Equal(t, 50, len(a), "length")
	assert.Equal(t, 50, len(b), "length")
}

// TestFakeSchemaCleanupNil ensures cleanup is safe when db is nil
func TestFakeSchemaCleanupNil(t *testing.T) {
	opts, cleanup := lstesting.FakeSchema(t, "CASCADE")
	assert.NotEmpty(t, opts.SchemaOverride)
	cleanup(nil) // should be no panic
}
