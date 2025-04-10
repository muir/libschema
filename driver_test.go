package libschema_test

import (
	"testing"

	"github.com/muir/libschema"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mostly this just needs to compile
func TestOpenAnyDB(t *testing.T) {
	_, err := libschema.OpenAnyDB("http://foo/bar")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not find database driver matching http")
	_, err = libschema.OpenAnyDB("postgres")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not find appropriate database driver for DSN")
}
