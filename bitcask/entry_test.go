package bitcask

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestO(t *testing.T) {

	var a string = "Hello"
	var b string = "Hello"

	assert.Equal(t, a, b, "The two words should be the same.")
}

func TestEntryDecode(t *testing.T) {
	key := "xiao"
	value := "carl"

	entry := NewEntryWithData([]byte(key), []byte(value))

	byt := entry.Encode()
	fmt.Println(byt)
	fmt.Println(entry.GetCrc(byt[0:4]))
	var a string = "Hello"
	var b string = "Hello"

	assert.Equal(t, a, b, "The two words should be the same.")
}
