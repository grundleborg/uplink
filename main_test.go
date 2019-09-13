package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewInstanceId(t *testing.T) {
	for i := 1; i <= 100; i++ {
		id := NewInstanceId()
		assert.Len(t, id, 8)
		assert.False(t, strings.Contains(id, "="))
		assert.False(t, strings.Contains(id, "/"))
	}
}
