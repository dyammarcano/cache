package main

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var ss *Store

func TestMain(m *testing.M) {
	cfg := &Config{
		StorePath: filepath.Join(os.TempDir(), "badger", "testing"),
	}

	if err := cfg.Validate(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Test NewStore
	s, err := NewStore(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ss = s

	// Run tests
	exitCode := m.Run()

	// Exit with the status code from the tests
	os.Exit(exitCode)
}

func TestStore_MakeKey(t *testing.T) {
	key := ss.MakeKey("testing")
	assert.Equal(t, 28, len(key))
}

func TestStore_SetGet(t *testing.T) {
	err := ss.SetExpire("key", []byte("value"), time.Minute*5)
	assert.NoError(t, err)

	value, err := ss.GetOnce("key")
	assert.NoError(t, err)

	assert.Equal(t, "value", string(value))

	// try to get value
	value, err = ss.Get("key")
	assert.NoError(t, err)

	assert.Equal(t, "", string(value))
}

func TestStore_BulkInsert(t *testing.T) {
	data := make([]string, 1000)

	for i := 0; i < len(data); i++ {
		data[i] = "###########################################################################"
	}

	for _, v := range data {
		err := ss.SetExpire("k", []byte(v), time.Second*5)
		assert.NoError(t, err)
	}

	// count keys
	count, err := ss.CountPrefix("k")
	assert.NoError(t, err)

	assert.Equal(t, len(data), count)

	// wait for expiration
	<-time.After(time.Second * 6)

	// count keys
	count, err = ss.CountPrefix("k")
	assert.NoError(t, err)

	assert.Equal(t, 0, count)
}
