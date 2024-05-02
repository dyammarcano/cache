package kv2cache

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var ss *KV

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
	key := ss.MakeKey("testing", []byte("#########################################################"))
	assert.Equal(t, 24, len(key))
	t.Log(string(key))
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
	for i := 0; i < 1000; i++ {
		data := fmt.Sprintf("###########################################################################:%d", i)

		err := ss.SetExpire("kk", []byte(data), time.Second*3)
		assert.NoError(t, err)
	}

	// count keys
	count, err := ss.CountPrefix("kk")
	assert.NoError(t, err)

	assert.Equal(t, 1000, count)

	// wait for expiration
	<-time.After(time.Second * 6)

	// count keys
	count, err = ss.CountPrefix("kk")
	assert.NoError(t, err)

	assert.Equal(t, 0, count)
}
