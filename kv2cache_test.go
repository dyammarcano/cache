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

	// Test NewCache
	s, err := NewCache(context.Background(), cfg)
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

func TestStore_GetKeys(t *testing.T) {
	for i := 0; i < 1000; i++ {
		data := fmt.Sprintf("###########################################################################:%d", i)

		err := ss.SetExpire("kv2cacheCliApp#postman", []byte(data), time.Second*15)
		assert.NoError(t, err)
	}

	// count keys
	count, err := ss.GetKeys("kv2cacheCliApp#postman")
	assert.NoError(t, err)

	assert.Equal(t, 1000, len(count))
}

func TestStore_CountPrefix(t *testing.T) {
	for i := 0; i < 100; i++ {
		data := fmt.Sprintf("############################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################:%d", i)

		err := ss.SetExpire("kv2cacheCliApp#postman", []byte(data), time.Minute*5)
		assert.NoError(t, err)
	}

	// count keys
	count, err := ss.CountPrefix("kv2cacheCliApp#postman")
	assert.NoError(t, err)

	t.Log(count)
}
