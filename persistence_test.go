//go:build windows

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

	// Perform teardown tasks here

	// Exit with the status code from the tests
	os.Exit(exitCode)
}

//func TestStore_Stress(t *testing.T) {
//	total := 1000000
//	startTime := time.Now()
//	randonBytes := generateNBytes(1024)
//
//	var wg sync.WaitGroup
//
//	for i := 0; i < total; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//
//			newDeposedKey := key.NewDeposedKey().
//				SetSource(key.AutoPrefix).
//				SetDestination(key.DataPrefix).
//				SetDirection(key.PersistentPrefix).
//				SetPersistence(key.TemporaryPrefix)
//
//			keyBytes, err := newDeposedKey.GenerateKey()
//			if err != nil {
//				log.Error(err.Error())
//				return
//			}
//
//			err = ss.Set(keyBytes, randonBytes)
//			assert.NoError(t, err)
//
//			// Test Get
//			gotValue, err := ss.Get(keyBytes)
//			assert.NoError(t, err)
//
//			if !bytes.Equal(gotValue, randonBytes) {
//				t.Errorf("Get value mismatch, want: %s, got: %s", randonBytes, gotValue)
//			}
//		}(i)
//	}
//
//	wg.Wait()
//	fmt.Printf("Tempo total: %s, registros totais: %s, registros inseridos por segundo: %s\n",
//		time.Since(startTime),
//		humanize.Comma(int64(total)),
//		humanize.CommafWithDigits(float64(total)/time.Since(startTime).Seconds(), 2),
//	)
//}
//
//func generateNBytes(amount int) []byte {
//	b := make([]byte, amount)
//	for i := range b {
//		b[i] = 'a'
//	}
//	return b
//}
//
//func TestStore_SetBatch(t *testing.T) {
//	total := 1000000
//	startTime := time.Now()
//	randonBytes := generateNBytes(1024)
//
//	var wg sync.WaitGroup
//
//	for i := 0; i < total; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//
//			newDeposedKey := key.NewDeposedKey().
//				SetSource(key.AutoPrefix).
//				SetDestination(key.DataPrefix).
//				SetDirection(key.PersistentPrefix).
//				SetPersistence(key.TemporaryPrefix)
//
//			keyBytes, err := newDeposedKey.GenerateKey()
//			if err != nil {
//				log.Error(err.Error())
//				return
//			}
//
//			ss.AddNewEntryToBatch(keyBytes, randonBytes, 1*time.Hour)
//		}(i)
//	}
//
//	wg.Wait()
//
//	fmt.Printf("Tempo total: %s, registros totais: %s, registros inseridos por segundo: %s\n",
//		time.Since(startTime),
//		humanize.Comma(int64(total)),
//		humanize.CommafWithDigits(float64(total)/time.Since(startTime).Seconds(), 2),
//	)
//}
//
//func TestStore_Stream(t *testing.T) {
//	newDeposedKey := key.NewDeposedKey().
//		SetSource(key.AutoPrefix).
//		SetDestination(key.DataPrefix).
//		SetDirection(key.PersistentPrefix).
//		SetPersistence(key.TemporaryPrefix)
//
//	dataCh, err := ss.GetStream(newDeposedKey.GetPrefix())
//	assert.NoError(t, err)
//
//	items := 0
//	for range dataCh {
//		items++
//		//fmt.Printf("%s\n", item)
//		//<-time.After(1 * time.Second)
//	}
//
//	fmt.Printf("items: %s\n", humanize.CommafWithDigits(float64(items), 2))
//}
//
//func TestStore_GetKeysPages(t *testing.T) {
//	newDeposedKey := key.NewDeposedKey().
//		SetSource(key.AutoPrefix).
//		SetDestination(key.DataPrefix).
//		SetDirection(key.PersistentPrefix).
//		SetPersistence(key.TemporaryPrefix)
//
//	keys, err := ss.GetKeysPages(newDeposedKey.GetPrefix(), 100)
//	assert.NoError(t, err)
//
//	fmt.Printf("keys: %s\n", humanize.CommafWithDigits(float64(len(keys)), 2))
//
//	data, err := json.Marshal(keys)
//	assert.NoError(t, err)
//
//	if err = os.WriteFile("C:/arqprod_local/testing/keys.json", data, 0644); err != nil {
//		fmt.Println(err)
//	}
//}

func TestStore_SetGet(t *testing.T) {
	err := ss.SetExpire([]byte("key"), []byte("value"), time.Minute)
	assert.NoError(t, err)

	value, err := ss.GetOnce([]byte("key"))
	assert.NoError(t, err)

	assert.Equal(t, "value", string(value))

	// try to get value
	value, err = ss.GetOnce([]byte("key"))
	assert.Error(t, err)

	assert.Equal(t, "", string(value))
}

//func TestGetPrefixStream(t *testing.T) {
//	go func() {
//		lap := 0
//		for {
//			msg := fmt.Sprintf("hello world,\t%d", lap)
//			<-time.After(1 * time.Second)
//			log.Infof(msg)
//			err1 := ss.SetPrefixExpire("testing:", UidKey(), []byte(msg), 1*time.Minute)
//			assert.NoError(t, err1)
//			lap++
//		}
//	}()
//
//	stream, err := ss.GetPrefixStream(ctx, "testing:")
//	assert.NoError(t, err)
//
//	for {
//		select {
//		case msg := <-stream:
//			fmt.Printf("%v\n", msg)
//		}
//	}
//}
//
//func TestCountPrefix(t *testing.T) {
//	//values, err := ss.CountPrefix("kafka;metrics")
//	//assert.NoError(t, err)
//	//fmt.Println(values)
//	//
//	values, err := ss.CountPrefix("kafka;events")
//	assert.NoError(t, err)
//	fmt.Println(values)
//
//	key, err := ss.FindLatestKey("kafka;events")
//	assert.NoError(t, err)
//
//	data, err := ss.GetPrefix("kafka;events", key)
//	assert.NoError(t, err)
//
//	fmt.Println(data)
//}
