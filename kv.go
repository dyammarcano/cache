package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"hash"
	"hash/fnv"
	"sync"
	"time"
)

type (
	KV struct {
		db      *badger.DB
		mu      sync.RWMutex
		entries []*badger.Entry
		ctx     context.Context
		hasher  hash.Hash64
	}

	KeysPage struct {
		Page int      `json:"page"`
		Keys []string `json:"keys"`
	}
)

func NewKV(ctx context.Context, cfg *Config) (*KV, error) {
	if !cfg.validated {
		return nil, fmt.Errorf("config not validated")
	}

	db, err := badger.Open(cfg.SetDefaultOptions())
	if err != nil {
		return nil, err
	}

	kv := &KV{
		db:      db,
		mu:      sync.RWMutex{},
		entries: []*badger.Entry{},
		ctx:     ctx,
		hasher:  fnv.New64a(),
	}

	ticker := time.NewTicker(time.Microsecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				if err = kv.db.Close(); err != nil {
					zlog.Error().Msgf("Failed to close ss: %v", err)
				}
				return
			case <-ticker.C:
				if len(kv.entries) > 0 {
					if err = kv.batchWriteAction(); err != nil {
						zlog.Error().Msgf("failed to batch write: %v", err)
					}
				}
			}
		}
	}()

	return kv, nil
}

func (k *KV) Close() error {
	return k.db.Close()
}

func (k *KV) batchWriteAction() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if len(k.entries) > 0 {
		nb := k.db.NewWriteBatch()
		defer nb.Cancel()

		for _, b := range k.entries {
			if err := nb.SetEntry(b); err != nil {
				return err
			}
		}

		if err := nb.Flush(); err != nil {
			return err
		}

		k.entries = []*badger.Entry{}
	}

	return nil
}

func (k *KV) batchDeleteAction(batch []badger.Entry) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	nb := k.db.NewWriteBatch()
	defer nb.Cancel()

	for _, b := range batch {
		if err := nb.Delete(b.Key); err != nil {
			return err
		}
	}

	if err := nb.Flush(); err != nil {
		return err
	}

	return nil
}

func (k *KV) addEntryToBatch(batch *badger.Entry) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.entries = append(k.entries, batch)
}

func (k *KV) MakeKeyPrefix(prefix string, v []byte) []byte {
	k.hasher.Reset()
	if _, err := k.hasher.Write(v); err != nil {
		return nil
	}
	return []byte(fmt.Sprintf("%s:%x", prefix, k.hasher.Sum(nil)))
}

func (k *KV) MakeKeyStr(prefix, v string) string {
	k.hasher.Reset()
	if _, err := k.hasher.Write([]byte(prefix)); err != nil {
		return ""
	}
	if _, err := k.hasher.Write([]byte(v)); err != nil {
		return ""
	}

	return fmt.Sprintf("%x", k.hasher.Sum(nil))
}

func (k *KV) AddToBatch(batch *badger.Entry) {
	k.addEntryToBatch(batch)
}

func (k *KV) AddNewEntryToBatch(key, value []byte, expiresAt time.Duration) {
	k.addEntryToBatch(&badger.Entry{Key: key, Value: value, ExpiresAt: uint64(expiresAt)})
}

func (k *KV) Set(prefix string, value []byte) error {
	return k.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k.MakeKeyPrefix(prefix, value), value)
	})
}

func (k *KV) SetExpire(prefix string, value []byte, expire time.Duration) error {
	var entry = badger.NewEntry(k.MakeKeyPrefix(prefix, value), value)
	if expire > time.Second {
		entry = entry.WithTTL(expire)
	}
	return k.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(entry)
	})
}

func (k *KV) Get(prefix string) ([]byte, error) {
	value := make([]byte, 0)
	err := k.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			value = val
			return nil
		}
		return nil
	})
	return value, err
}

func (k *KV) GetOnce(prefix string) ([]byte, error) {
	value := make([]byte, 0)
	err := k.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			value = val
			if err = txn.Delete(it.Item().Key()); err != nil {
				return err
			}
			return nil
		}
		return nil
	})
	return value, err
}

func (k *KV) GetKeysPages(prefix []byte, pageSize int) ([]KeysPage, error) {
	var (
		keys     []string
		keysPage []KeysPage
		page     int
	)

	err := k.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, string(it.Item().Key()))
			if pageSize > 0 && len(keys)%pageSize == 0 {
				page++
				keysPage = append(keysPage, KeysPage{Page: page, Keys: keys})
				keys = []string{}
			}
		}
		if len(keys) > 0 {
			page++
			keysPage = append(keysPage, KeysPage{Page: page, Keys: keys})
			keys = []string{}
		}
		return nil
	})
	return keysPage, err
}

func (k *KV) GetLimit(prefix string, limit int) ([][]byte, error) {
	values := make([][]byte, 0)
	err := k.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)) && len(values) < limit; it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			values = append(values, value)
		}
		return nil
	})
	return values, err
}

func (k *KV) GetStream(prefix string) (chan *badger.Item, error) {
	forward := make(chan *badger.Item)
	go func() {
		defer close(forward)
		err := k.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
				forward <- it.Item()
			}
			return nil
		})
		if err != nil {
			zlog.Error().Msgf("error: %v", err)
		}
	}()
	return forward, nil
}

func (k *KV) Exists(key string) (bool, error) {
	var exists bool
	err := k.db.View(func(tx *badger.Txn) error {
		if val, err := tx.Get([]byte(key)); err != nil {
			return err
		} else if val != nil {
			exists = true
		}
		return nil
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = nil
	}
	return exists, err
}

func (k *KV) ValuesPrefix(prefix string) ([][]byte, error) {
	values := make([][]byte, 0)
	err := k.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			values = append(values, value)
		}
		return nil
	})
	return values, err
}

func (k *KV) ValuesPrefixLimit(prefix string, limit int) ([][]byte, error) {
	values := make([][]byte, 0)
	if len(prefix) == 0 {
		return values, errors.New("prefix should not be empty")
	}
	err := k.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.Reverse = false
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			values = append(values, value)
		}
		if len(values) > limit {
			// get the last range of values
			values = values[len(values)-limit:]
		}
		return nil
	})
	return values, err
}

func (k *KV) FindLatestKey(prefix string) ([]byte, error) {
	var latestKey []byte
	err := k.db.View(func(txn *badger.Txn) error {
		latestKey = []byte{} // to avoid nil
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.Valid(); it.Next() {
			if !bytes.HasPrefix(it.Item().Key(), []byte(prefix)) {
				// Stop when the prefix doesn't match
				break
			}
			latestKey = it.Item().Key()[len(prefix)+1:]
		}
		return nil
	})
	return latestKey, err
}

func (k *KV) Items() ([]*badger.Item, error) {
	items := make([]*badger.Item, 0)
	err := k.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			items = append(items, it.Item())
		}
		return nil
	})
	return items, err
}

func (k *KV) CountPrefix(prefix string) (int, error) {
	var count int
	err := k.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			if item.ExpiresAt() != 0 {
				count++
			}
		}
		return nil
	})
	return count, err
}

func (k *KV) Delete(key []byte) error {
	return k.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(key); it.ValidForPrefix(key); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (k *KV) DeleteKeys(keys [][]byte) error {
	return k.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for _, k := range keys {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (k *KV) DeleteAll() error {
	return k.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (k *KV) DropPrefix(prefix string) error {
	return k.Delete([]byte(prefix))
}

func (k *KV) Keys(prefix string) ([]string, error) {
	var keys []string

	err := k.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			keys = append(keys, string(it.Item().Key()))
		}
		return nil
	})
	return keys, err
}

func (k *KV) SetWithTTL(prefix string, value []byte, ttl time.Duration) error {
	return k.SetExpire(prefix, value, ttl)
}

//func (c *KV) HasPrefix(prefix string, key []byte) (bool, error) {
//	var has bool
//	err := c.db.View(func(txn *badger.Txn) error {
//		it := txn.NewIterator(badger.DefaultIteratorOptions)
//		defer it.Close()
//		key = c.MakeKeyPrefix(prefix,key)
//		for it.Seek(key); it.ValidForPrefix(key); it.Next() {
//			has = true
//		}
//		return nil
//	})
//	return has, err
//}
