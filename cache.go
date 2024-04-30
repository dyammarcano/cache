package cache

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
	Cache struct {
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

func NewStore(ctx context.Context, cfg *Config) (*Cache, error) {
	if !cfg.validated {
		return nil, fmt.Errorf("config not validated")
	}

	db, err := badger.Open(cfg.SetDefaultOptions())
	if err != nil {
		return nil, err
	}

	store := &Cache{
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
				if err = store.db.Close(); err != nil {
					zlog.Error().Msgf("Failed to close ss: %v", err)
				}
				return
			case <-ticker.C:
				if len(store.entries) > 0 {
					if err = store.batchWriteAction(); err != nil {
						zlog.Error().Msgf("failed to batch write: %v", err)
					}
				}
			}
		}
	}()

	return store, nil
}

func (c *Cache) batchWriteAction() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) > 0 {
		nb := c.db.NewWriteBatch()
		defer nb.Cancel()

		for _, b := range c.entries {
			if err := nb.SetEntry(b); err != nil {
				return err
			}
		}

		if err := nb.Flush(); err != nil {
			return err
		}

		c.entries = []*badger.Entry{}
	}

	return nil
}

func (c *Cache) batchDeleteAction(batch []badger.Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nb := c.db.NewWriteBatch()
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

func (c *Cache) MakeKey(prefix string, data []byte) []byte {
	c.hasher.Reset()
	if _, err := c.hasher.Write(data); err != nil {
		return nil
	}
	return []byte(fmt.Sprintf("%s:%x", prefix, c.hasher.Sum(nil)))
}

func (c *Cache) addEntryToBatch(batch *badger.Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = append(c.entries, batch)
}

func (c *Cache) AddToBatch(batch *badger.Entry) {
	c.addEntryToBatch(batch)
}

func (c *Cache) AddNewEntryToBatch(key, value []byte, expiresAt time.Duration) {
	c.addEntryToBatch(&badger.Entry{Key: key, Value: value, ExpiresAt: uint64(expiresAt)})
}

func (c *Cache) Set(prefix string, value []byte) error {
	return c.db.Update(func(txn *badger.Txn) error {
		return txn.Set(c.MakeKey(prefix, value), value)
	})
}

func (c *Cache) SetExpire(prefix string, value []byte, expire time.Duration) error {
	var entry = badger.NewEntry(c.MakeKey(prefix, value), value)
	if expire > time.Second {
		entry = entry.WithTTL(expire)
	}
	return c.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(entry)
	})
}

//func (c *Cache) HasPrefix(prefix string, key []byte) (bool, error) {
//	var has bool
//	err := c.db.View(func(txn *badger.Txn) error {
//		it := txn.NewIterator(badger.DefaultIteratorOptions)
//		defer it.Close()
//		key = c.MakeKey(prefix,key)
//		for it.Seek(key); it.ValidForPrefix(key); it.Next() {
//			has = true
//		}
//		return nil
//	})
//	return has, err
//}

func (c *Cache) Get(prefix string) ([]byte, error) {
	value := make([]byte, 0)
	err := c.db.View(func(txn *badger.Txn) error {
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

func (c *Cache) GetOnce(prefix string) ([]byte, error) {
	value := make([]byte, 0)
	err := c.db.Update(func(txn *badger.Txn) error {
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

func (c *Cache) GetKeysPages(prefix []byte, pageSize int) ([]KeysPage, error) {
	var (
		keys     []string
		keysPage []KeysPage
		page     int
	)

	err := c.db.View(func(txn *badger.Txn) error {
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

func (c *Cache) GetLimit(prefix string, limit int) ([][]byte, error) {
	values := make([][]byte, 0)
	err := c.db.View(func(txn *badger.Txn) error {
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

func (c *Cache) GetStream(prefix string) (chan *badger.Item, error) {
	forward := make(chan *badger.Item)
	go func() {
		defer close(forward)
		err := c.db.View(func(txn *badger.Txn) error {
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

func (c *Cache) Exists(key string) (bool, error) {
	var exists bool
	err := c.db.View(func(tx *badger.Txn) error {
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

func (c *Cache) ValuesPrefix(prefix string) ([][]byte, error) {
	values := make([][]byte, 0)
	err := c.db.View(func(txn *badger.Txn) error {
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

func (c *Cache) ValuesPrefixLimit(prefix string, limit int) ([][]byte, error) {
	values := make([][]byte, 0)
	if len(prefix) == 0 {
		return values, errors.New("prefix should not be empty")
	}
	err := c.db.View(func(txn *badger.Txn) error {
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

func (c *Cache) FindLatestKey(prefix string) ([]byte, error) {
	var latestKey []byte
	err := c.db.View(func(txn *badger.Txn) error {
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

func (c *Cache) Items() ([]*badger.Item, error) {
	items := make([]*badger.Item, 0)
	err := c.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			items = append(items, it.Item())
		}
		return nil
	})
	return items, err
}

func (c *Cache) CountPrefix(prefix string) (int, error) {
	var count int
	err := c.db.View(func(txn *badger.Txn) error {
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

func (c *Cache) Delete(key []byte) error {
	return c.db.Update(func(txn *badger.Txn) error {
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

func (c *Cache) DeleteKeys(keys [][]byte) error {
	return c.db.Update(func(txn *badger.Txn) error {
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

func (c *Cache) DeleteAll() error {
	return c.db.Update(func(txn *badger.Txn) error {
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

func (c *Cache) DropPrefix(prefix string) error {
	return c.Delete([]byte(prefix))
}
