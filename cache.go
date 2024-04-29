package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/rs/xid"
	"sync"
	"time"
)

type (
	Store struct {
		db      *badger.DB
		mu      sync.RWMutex
		entries []*badger.Entry
		ctx     context.Context
	}

	KeysPage struct {
		Page int      `json:"page"`
		Keys []string `json:"keys"`
	}
)

func NewStore(ctx context.Context, cfg *Config) (*Store, error) {
	if !cfg.validated {
		return nil, fmt.Errorf("config not validated")
	}

	db, err := badger.Open(cfg.SetDefaultOptions())
	if err != nil {
		return nil, err
	}

	store := &Store{
		db:      db,
		mu:      sync.RWMutex{},
		entries: []*badger.Entry{},
		ctx:     ctx,
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

func (s *Store) batchWriteAction() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.entries) > 0 {
		nb := s.db.NewWriteBatch()
		defer nb.Cancel()

		for _, b := range s.entries {
			if err := nb.SetEntry(b); err != nil {
				return err
			}
		}

		if err := nb.Flush(); err != nil {
			return err
		}

		s.entries = []*badger.Entry{}
	}

	return nil
}

func (s *Store) batchDeleteAction(batch []badger.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nb := s.db.NewWriteBatch()
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

func (s *Store) MakeKey(prefix string) []byte {
	return []byte(fmt.Sprintf("%s:%s", prefix, xid.New().String()))
}

func (s *Store) addEntryToBatch(batch *badger.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = append(s.entries, batch)
}

func (s *Store) AddToBatch(batch *badger.Entry) {
	s.addEntryToBatch(batch)
}

func (s *Store) AddNewEntryToBatch(key, value []byte, expiresAt time.Duration) {
	s.addEntryToBatch(&badger.Entry{Key: key, Value: value, ExpiresAt: uint64(expiresAt)})
}

func (s *Store) Set(prefix string, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.MakeKey(prefix), value)
	})
}

func (s *Store) SetExpire(prefix string, value []byte, expire time.Duration) error {
	var entry = badger.NewEntry(s.MakeKey(prefix), value)
	if expire > time.Second {
		entry = entry.WithTTL(expire)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(entry)
	})
}

func (s *Store) HasPrefix(prefix string, key []byte) (bool, error) {
	var has bool
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		key = []byte(fmt.Sprintf("%s:%s", prefix, key))
		for it.Seek(key); it.ValidForPrefix(key); it.Next() {
			has = true
		}
		return nil
	})
	return has, err
}

func (s *Store) HasAllPrefix(prefix string, keys [][]byte) (bool, error) {
	var has bool
	err := s.db.View(func(txn *badger.Txn) error {
		for _, k := range keys {
			if _, err := txn.Get([]byte(fmt.Sprintf("%s:%s", prefix, k))); err != nil {
				return err
			}
		}
		has = true
		return nil
	})
	return has, err
}

func (s *Store) HasAnyPrefix(prefix string, keys [][]byte) (bool, error) {
	var has bool
	err := s.db.View(func(txn *badger.Txn) error {
		for _, k := range keys {
			if _, err := txn.Get([]byte(fmt.Sprintf("%s:%s", prefix, k))); err == nil {
				has = true
				break
			}
		}
		return nil
	})
	return has, err
}

func (s *Store) Get(prefix string) ([]byte, error) {
	value := make([]byte, 0)
	err := s.db.View(func(txn *badger.Txn) error {
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

func (s *Store) GetOnce(prefix string) ([]byte, error) {
	value := make([]byte, 0)
	err := s.db.Update(func(txn *badger.Txn) error {
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

func (s *Store) GetKeysPages(prefix []byte, pageSize int) ([]KeysPage, error) {
	var (
		keys     []string
		keysPage []KeysPage
		page     int
	)

	err := s.db.View(func(txn *badger.Txn) error {
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

func (s *Store) GetLimit(prefix []byte, limit int) ([][]byte, error) {
	var values [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		values = [][]byte{} // to avoid nil
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(values) < limit; it.Next() {
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

func (s *Store) GetStream(prefix []byte) (chan *badger.Item, error) {
	forward := make(chan *badger.Item, 1000)
	go func() {
		defer close(forward)
		err := s.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				forward <- it.Item()
			}
			return nil
		})
		if err != nil {
			zlog.Error().Msgf("error: %s", err)
		}
	}()
	return forward, nil
}

func (s *Store) Exists(key string) (bool, error) {
	var exists bool
	err := s.db.View(func(tx *badger.Txn) error {
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

func (s *Store) ValuesPrefix(prefix []byte) ([][]byte, error) {
	var values [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
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

func (s *Store) ValuesPrefixLimit(prefix []byte, limit int) ([][]byte, error) {
	var values [][]byte
	if len(prefix) == 0 {
		return values, errors.New("prefix should not be empty")
	}
	err := s.db.View(func(txn *badger.Txn) error {
		values = [][]byte{} // to avoid nil
		opt := badger.DefaultIteratorOptions
		opt.Reverse = false
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
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

func (s *Store) FindLatestKey(prefix []byte) ([]byte, error) {
	var latestKey []byte
	err := s.db.View(func(txn *badger.Txn) error {
		latestKey = []byte{} // to avoid nil
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(prefix)
		for ; it.Valid(); it.Next() {
			k := it.Item().Key()
			if !bytes.HasPrefix(k, prefix) {
				// Stop when the prefix doesn't match
				break
			}
			latestKey = k[len(prefix)+1:]
		}
		return nil
	})
	return latestKey, err
}

func (s *Store) Items() ([]*badger.Item, error) {
	items := make([]*badger.Item, 0)
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			items = append(items, it.Item())
		}
		return nil
	})
	return items, err
}

func (s *Store) CountPrefix(prefix string) (int, error) {
	var count int
	err := s.db.View(func(txn *badger.Txn) error {
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

func (s *Store) Delete(key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
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

func (s *Store) DeleteKeys(keys [][]byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
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

func (s *Store) DeleteAll() error {
	return s.db.Update(func(txn *badger.Txn) error {
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

func (s *Store) DropPrefix(prefix string) error {
	return s.Delete([]byte(prefix))
}
