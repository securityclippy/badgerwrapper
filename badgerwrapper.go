//Wrapper for regularly used badgerDB functions
package badgerwrapper

import (
	"github.com/dgraph-io/badger"
	logger "github.com/sirupsen/logrus"
	"time"
	"encoding/binary"
	"github.com/pkg/errors"
)

var (
	errorKeyNotFound = errors.New("Key not found")
)


// Badger is a simple helper for db access
type Badger struct {
	db *badger.DB
}

// KVP simple named key value pair storage
type KVP struct {
	Key []byte
	Value []byte
}

// NewBadgerDB returns a new badgerdb
func NewBadgerDB() (Badger, error) {
	b := Badger{}
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		logger.Error(err)
		return b, err
	}
	b.db = db
	return b, nil
}

// NewBadgerDB returns a new badgerdb
func NewBadgerDBFromPath(path string) (Badger, error) {
	b := Badger{}
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		logger.Error(err)
		return b, err
	}
	b.db = db
	return b, nil
}


// Update update a set of key/values in the db
func (b Badger) Update(key, value []byte) (error) {
	err := b.db.Update(func(txn *badger.Txn) error {
		txn.Set(key, value)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}


func (b Badger) Delete(key []byte) (error) {
	err := b.db.Update(
		func(txn *badger.Txn) error {
			err := txn.Delete(key)
			if err != nil {
				return err
			}
			return nil
	})
	return err
}

func (b Badger) UpdateWithTTL(key, value []byte, ttl int) (error) {
	err := b.db.Update(func(txn *badger.Txn) error {
		txn.SetWithTTL(key, value, time.Duration(ttl) * time.Second)
		return nil
	})
	if err != nil {
		return err
	}
	return err
}

//Get return the value of a given key
func (b Badger) Get(key []byte) ([]byte, error) {
	KeyVal := []byte{}
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		for _, i := range val {
			KeyVal = append(KeyVal, i)
		}
		return nil
	})

	if err != nil {
		return KeyVal, err
	}
	return KeyVal, nil
}


// SearchPrefix returns all key value paris which match teh specified prefix
func (b Badger) SearchPrefix(prefix string) ([]KVP, error) {
	results := []KVP{}
	err := b.db.View(
		func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			pfx := []byte(prefix)
			for it.Seek(pfx);  it.ValidForPrefix(pfx);  it.Next() {
				item := it.Item()
				k := item.Key()
				v, err := item.Value()
				if err != nil {
					return err
				}
				kvp := KVP{
					Key: k,
					Value: v,
					}
				results = append(results, kvp)
			}
		return nil
		})
	if err != nil {
		return []KVP{}, nil
	}
	return results, nil
}

func (b Badger) IterView() ([]KVP, error) {
	results := []KVP{}
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.Value()
			if err != nil {
				return err
			}
			res := KVP{k,v}
			results = append(results, res)
		}
		return nil
	})
	if err != nil {
		return []KVP{}, err
	}
	return results, nil
}

func Uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Merge function to add two uint64 numbers
func add(existing, new []byte) []byte {
	return Uint64ToBytes(BytesToUint64(existing) + BytesToUint64(new))
}

func (b Badger) IncrementKeyValue(key []byte) (uint64, error) {
	logger.Infof("trying to increment %s", string(key))
	m := b.db.GetMergeOperator(key, add, 200 *time.Millisecond)
	m.Add(Uint64ToBytes(1))

	res, err := m.Get()
	if err != nil {
		switch err {
		case badger.ErrKeyNotFound:
			logger.Info("Hit error case")
			err = b.Update(key, Uint64ToBytes(1))
			if err != nil {
				logger.Error(err)
			}
		default:
			logger.Info("hit default")
			return uint64(0), err
		}
	}

	lookupVal, err := b.Get(key)

	if err != nil {
		logger.Error(err)
	}

	logger.Infof("lookup val:  %s", string(lookupVal))

	logger.Infof("res: %+v", BytesToUint64(res))

	return BytesToUint64(res), nil
}


