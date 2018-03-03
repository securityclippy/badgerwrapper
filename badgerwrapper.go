//Wrapper for regularly used badgerDB functions
package badgerwrapper

import (
"github.com/dgraph-io/badger"
"log"
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
		log.Println(err)
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
