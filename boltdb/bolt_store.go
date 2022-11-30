package storeboltdb

import (
	"bytes"
	"encoding/binary"

	eventtracker "github.com/0xPolygon/eth-event-tracker"
	"github.com/umbracle/ethgo"
	bolt "go.etcd.io/bbolt"
)

var _ eventtracker.Entry = (*Entry)(nil)

var (
	dbLogs       = []byte("logs")
	logsPrefix   = []byte("logs-")
	keyLastBlock = []byte("last-block")
)

// BoltStore is a tracker store implementation.
type BoltStore struct {
	conn *bolt.DB
}

// New creates a new boltdbstore
func New(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	store := &BoltStore{
		conn: db,
	}
	return store, nil
}

// GetEntry implements the store interface
func (b *BoltStore) GetEntry(hash string) (eventtracker.Entry, error) {
	txn, err := b.conn.Begin(true)
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	bucketName := append(dbLogs, []byte(hash)...)
	if _, err := txn.CreateBucketIfNotExists(bucketName); err != nil {
		return nil, err
	}
	if err := txn.Commit(); err != nil {
		return nil, err
	}
	e := &Entry{
		conn:   b.conn,
		bucket: bucketName,
	}
	return e, nil
}

// Entry is an store.Entry implementation
type Entry struct {
	conn   *bolt.DB
	bucket []byte
}

// NewEntry creates a new entry
func NewEntry(conn *bolt.DB, bucket []byte) *Entry {
	return &Entry{conn: conn, bucket: bucket}
}

// GetLastBlock implements the entry interface
func (e *Entry) GetLastBlock() (*ethgo.Block, error) {
	txn, err := e.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	bkt := txn.Bucket(e.bucket)
	data := bkt.Get(keyLastBlock)
	if data == nil {
		return nil, nil
	}

	var block ethgo.Block
	if err := block.UnmarshalJSON(data); err != nil {
		return nil, err
	}
	return &block, nil
}

// StoreEvent implements the entry interface
func (e *Entry) StoreEvent(evnt *eventtracker.Event) error {
	tx, err := e.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(e.bucket)

	// remove events
	if evnt.Indx > 0 {
		indxKey := genLogKey(uint64(evnt.Indx))

		curs := bucket.Cursor()
		for k, _ := curs.Seek(indxKey); k != nil && bytes.HasPrefix(k, logsPrefix); k, _ = curs.Next() {
			if err := curs.Delete(); err != nil {
				return err
			}
		}
	}

	// add events
	{
		indx, err := e.lastIndexImpl(bucket)
		if err != nil {
			return err
		}

		for logIndx, log := range evnt.Added {
			key := genLogKey(indx + uint64(logIndx))

			val, err := log.MarshalJSON()
			if err != nil {
				return err
			}
			if err := bucket.Put(key, val); err != nil {
				return err
			}
		}
	}

	// write last block
	if evnt.Block != nil {
		raw, err := evnt.Block.MarshalJSON()
		if err != nil {
			return err
		}
		if err := bucket.Put(keyLastBlock, raw); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetLog implements the store interface
func (e *Entry) GetLog(indx uint64, log *ethgo.Log) error {
	txn, err := e.conn.Begin(false)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	bucket := txn.Bucket(e.bucket)
	val := bucket.Get(genLogKey(indx))

	if err := log.UnmarshalJSON(val); err != nil {
		return err
	}
	return nil
}

// LastIndex implements the store interface
func (e *Entry) LastIndex() (uint64, error) {
	tx, err := e.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return e.lastIndexImpl(tx.Bucket(e.bucket))
}

func (e *Entry) lastIndexImpl(bkt *bolt.Bucket) (uint64, error) {
	curs := bkt.Cursor()
	curs.Seek(logsPrefix)

	if last, _ := curs.Last(); last != nil {
		last = bytes.TrimPrefix(last, logsPrefix)
		return bytesToUint64(last) + 1, nil
	}
	return 0, nil
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func genLogKey(i uint64) []byte {
	buf := []byte(logsPrefix)
	buf = append(buf, uint64ToBytes(i)...)
	return buf
}
