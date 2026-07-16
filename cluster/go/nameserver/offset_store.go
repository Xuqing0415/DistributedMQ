package nameserver

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

const (
	offsetBucket = "consumer_offsets"
)

type OffsetStore struct {
	db *bolt.DB
}

func NewOffsetStore(dataDir string) (*OffsetStore, error) {
	dbPath := filepath.Join(dataDir, "offsets.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open offset db: %v", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(offsetBucket))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket: %v", err)
	}

	return &OffsetStore{db: db}, nil
}

func (os *OffsetStore) PutOffset(groupID, topic string, partition int, offset int64) error {
	key := fmt.Sprintf("%s/%s/%d", groupID, topic, partition)
	return os.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(offsetBucket))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		offsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBytes, uint64(offset))
		return bucket.Put([]byte(key), offsetBytes)
	})
}

func (os *OffsetStore) GetOffset(groupID, topic string, partition int) (int64, error) {
	key := fmt.Sprintf("%s/%s/%d", groupID, topic, partition)
	var offset int64

	err := os.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(offsetBucket))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		data := bucket.Get([]byte(key))
		if data == nil {
			offset = 0
			return nil
		}

		offset = int64(binary.BigEndian.Uint64(data))
		return nil
	})

	return offset, err
}

func (os *OffsetStore) DeleteOffset(groupID, topic string, partition int) error {
	key := fmt.Sprintf("%s/%s/%d", groupID, topic, partition)
	return os.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(offsetBucket))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		return bucket.Delete([]byte(key))
	})
}

func (os *OffsetStore) Close() error {
	return os.db.Close()
}