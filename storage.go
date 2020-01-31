package bolt

import (
	"fmt"
	"net/url"

	"go.etcd.io/bbolt"
)

var (
	bucketRequests = []byte("requests")
	bucketCookies  = []byte("cookies")
	bucketQueue    = []byte("queue")
)

// Storage is a implementation for colly/queue and colly/storage
type Storage struct {
	db *bbolt.DB
}

func NewStorage(path string) (*Storage, error) {
	db, err := bbolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	return &Storage{db: db}, nil
}

func (s *Storage) Init() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		for _, b := range [][]byte{
			bucketRequests,
			bucketCookies,
			bucketQueue,
		} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Storage) Visited(id uint64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketRequests).Put(u64ToBytes(id), []byte{})
	})
}

func (s *Storage) IsVisited(id uint64) (bool, error) {
	var isVisited bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		isVisited = tx.Bucket(bucketRequests).Get(u64ToBytes(id)) != nil
		return nil
	})
	return isVisited, err
}

func (s *Storage) Cookies(u *url.URL) string {
	var cookies string
	s.db.View(func(tx *bbolt.Tx) error {
		cookies = string(tx.Bucket(bucketCookies).Get([]byte(u.String())))
		return nil
	})
	return cookies
}

func (s *Storage) SetCookies(u *url.URL, cookies string) {
	s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketCookies).Put([]byte(u.String()), []byte(cookies))
	})
}

func (s *Storage) AddRequest(request []byte) error {
	err := s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketQueue)
		n, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		return bucket.Put(u64ToBytes(n), request)
	})
	return err
}

func (s *Storage) GetRequest() ([]byte, error) {
	var request []byte
	err := s.db.Update(func(tx *bbolt.Tx) error {
		queueBucket := tx.Bucket(bucketQueue)
		if queueBucket.Stats().KeyN == 0 {
			return fmt.Errorf("the queue is empty")
		}
		c := queueBucket.Cursor()
		_, request = c.First()
		return c.Delete()
	})
	return request, err
}

func (s *Storage) QueueSize() (int, error) {
	var queueSize int
	err := s.db.View(func(tx *bbolt.Tx) error {
		queueSize = tx.Bucket(bucketQueue).Stats().KeyN
		return nil
	})
	return queueSize, err
}

func u64ToBytes(n uint64) []byte {
	return []byte{
		byte(0xff & n),
		byte(0xff & (n >> 8)),
		byte(0xff & (n >> 16)),
		byte(0xff & (n >> 24)),
		byte(0xff & (n >> 32)),
		byte(0xff & (n >> 40)),
		byte(0xff & (n >> 48)),
		byte(0xff & (n >> 56)),
	}
}
