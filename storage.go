package bolt

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

var (
	bucketRequests = []byte("requests")
	bucketCookies  = []byte("cookies")
	bucketQueue    = []byte("queue")

	// ErrEmptyQueue is returned when an URL is requested from an empty queue.
	ErrEmptyQueue = fmt.Errorf("queue is empty")
)

// Storage is a implementation for colly/queue and colly/storage
type Storage struct {
	db      *bbolt.DB
	mode    os.FileMode
	options *bbolt.Options
	debug   Logger
}

// Logger is the interface used for debug logging.
type Logger func(...interface{})

// Option enables configuration of the storage.
type Option func(*Storage) error

// Timeout sets the underlying BoltDB timeout.
func Timeout(t time.Duration) Option {
	return func(s *Storage) error {
		s.options.Timeout = t
		return nil
	}
}

// Mode determines the file creation mode. It defaults to 0666.
func Mode(m os.FileMode) Option {
	return func(s *Storage) error {
		s.mode = m
		return nil
	}
}

// Debug sets a Logger for the storage.
func Debug(l Logger) Option {
	return func(s *Storage) error {
		s.debug = l
		return nil
	}
}

// New creates a new storage implementation for Colly.
// A database will be created at the provided path if it does not already exist.
func New(path string, opts ...Option) (*Storage, error) {
	out := &Storage{
		options: bbolt.DefaultOptions,
		mode:    0666,
		debug:   func(v ...interface{}) {},
	}
	for _, o := range opts {
		if err := o(out); err != nil {
			return nil, err
		}
	}
	var err error
	out.debug("bolt: using file", path, "mode", out.mode)
	out.db, err = bbolt.Open(path, out.mode, out.options)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Close ensures the database is left in a valid state.
func (s *Storage) Close() error {
	return s.db.Close()
}

// Init implements the colly.Storage interface.
func (s *Storage) Init() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		s.debug("bolt: creating buckets")
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

// Visited implements the colly.Storage interface.
func (s *Storage) Visited(id uint64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketRequests).Put(u64ToBytes(id), []byte{})
	})
}

// IsVisited implements the colly.Storage interface.
func (s *Storage) IsVisited(id uint64) (bool, error) {
	var isVisited bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		isVisited = tx.Bucket(bucketRequests).Get(u64ToBytes(id)) != nil
		return nil
	})
	return isVisited, err
}

// Cookies implements the colly.Storage interface.
func (s *Storage) Cookies(u *url.URL) string {
	var cookies string
	s.db.View(func(tx *bbolt.Tx) error {
		cookies = string(tx.Bucket(bucketCookies).Get([]byte(u.String())))
		return nil
	})
	return cookies
}

// SetCookies implements the colly.Storage interface.
func (s *Storage) SetCookies(u *url.URL, cookies string) {
	s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketCookies).Put([]byte(u.String()), []byte(cookies))
	})
}

// AddRequest implements the colly.Storage interface.
func (s *Storage) AddRequest(request []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketQueue)
		n, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		return bucket.Put(u64ToBytes(n), request)
	})
}

// GetRequest implements the colly.Storage interface.
func (s *Storage) GetRequest() ([]byte, error) {
	var request []byte
	err := s.db.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketQueue).Cursor()
		_, request = c.First()
		if request == nil {
			return ErrEmptyQueue
		}
		return c.Delete()
	})
	return request, err
}

// QueueSize implements the colly.Queue interface.
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
