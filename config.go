package kv2cache

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	GCInterval   time.Duration
	VlogsPath    string
	RegistryPath string
	StorePath    string
	// Usually modified options.

	SyncWrites        bool
	NumVersionsToKeep int
	ReadOnly          bool
	Logger            Logger
	Compression       options.CompressionType
	InMemory          bool
	MetricsEnabled    bool
	// Sets the Stream.numGo field
	NumGoroutines int

	// Fine tuning options.

	MemTableSize        int64
	BaseTableSize       int64
	BaseLevelSize       int64
	LevelSizeMultiplier int
	TableSizeMultiplier int
	MaxLevels           int

	VLogPercentile float64
	ValueThreshold int64
	NumMemtables   int
	// Changing BlockSize across DB runs will not break badger. The block size is
	// read from the block index stored at the end of the table.
	BlockSize          int
	BloomFalsePositive float64
	BlockCacheSize     int64
	IndexCacheSize     int64

	NumLevelZeroTables      int
	NumLevelZeroTablesStall int

	ValueLogFileSize   int64
	ValueLogMaxEntries uint32

	NumCompactors        int
	CompactL0OnClose     bool
	LmaxCompaction       bool
	ZSTDCompressionLevel int

	// When set, checksum will be validated for each entry read from the value log file.
	VerifyValueChecksum bool

	// Encryption related options.
	EncryptionKey                 []byte        // encryption key
	EncryptionKeyRotationDuration time.Duration // key rotation duration

	// BypassLockGuard will bypass the lock guard on badger. Bypassing lock
	// guard can cause data corruption if multiple badger instances are using
	// the same directory. Use this options with caution.
	BypassLockGuard bool

	// ChecksumVerificationMode decides when db should verify checksums for SSTable blocks.
	ChecksumVerificationMode options.ChecksumVerificationMode

	// DetectConflicts determines whether the transactions would be checked for
	// conflicts. The transactions can be processed at a higher rate when
	// conflict detection is disabled.
	DetectConflicts bool

	// NamespaceOffset specifies the offset from where the next 8 bytes contains the namespace.
	NamespaceOffset int

	// Magic version used by the application using badger to ensure that it doesn't open the DB
	// with incompatible data format.
	ExternalMagicVersion uint16
	validated            bool
}

func (c *Config) Validate() error {
	if c.StorePath == "" {
		return fmt.Errorf("store path is required")
	}

	if c.VlogsPath == "" {
		c.VlogsPath = filepath.Join(c.StorePath, "vlogs")
	}

	if c.RegistryPath == "" {
		c.RegistryPath = filepath.Join(c.StorePath, "registry")
	}

	if !checkIfDirExists(c.StorePath) {
		if err := os.MkdirAll(c.StorePath, os.ModePerm); err != nil {
			return fmt.Errorf("error creating store path: %s", err)
		}
	}

	if !checkIfDirExists(c.VlogsPath) {
		if err := os.MkdirAll(c.VlogsPath, os.ModePerm); err != nil {
			return fmt.Errorf("error creating vlogs path: %s", err)
		}
	}

	if !checkIfDirExists(c.RegistryPath) {
		if err := os.MkdirAll(c.RegistryPath, os.ModePerm); err != nil {
			return fmt.Errorf("error creating registry path: %s", err)
		}
	}

	c.validated = true
	return nil
}

func (c *Config) WithLogger(logger Logger) {
	c.Logger = badgerLogger{}
}

func (c *Config) SetDefaultOptions() badger.Options {
	return badger.DefaultOptions(c.RegistryPath).
		WithValueDir(c.VlogsPath).
		WithCompression(c.Compression).
		//WithIndexCacheSize(c.IndexCacheSize).
		//WithBlockCacheSize(c.BlockCacheSize).
		WithLogger(c.Logger).
		//WithMemTableSize(c.MemTableSize).
		WithMetricsEnabled(c.MetricsEnabled).
		WithEncryptionKey(c.EncryptionKey).
		WithVerifyValueChecksum(c.VerifyValueChecksum).
		WithChecksumVerificationMode(c.ChecksumVerificationMode).
		WithInMemory(c.InMemory)
}

func checkIfDirExists(dir string) bool {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return false
	}
	return true
}
