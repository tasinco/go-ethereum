package rtprunerutils

import (
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
)

var (
	noopWriter                      = &NoopWriter{}
	_          ethdb.KeyValueWriter = NoopWriter{}
)

type NoopWriter struct{}

func (n NoopWriter) Put([]byte, []byte) error {
	return nil
}

func (n NoopWriter) Delete([]byte) error {
	return nil
}

func NewInterceptorWriter() *InterceptorWriter {
	return &InterceptorWriter{
		writer: noopWriter,
	}
}

type InterceptorWriter struct {
	writer ethdb.KeyValueWriter
	l      sync.RWMutex
	err    error
}

func (w *InterceptorWriter) SetError(err error) error {
	if err == nil {
		return nil
	}
	w.l.Lock()
	defer w.l.Unlock()
	w.err = err
	return err
}

func (w *InterceptorWriter) Err() error {
	w.l.Lock()
	defer w.l.Unlock()
	return w.err
}

func (w *InterceptorWriter) SetWriter(writer ethdb.KeyValueWriter) {
	w.l.Lock()
	defer w.l.Unlock()
	if writer == nil {
		writer = noopWriter
	}
	w.writer = writer
}

func (w *InterceptorWriter) Put(key []byte, value []byte) error {
	w.l.RLock()
	defer w.l.RUnlock()
	return w.writer.Put(key, value)
}

func (w *InterceptorWriter) Delete(key []byte) error {
	return nil
}

func NewInterceptDatabase(db ethdb.Database) ethdb.Database {
	return &InterceptDatabase{
		db:     db,
		Writer: NewInterceptorWriter(),
	}
}

type InterceptDatabase struct {
	db     ethdb.Database
	Writer *InterceptorWriter
	locked int32
	lock   Locker
}

func (w *InterceptDatabase) HasAncient(kind string, number uint64) (bool, error) {
	return w.db.HasAncient(kind, number)
}

func (w *InterceptDatabase) Ancient(kind string, number uint64) ([]byte, error) {
	return w.db.Ancient(kind, number)
}

func (w *InterceptDatabase) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	return w.db.AncientRange(kind, start, count, maxBytes)
}

func (w *InterceptDatabase) Ancients() (uint64, error) {
	return w.db.Ancients()
}

func (w *InterceptDatabase) Tail() (uint64, error) {
	return w.db.Tail()
}

func (w *InterceptDatabase) AncientSize(kind string) (uint64, error) {
	return w.db.AncientSize(kind)
}

func (w *InterceptDatabase) ReadAncients(fn func(ethdb.AncientReaderOp) error) (err error) {
	return w.db.ReadAncients(fn)
}

func (w *InterceptDatabase) ModifyAncients(f func(ethdb.AncientWriteOp) error) (int64, error) {
	return w.db.ModifyAncients(f)
}

func (w *InterceptDatabase) TruncateHead(n uint64) error {
	return w.db.TruncateHead(n)
}

func (w *InterceptDatabase) TruncateTail(n uint64) error {
	return w.db.TruncateTail(n)
}

func (w *InterceptDatabase) Sync() error {
	return w.db.Sync()
}

func (w *InterceptDatabase) MigrateTable(s string, f func([]byte) ([]byte, error)) error {
	return w.db.MigrateTable(s, f)
}

func (w *InterceptDatabase) NewBatchWithSize(size int) ethdb.Batch {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.NewBatchNoLock()
}

func (w *InterceptDatabase) AncientDatadir() (string, error) {
	return w.db.AncientDatadir()
}

func (w *InterceptDatabase) NewSnapshot() (ethdb.Snapshot, error) {
	return w.db.NewSnapshot()
}

func (w *InterceptDatabase) CaptureStart(writer ethdb.KeyValueWriter) {
	w.Writer.SetWriter(writer)
}

func (w *InterceptDatabase) CaptureStop() {
	w.Writer.SetWriter(nil)
}

func (w *InterceptDatabase) Err() error {
	return w.Writer.Err()
}

func (w *InterceptDatabase) Has(key []byte) (bool, error) {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.db.Has(key)
}

func (w *InterceptDatabase) Get(key []byte) ([]byte, error) {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.db.Get(key)
}

func (w *InterceptDatabase) Put(key []byte, value []byte) error {
	w.lock.RLock()
	defer w.lock.RUnlock()
	// only need the key for the filter.
	w.Writer.SetError(w.Writer.Put(key, key))
	return w.Writer.SetError(w.db.Put(key, value))
}

func (w *InterceptDatabase) Delete(key []byte) error {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.Writer.SetError(w.db.Delete(key))
}

func (w *InterceptDatabase) NewBatch() ethdb.Batch {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.NewBatchNoLock()
}

func (w *InterceptDatabase) NewBatchNoLock() *InterceptDatabaseBatch {
	return &InterceptDatabaseBatch{
		batch:  w.db.NewBatch(),
		parent: w,
	}
}

func (w *InterceptDatabase) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.NewIteratorNoLock(prefix, start)
}

func (w *InterceptDatabase) NewIteratorNoLock(prefix []byte, start []byte) ethdb.Iterator {
	return w.db.NewIterator(prefix, start)
}

func (w *InterceptDatabase) Stat(property string) (string, error) {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.db.Stat(property)
}

func (w *InterceptDatabase) Compact(start []byte, limit []byte) error {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.db.Compact(start, limit)
}

func (w *InterceptDatabase) Close() error {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.db.Close()
}

func (w *InterceptDatabase) Locker() *Locker {
	return &w.lock
}

type InterceptDatabaseBatch struct {
	batch  ethdb.Batch
	parent *InterceptDatabase
}

func (w *InterceptDatabaseBatch) Put(key []byte, value []byte) error {
	w.parent.Writer.SetError(w.parent.Writer.Put(key, key))
	return w.parent.Writer.SetError(w.batch.Put(key, value))
}

func (w *InterceptDatabaseBatch) Delete(key []byte) error {
	return w.parent.Writer.SetError(w.batch.Delete(key))
}

func (w *InterceptDatabaseBatch) ValueSize() int {
	return w.batch.ValueSize()
}

func (w *InterceptDatabaseBatch) Write() error {
	w.parent.lock.RLock()
	defer w.parent.lock.RUnlock()
	return w.WriteNoLock()
}

func (w *InterceptDatabaseBatch) WriteNoLock() error {
	return w.parent.Writer.SetError(w.batch.Write())
}

func (w *InterceptDatabaseBatch) Reset() {
	w.parent.lock.RLock()
	defer w.parent.lock.RUnlock()
	w.ResetNoLock()
}

func (w *InterceptDatabaseBatch) ResetNoLock() {
	w.batch.Reset()
}

func (w *InterceptDatabaseBatch) Replay(writer ethdb.KeyValueWriter) error {
	w.parent.lock.RLock()
	defer w.parent.lock.RUnlock()
	return w.batch.Replay(writer)
}

// GetInterceptDatabase safely casts given triedb into *InterceptDatabase
// may return a nil pointer if unsuitable
func GetInterceptDatabase(d ethdb.Database) *InterceptDatabase {
	var interceptDb *InterceptDatabase
	if wdb, ok := d.(*InterceptDatabase); ok {
		interceptDb = wdb
	}
	return interceptDb
}
