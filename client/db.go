// TODO(pmattis): ConditionalPut, DeleteRange

package client

import (
	"encoding"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/testutils"
	gogoproto "github.com/gogo/protobuf/proto"
)

// KeyValue ...
type KeyValue struct {
	key       []byte
	value     interface{}
	timestamp int64
}

func (kv *KeyValue) String() string {
	switch t := kv.value.(type) {
	case nil:
		return string(kv.key) + ":nil"
	case []byte:
		return string(kv.key) + ":" + string(t)
	case *int64:
		return string(kv.key) + ":" + strconv.FormatInt(*t, 10)
	}
	return string(kv.key) + ":<error>"
}

// Key ...
func (kv *KeyValue) Key(k interface{}) error {
	switch t := k.(type) {
	case encoding.BinaryUnmarshaler:
		return t.UnmarshalBinary(kv.key)
	case *[]byte:
		*t = kv.key
	case *string:
		*t = string(kv.key)
	}
	return fmt.Errorf("unable to unmarshal value: %T", k)
}

// KeyBytes ...
func (kv *KeyValue) KeyBytes() []byte {
	return kv.key
}

// Exists ...
func (kv *KeyValue) Exists() bool {
	return kv.value != nil
}

// Value ...
func (kv *KeyValue) Value(v interface{}) error {
	switch val := kv.value.(type) {
	case []byte:
		switch t := v.(type) {
		case encoding.BinaryUnmarshaler:
			return t.UnmarshalBinary(val)
		case gogoproto.Message:
			return gogoproto.Unmarshal(val, t)
		case *[]byte:
			*t = val
		case *string:
			*t = string(val)
		}
	case *int64:
		switch t := v.(type) {
		case *int64:
			*t = *val
		case *uint64:
			*t = uint64(*val)
		case *int32:
			*t = int32(*val)
		case *uint32:
			*t = uint32(*val)
		case *int:
			*t = int(*val)
		case *uint:
			*t = uint(*val)
		}
	}
	return fmt.Errorf("unable to unmarshal value: %T -> %T", kv.value, v)
}

func (kv *KeyValue) setValue(v *proto.Value) {
	if v == nil {
		return
	}
	if v.Bytes != nil {
		kv.value = v.Bytes
	} else if v.Integer != nil {
		kv.value = v.Integer
	}
	if ts := v.Timestamp; ts != nil {
		kv.timestamp = ts.WallTime
	}
}

// ValueBytes ...
func (kv *KeyValue) ValueBytes() []byte {
	return kv.value.([]byte)
}

// ValueInt ...
func (kv *KeyValue) ValueInt() int64 {
	return *kv.value.(*int64)
}

// Timestamp ...
func (kv *KeyValue) Timestamp(ts interface{}) error {
	switch t := ts.(type) {
	case *time.Time:
		panic("TODO(pmattis)")
	case *int64:
		*t = kv.timestamp
	}
	return fmt.Errorf("unable to unmarshal timestamp: %T", ts)
}

// Result ...
type Result struct {
	calls int
	Err   error
	Rows  []KeyValue
}

// DB ...
type DB struct {
	kv *KV
}

// Open ...
func Open(addr string) *DB {
	u, err := url.Parse(addr)
	if err != nil {
		log.Fatal(err)
	}

	// TODO(pmattis): This isn't right.
	ctx := testutils.NewTestBaseContext()

	var sender KVSender
	switch u.Scheme {
	case "http", "https":
		sender, err = NewHTTPSender(u.Host, ctx)
	case "rpc", "rpcs":
		sender, err = NewRPCSender(u.Host, ctx)
	default:
		log.Fatalf("unknown scheme: %s", u.Scheme)
	}
	if err != nil {
		log.Fatal(err)
	}

	kv := NewKV(nil, sender)
	kv.User = u.User.Username()
	return &DB{kv: kv}
}

// Get ...
func (db *DB) Get(args ...interface{}) Result {
	return get(db, args...)
}

// Put ...
func (db *DB) Put(key, value interface{}) error {
	return put(db, key, value)
}

// Inc ...
func (db *DB) Inc(key interface{}, value int64) Result {
	return inc(db, key, value)
}

// Scan ...
func (db *DB) Scan(begin, end interface{}, maxRows int64) Result {
	return scan(db, begin, end, maxRows)
}

// Del ...
func (db *DB) Del(args ...interface{}) error {
	return del(db, args...)
}

// Run ...
func (db *DB) Run(b *Batch) error {
	if err := b.prepare(); err != nil {
		return err
	}
	if err := db.kv.Run(b.calls...); err != nil {
		return err
	}
	return b.fillResults()
}

// Tx ...
func (db *DB) Tx(retryable func(tx *Tx) error) error {
	return db.kv.RunTransaction(nil, func(txn *Txn) error {
		tx := &Tx{txn}
		return retryable(tx)
	})
}

// Tx ...
type Tx struct {
	txn *Txn
}

// Get ...
func (tx *Tx) Get(args ...interface{}) Result {
	return get(tx, args...)
}

// Put ...
func (tx *Tx) Put(key, value interface{}) error {
	return put(tx, key, value)
}

// Inc ...
func (tx *Tx) Inc(key interface{}, value int64) Result {
	return inc(tx, key, value)
}

// Scan ...
func (tx *Tx) Scan(begin, end interface{}, maxRows int64) Result {
	return scan(tx, begin, end, maxRows)
}

// Del ...
func (tx *Tx) Del(args ...interface{}) error {
	return del(tx, args...)
}

// Run ...
func (tx *Tx) Run(b *Batch) error {
	if err := b.prepare(); err != nil {
		return err
	}
	if err := tx.txn.Run(b.calls...); err != nil {
		return err
	}
	return b.fillResults()
}

// Commit ...
func (tx *Tx) Commit(b *Batch) error {
	args := &proto.EndTransactionRequest{Commit: true}
	reply := &proto.EndTransactionResponse{}
	b.calls = append(b.calls, Call{Args: args, Reply: reply})
	b.initResult(1, 0, nil)
	return tx.Run(b)
}

// Batch ...
type Batch struct {
	Results []Result
	calls   []Call
	// TODO(pmattis): Use fixed size arrays for Result and KeyValue to
	// avoid allocations in the common case.
	// resultsBuf [8]Result
	// rowsBuf [8]KeyValue
}

func (b *Batch) prepare() error {
	for _, r := range b.Results {
		if err := r.Err; err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) initResult(calls, numRows int, err error) {
	r := Result{calls: calls, Err: err}
	if numRows > 0 {
		r.Rows = make([]KeyValue, numRows)
	}
	b.Results = append(b.Results, r)
}

func (b *Batch) fillResults() error {
	offset := 0
	for i := range b.Results {
		result := &b.Results[i]

		for k := 0; k < result.calls; k++ {
			call := b.calls[offset+k]

			switch t := call.Reply.(type) {
			case *proto.GetResponse:
				row := &result.Rows[k]
				row.key = []byte(call.Args.(*proto.GetRequest).Key)
				row.setValue(t.Value)
			case *proto.PutResponse:
				row := &result.Rows[k]
				row.key = []byte(call.Args.(*proto.PutRequest).Key)
				row.setValue(&call.Args.(*proto.PutRequest).Value)
			case *proto.IncrementResponse:
				row := &result.Rows[k]
				row.key = []byte(call.Args.(*proto.IncrementRequest).Key)
				row.value = &t.NewValue
			case *proto.ScanResponse:
				for _, kv := range t.Rows {
					var row KeyValue
					row.key = kv.Key
					row.setValue(&kv.Value)
					result.Rows = append(result.Rows, row)
				}
			case *proto.DeleteResponse:
				row := &result.Rows[k]
				row.key = []byte(call.Args.(*proto.DeleteRequest).Key)
			}
		}
		offset += result.calls
	}
	return nil
}

// Get ...
func (b *Batch) Get(args ...interface{}) *Batch {
	var calls []Call
	for _, arg := range args {
		k, err := marshalKey(arg)
		if err != nil {
			b.initResult(0, len(args), err)
			break
		}
		calls = append(calls, Get(proto.Key(k)))
	}
	b.calls = append(b.calls, calls...)
	b.initResult(len(calls), len(calls), nil)
	return b
}

// Put ...
func (b *Batch) Put(key, value interface{}) *Batch {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	v, err := marshalValue(value)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	b.calls = append(b.calls, Put(proto.Key(k), v))
	b.initResult(1, 1, nil)
	return b
}

// Inc ...
func (b *Batch) Inc(key interface{}, value int64) *Batch {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	b.calls = append(b.calls, Increment(proto.Key(k), value))
	b.initResult(1, 1, nil)
	return b
}

// Scan ...
func (b *Batch) Scan(s, e interface{}, maxRows int64) *Batch {
	begin, err := marshalKey(s)
	if err != nil {
		b.initResult(0, 0, err)
		return b
	}
	end, err := marshalKey(e)
	if err != nil {
		b.initResult(0, 0, err)
		return b
	}
	b.calls = append(b.calls, Scan(proto.Key(begin), proto.Key(end), maxRows))
	b.initResult(1, 0, nil)
	return b
}

// Del ...
func (b *Batch) Del(args ...interface{}) *Batch {
	var calls []Call
	for _, arg := range args {
		k, err := marshalKey(arg)
		if err != nil {
			b.initResult(0, len(args), err)
			return b
		}
		calls = append(calls, Delete(proto.Key(k)))
	}
	b.calls = append(b.calls, calls...)
	b.initResult(len(calls), len(calls), nil)
	return b
}

type batcher struct{}

// B ...
var B batcher

// Get ...
func (b batcher) Get(args ...interface{}) *Batch {
	return (&Batch{}).Get(args...)
}

// Put ...
func (b batcher) Put(key, value interface{}) *Batch {
	return (&Batch{}).Put(key, value)
}

// Inc ...
func (b batcher) Inc(key interface{}, value int64) *Batch {
	return (&Batch{}).Inc(key, value)
}

// Scan ...
func (b batcher) Scan(s, e interface{}, maxRows int64) *Batch {
	return (&Batch{}).Scan(s, e, maxRows)
}

// Del ...
func (b batcher) Del(args ...interface{}) *Batch {
	return (&Batch{}).Del(args)
}

func marshalKey(k interface{}) ([]byte, error) {
	switch t := k.(type) {
	case encoding.BinaryMarshaler:
		return t.MarshalBinary()
	case string:
		return []byte(t), nil
	case []byte:
		return t, nil
	case proto.Key:
		return []byte(t), nil
	}
	return nil, fmt.Errorf("unable to marshal key: %T", k)
}

func marshalValue(v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case encoding.BinaryMarshaler:
		return t.MarshalBinary()
	case gogoproto.Message:
		return gogoproto.Marshal(t)
	case string:
		return []byte(t), nil
	case []byte:
		return t, nil
	}
	return nil, fmt.Errorf("unable to marshal value: %T", v)
}

type runner interface {
	Run(b *Batch) error
}

func get(r runner, args ...interface{}) Result {
	b := &Batch{}
	b.Get(args...)
	if err := r.Run(b); err != nil {
		return Result{Err: err}
	}
	return b.Results[0]
}

func put(r runner, key, value interface{}) error {
	b := &Batch{}
	b.Put(key, value)
	if err := r.Run(b); err != nil {
		return err
	}
	return b.Results[0].Err
}

func inc(r runner, key interface{}, value int64) Result {
	b := &Batch{}
	b.Inc(key, value)
	if err := r.Run(b); err != nil {
		return Result{Err: err}
	}
	return b.Results[0]
}

func scan(r runner, begin, end interface{}, maxRows int64) Result {
	b := &Batch{}
	b.Scan(begin, end, maxRows)
	if err := r.Run(b); err != nil {
		return Result{Err: err}
	}
	return b.Results[0]
}

func del(r runner, args ...interface{}) error {
	b := &Batch{}
	b.Del(args...)
	if err := r.Run(b); err != nil {
		return err
	}
	return b.Results[0].Err
}
