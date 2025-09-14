package dsqueue_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-dsqueue"
	"github.com/ipfs/go-test/random"
)

const dsqName = "testq"

func assertOrdered(t *testing.T, cids []cid.Cid, q *dsqueue.DSQueue) {
	t.Helper()

	var count int
	for i, c := range cids {
		select {
		case dequeued, ok := <-q.Out():
			if !ok {
				t.Fatal("queue closed")
			}
			bs := base64.RawURLEncoding.EncodeToString(dequeued)
			cd, err := cid.Parse(dequeued)
			if err != nil {
				t.Fatalf("Invalid cid in queue: %s", err)
			}
			if c != cd {
				t.Fatalf("Error in ordering of CID %d retrieved from queue. Expected: %s, got: %s, dequed: %s", i, c, cd, bs)
			}
			count++
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for cids to be provided.")
		}
	}
	t.Log("read", count, "cids")
}

func TestBasicOperation(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	if queue.Name() != dsqName {
		t.Fatal("wrong queue name")
	}

	queue.Put(nil)
	select {
	case <-queue.Out():
		t.Fatal("nothing should be in queue")
	case <-time.After(time.Millisecond):
	}

	items := []string{"apple", "banana", "cherry"}

	out := make(chan []string)
	go func() {
		var outStrs []string
		for dq := range queue.Out() {
			dqItem := string(dq)
			outStrs = append(outStrs, dqItem)
			if len(outStrs) == len(items) {
				break
			}
		}
		out <- outStrs
	}()

	for _, item := range items {
		queue.Put([]byte(item))
	}

	qout := <-out

	if len(qout) != len(items) {
		t.Fatalf("dequeued wrong number of items, expected %d, got %d", len(items), len(qout))
	}

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = queue.Put([]byte(items[0]))
	if err == nil {
		t.Fatal("expected error calling Enqueue after Close")
	}

	_, err = queue.GetN(5)
	if err == nil {
		t.Fatal("expected error calling GetN after Close")
	}
}

func TestGetN(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName, dsqueue.WithBufferSize(5), dsqueue.WithDedupCacheSize(0))
	defer queue.Close()

	cids := random.Cids(29)
	for _, c := range cids {
		queue.Put(c.Bytes())
	}

	outItems, err := queue.GetN(50)
	if err != nil {
		t.Fatal(err)
	}

	if len(outItems) != len(cids) {
		t.Fatalf("dequeued wrond number of items, expected %d, got %d", len(cids), len(outItems))
	}

	for i := range outItems {
		outCid, err := cid.Parse(outItems[i])
		if err != nil {
			t.Fatal(err)
		}
		if outCid != cids[i] {
			t.Fatal("retrieved items out of order")
		}
	}

	outItems, err = queue.GetN(10)
	if err != nil {
		t.Fatal(err)
	}

	if len(outItems) != 0 {
		t.Fatal("shoul not get anymore items from queue")
	}
}

func TestMangledData(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())

	// put bad data in the queue
	qds := namespace.Wrap(ds, datastore.NewKey("/dsq-"+dsqName))
	item := "borked"
	queueKey := datastore.NewKey(item)
	err := qds.Put(context.Background(), queueKey, []byte(item))
	if err != nil {
		t.Fatal(err)
	}

	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	cids := random.Cids(10)
	for _, c := range cids {
		queue.Put(c.Bytes())
	}

	// expect to only see the valid cids we entered
	assertOrdered(t, cids, queue)
}

func TestInitialization(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	cids := random.Cids(10)
	for _, c := range cids {
		queue.Put(c.Bytes())
	}

	assertOrdered(t, cids[:5], queue)

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	// make a new queue, same data
	queue = dsqueue.New(ds, dsqName)
	defer queue.Close()

	assertOrdered(t, cids[5:], queue)
}

func TestIdleFlush(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName, dsqueue.WithBufferSize(-1), dsqueue.WithIdleWriteTime(time.Millisecond))
	defer queue.Close()

	cids := random.Cids(10)
	for _, c := range cids {
		queue.Put(c.Bytes())
	}

	dsn := namespace.Wrap(ds, datastore.NewKey("/dsq-"+dsqName))
	time.Sleep(10 * time.Millisecond)

	ctx := context.Background()
	n, err := countItems(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("expected nothing in datastore")
	}

	wanted := len(cids) - 1
	got := 0
	for range 5 {
		time.Sleep(time.Second)
		n, err = countItems(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		got += n
		if got >= wanted {
			break
		}
	}

	if got != wanted {
		t.Fatalf("should have flushed %d cids to datastore, got %d", wanted, got)
	}
}

func countItems(ctx context.Context, ds datastore.Datastore) (int, error) {
	qry := query.Query{
		KeysOnly: true,
	}
	results, err := ds.Query(ctx, qry)
	if err != nil {
		return 0, fmt.Errorf("cannot query datastore: %w", err)
	}
	defer results.Close()

	var count int
	for result := range results.Next() {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if result.Error != nil {
			return 0, fmt.Errorf("cannot read query result from datastore: %w", result.Error)
		}
		count++
	}

	return count, nil
}

func TestPersistManyCids(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName, dsqueue.WithBufferSize(5), dsqueue.WithDedupCacheSize(0))
	defer queue.Close()

	cids := random.Cids(25)
	for _, c := range cids {
		queue.Put(c.Bytes())
	}

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	// make a new queue, same data
	queue = dsqueue.New(ds, dsqName)
	defer queue.Close()

	assertOrdered(t, cids, queue)
}

func TestPersistOneCid(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	cids := random.Cids(1)
	queue.Put(cids[0].Bytes())

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	// make a new queue, same data
	queue = dsqueue.New(ds, dsqName)
	defer queue.Close()

	assertOrdered(t, cids, queue)
}

func TestDeduplicateCids(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	cids := random.Cids(5)
	queue.Put(cids[0].Bytes())
	queue.Put(cids[0].Bytes())
	queue.Put(cids[1].Bytes())
	queue.Put(cids[2].Bytes())
	queue.Put(cids[1].Bytes())
	queue.Put(cids[3].Bytes())
	queue.Put(cids[0].Bytes())
	queue.Put(cids[4].Bytes())

	assertOrdered(t, cids, queue)

	// Test with dedup cache disabled.
	queue = dsqueue.New(ds, dsqName, dsqueue.WithDedupCacheSize(-1))
	defer queue.Close()

	cids = append(cids, cids[0], cids[0], cids[1])
	for _, c := range cids {
		queue.Put(c.Bytes())
	}
	assertOrdered(t, cids, queue)
}

func TestClear(t *testing.T) {
	const cidCount = 25

	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	for _, c := range random.Cids(cidCount) {
		queue.Put(c.Bytes())
	}

	// Cause queued entried to be saved in datastore.
	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	queue = dsqueue.New(ds, dsqName)
	defer queue.Close()

	for _, c := range random.Cids(cidCount) {
		queue.Put(c.Bytes())
	}

	rmCount := queue.Clear()
	t.Log("Cleared", rmCount, "entries from provider queue")
	if rmCount != 2*cidCount {
		t.Fatalf("expected %d cleared, got %d", 2*cidCount, rmCount)
	}

	if err = queue.Close(); err != nil {
		t.Fatal(err)
	}

	// Ensure no data when creating new queue.
	queue = dsqueue.New(ds, dsqName)
	defer queue.Close()

	select {
	case <-queue.Out():
		t.Fatal("dequeue should not return")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestCloseTimeout(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	sds := &slowds{
		Batching: ds,
		delay:    time.Second,
	}
	queue := dsqueue.New(sds, dsqName, dsqueue.WithBufferSize(5), dsqueue.WithCloseTimeout(time.Microsecond))
	defer queue.Close()

	cids := random.Cids(5)
	for _, c := range cids {
		queue.Put(c.Bytes())
	}

	err := queue.Close()
	if err == nil {
		t.Fatal("expected error")
	}
	const expectErr = "5 items not written to datastore"
	if err.Error() != expectErr {
		t.Fatalf("did not get expected err %q, got %q", expectErr, err.Error())
	}

	// Test with no close timeout.
	queue = dsqueue.New(sds, dsqName, dsqueue.WithBufferSize(5), dsqueue.WithCloseTimeout(-1))
	defer queue.Close()

	for _, c := range cids {
		queue.Put(c.Bytes())
	}
	if err = queue.Close(); err != nil {
		t.Fatal(err)
	}
}

type slowds struct {
	datastore.Batching
	delay time.Duration
}

func (sds *slowds) Put(ctx context.Context, key datastore.Key, value []byte) error {
	time.Sleep(sds.delay)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return sds.Batching.Put(ctx, key, value)
}

func BenchmarkGetN(b *testing.B) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName, dsqueue.WithDedupCacheSize(0))
	defer queue.Close()

	cids := random.Cids(64)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, c := range cids {
			queue.Put(c.Bytes())
		}

		outItems, err := queue.GetN(100)
		if err != nil {
			b.Fatal(err)
		}
		if len(outItems) != len(cids) {
			b.Fatalf("dequeued wrond number of items, expected %d, got %d", len(cids), len(outItems))
		}
	}
}
