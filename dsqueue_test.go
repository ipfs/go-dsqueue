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
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-dsqueue"
	"github.com/ipfs/go-test/random"
)

const dsqName = "testq"

func assertOrdered(cids []cid.Cid, q *dsqueue.DSQueue, t *testing.T) {
	t.Helper()

	var count int
	for i, c := range cids {
		select {
		case dequeued, ok := <-q.Dequeue():
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

	cids := random.Cids(10)
	for _, c := range cids {
		queue.Enqueue(c.Bytes())
	}

	assertOrdered(cids, queue, t)

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = queue.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMangledData(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())

	// put bad data in the queue
	qds := namespace.Wrap(ds, datastore.NewKey("/dsq-"+dsqName))
	item := "borked"
	queueKey := datastore.NewKey(fmt.Sprintf("%s", item))
	err := qds.Put(context.Background(), queueKey, []byte(item))
	if err != nil {
		t.Fatal(err)
	}

	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	cids := random.Cids(10)
	for _, c := range cids {
		queue.Enqueue(c.Bytes())
	}

	// expect to only see the valid cids we entered
	expected := cids
	assertOrdered(expected, queue, t)
}

func TestInitialization(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	cids := random.Cids(10)
	for _, c := range cids {
		queue.Enqueue(c.Bytes())
	}

	assertOrdered(cids[:5], queue, t)

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	// make a new queue, same data
	queue = dsqueue.New(ds, dsqName)
	defer queue.Close()

	assertOrdered(cids[5:], queue, t)
}

func TestInitializationWithManyCids(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName, dsqueue.WithBufferSize(25))
	defer queue.Close()

	cids := random.Cids(100)
	for _, c := range cids {
		queue.Enqueue(c.Bytes())
	}

	/*
		err := queue.Close()
		if err != nil {
			t.Fatal(err)
		}

		// make a new queue, same data
		queue = dsqueue.New(ds, dsqName)
		defer queue.Close()
	*/

	assertOrdered(cids, queue, t)
}

func TestPersistManyCids(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName, dsqueue.WithBufferSize(5), dsqueue.WithDedupCacheSize(0))
	defer queue.Close()

	cids := random.Cids(25)
	for _, c := range cids {
		queue.Enqueue(c.Bytes())
	}

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	// make a new queue, same data
	queue = dsqueue.New(ds, dsqName)
	defer queue.Close()

	assertOrdered(cids, queue, t)
}

func TestPersistOneCid(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	cids := random.Cids(1)
	queue.Enqueue(cids[0].Bytes())

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	// make a new queue, same data
	queue = dsqueue.New(ds, dsqName)
	defer queue.Close()

	assertOrdered(cids, queue, t)
}

func TestDeduplicateCids(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	cids := random.Cids(5)
	queue.Enqueue(cids[0].Bytes())
	queue.Enqueue(cids[0].Bytes())
	queue.Enqueue(cids[1].Bytes())
	queue.Enqueue(cids[2].Bytes())
	queue.Enqueue(cids[1].Bytes())
	queue.Enqueue(cids[3].Bytes())
	queue.Enqueue(cids[0].Bytes())
	queue.Enqueue(cids[4].Bytes())

	assertOrdered(cids, queue, t)
}

func TestClear(t *testing.T) {
	const cidCount = 25

	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := dsqueue.New(ds, dsqName)
	defer queue.Close()

	for _, c := range random.Cids(cidCount) {
		queue.Enqueue(c.Bytes())
	}

	// Cause queued entried to be saved in datastore.
	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	queue = dsqueue.New(ds, dsqName)
	defer queue.Close()

	for _, c := range random.Cids(cidCount) {
		queue.Enqueue(c.Bytes())
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
	case <-queue.Dequeue():
		t.Fatal("dequeue should not return")
	case <-time.After(10 * time.Millisecond):
	}
}
