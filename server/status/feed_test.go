// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package status_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// simpleEventConsumer stores every event published to a feed.
type simpleEventConsumer struct {
	sub      *util.Subscription
	received []interface{}
}

func newSimpleEventConsumer(feed *util.Feed) *simpleEventConsumer {
	return &simpleEventConsumer{
		sub: feed.Subscribe(),
	}
}

func (sec *simpleEventConsumer) process() {
	for e := range sec.sub.Events() {
		sec.received = append(sec.received, e)
	}
}

// startConsumerSet starts a NodeEventFeed and a number of associated
// simple consumers.
func startConsumerSet(count int) (*util.Stopper, *util.Feed, []*simpleEventConsumer) {
	stopper := util.NewStopper()
	feed := &util.Feed{}
	consumers := make([]*simpleEventConsumer, count)
	for i := range consumers {
		consumers[i] = newSimpleEventConsumer(feed)
		stopper.RunWorker(consumers[i].process)
	}
	return stopper, feed, consumers
}

// waitForStopper stops the supplied util.Stopper and waits up to five seconds
// for it to complete.
func waitForStopper(t testing.TB, stopper *util.Stopper) {
	stopper.Stop()
	select {
	case <-stopper.IsStopped():
	case <-time.After(5 * time.Second):
		t.Fatalf("Stopper failed to stop after 5 seconds")
	}
}

func TestNodeEventFeed(t *testing.T) {
	defer leaktest.AfterTest(t)

	nodeDesc := proto.NodeDescriptor{
		NodeID: proto.NodeID(99),
	}

	// A testCase corresponds to a single Store event type. Each case contains a
	// method which publishes a single event to the given storeEventPublisher,
	// and an expected result interface which should match the produced
	// event.
	testCases := []struct {
		name      string
		publishTo func(status.NodeEventFeed)
		expected  interface{}
	}{
		{
			name: "Start",
			publishTo: func(nef status.NodeEventFeed) {
				nef.StartNode(nodeDesc, 100)
			},
			expected: &status.StartNodeEvent{
				Desc:      nodeDesc,
				StartedAt: 100,
			},
		},
		{
			name: "Get",
			publishTo: func(nef status.NodeEventFeed) {
				call := proto.GetCall(proto.Key("abc"))
				nef.CallComplete(call.Args, call.Reply)
			},
			expected: &status.CallSuccessEvent{
				NodeID: proto.NodeID(1),
				Method: proto.Get,
			},
		},
		{
			name: "Put",
			publishTo: func(nef status.NodeEventFeed) {
				call := proto.PutCall(proto.Key("abc"), proto.Value{Bytes: []byte("def")})
				nef.CallComplete(call.Args, call.Reply)
			},
			expected: &status.CallSuccessEvent{
				NodeID: proto.NodeID(1),
				Method: proto.Put,
			},
		},
		{
			name: "Get Error",
			publishTo: func(nef status.NodeEventFeed) {
				call := proto.GetCall(proto.Key("abc"))
				call.Reply.Header().SetGoError(util.Errorf("error"))
				nef.CallComplete(call.Args, call.Reply)
			},
			expected: &status.CallErrorEvent{
				NodeID: proto.NodeID(1),
				Method: proto.Get,
			},
		},
	}

	// Compile expected events into a single slice.
	expectedEvents := make([]interface{}, len(testCases))
	for i := range testCases {
		expectedEvents[i] = testCases[i].expected
	}

	// assertEventsEqual verifies that the given set of events is equal to the
	// expectedEvents.
	verifyEventSlice := func(source string, events []interface{}) {
		if a, e := len(events), len(expectedEvents); a != e {
			t.Errorf("%s had wrong number of events %d, expected %d", source, a, e)
			return
		}

		for i := range events {
			if a, e := events[i], expectedEvents[i]; !reflect.DeepEqual(a, e) {
				t.Errorf("%s had wrong event for case %s: got %v, expected %v", source, testCases[i].name, a, e)
			}
		}
	}

	// Run test cases directly through a feed.
	stopper, feed, consumers := startConsumerSet(3)
	nodefeed := status.NewNodeEventFeed(proto.NodeID(1), feed)
	for _, tc := range testCases {
		tc.publishTo(nodefeed)
	}
	feed.Close()
	waitForStopper(t, stopper)
	for i, c := range consumers {
		verifyEventSlice(fmt.Sprintf("feed direct consumer %d", i), c.received)
	}
}

// nodeEventReader reads the node-related events off of a feed subscription,
// ignoring other events.
type nodeEventReader struct {
	perNodeFeeds map[proto.NodeID][]string
}

// recordEvent records an events received from the node itself. Each event is
// recorded as a simple string value; this is less exhaustive than a full struct
// comparison, but should be easier to correct if future changes slightly modify
// these values. Events which do not pertain to a Node are ignored.
func (ner *nodeEventReader) recordEvent(event interface{}) {
	var nid proto.NodeID
	eventStr := ""
	switch event := event.(type) {
	case *status.CallSuccessEvent:
		if event.Method == proto.InternalResolveIntent {
			// Ignore this best-effort method.
			break
		}
		if event.Method == proto.InternalRangeLookup {
			// Due to a race with the server's status recording system, we can't
			// reliably depend on InternalRangeLookup to occur during the test.
			// Ignore this method.
			break
		}
		nid = event.NodeID
		eventStr = event.Method.String()
	case *status.CallErrorEvent:
		nid = event.NodeID
		eventStr = "failed " + event.Method.String()
	}
	if nid > 0 {
		ner.perNodeFeeds[nid] = append(ner.perNodeFeeds[nid], eventStr)
	}
}

func (ner *nodeEventReader) readEvents(sub *util.Subscription) {
	ner.perNodeFeeds = make(map[proto.NodeID][]string)
	for e := range sub.Events() {
		ner.recordEvent(e)
	}
}

// eventFeedString describes the event information that was recorded by
// nodeEventReader. The formatting is appropriate to paste directly into test as
// a new expected value.
func (ner *nodeEventReader) eventFeedString() string {
	var response string
	for id, feed := range ner.perNodeFeeds {
		response += fmt.Sprintf("proto.NodeID(%d): []string{\n", int64(id))
		for _, evt := range feed {
			response += fmt.Sprintf("\t\t\"%s\",\n", evt)
		}
		response += fmt.Sprintf("},\n")
	}
	return response
}

// TestServerNodeEventFeed verifies that a test server emits Node-specific
// events.
func TestServerNodeEventFeed(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	feed := s.EventFeed()

	// Start reading events from the feed before starting the stores.
	readStopper := util.NewStopper()
	ner := &nodeEventReader{}
	sub := feed.Subscribe()
	readStopper.RunWorker(func() {
		ner.readEvents(sub)
	})

	db, err := client.Open("https://root@" + s.ServingAddr() + "?certs=" + security.EmbeddedCertsDir)
	if err != nil {
		t.Fatal(err)
	}

	// Add some data in a transaction
	err = db.Txn(func(txn *client.Txn) error {
		b := &client.Batch{}
		b.Put("a", "asdf")
		b.Put("c", "jkl;")
		return txn.Commit(b)
	})
	if err != nil {
		t.Fatalf("error putting data to db: %s", err)
	}

	// Get some data, discarding the result.
	if _, err := db.Get("a"); err != nil {
		t.Fatalf("error getting data from db: %s", err)
	}

	// Scan, which should fail.
	if _, err = db.Scan("b", "a", 0); err == nil {
		t.Fatal("expected scan to fail.")
	}

	// Close feed and wait for reader to receive all events.
	feed.Close()
	readStopper.Stop()

	expectedNodeEvents := map[proto.NodeID][]string{
		proto.NodeID(1): {
			"Put",
			"Put",
			"EndTransaction",
			"Get",
			"failed Scan",
		},
	}

	// TODO(mtracy): This assertion has been made "fuzzy" in order to account
	// for the unpredictably ordered events from an asynchronous background
	// task.  A future commit should disable that background task (status
	// recording) during this test, and exact matching should be restored.
	/*
	   if a, e := ner.perNodeFeeds, expectedNodeEvents; !reflect.DeepEqual(a, e) {
	       t.Errorf("node feed did not match expected value. Actual values have been printed to compare with above expectation.\n")
	       t.Logf("Event feed information:\n%s", ner.eventFeedString())
	   }
	*/

	// The actual results should contain the expected results as an ordered
	// subset.
	passed := true
	for k := range expectedNodeEvents {
		// Maintain an index into the actual and expected feed slices.
		actual, expected := ner.perNodeFeeds[k], expectedNodeEvents[k]
		i, j := 0, 0
		// Advance indexes until one or both slices are exhausted.
		for i < len(expected) && j < len(actual) {
			// If the current expected value matches the current actual value,
			// advance both indexes. Otherwise, advance only the actual index.
			if reflect.DeepEqual(expected[i], actual[j]) {
				i++
			}
			j++
		}
		// Test succeeded if it advanced over every expected event.
		if i != len(expected) {
			passed = false
			break
		}
	}
	if !passed {
		t.Errorf("node feed did not contain expected subset. Actual values have been printed to compare with expectation.\n")
		t.Logf("Event feed information:\n%s", ner.eventFeedString())
	}
}

// TestNodeEventFeedTransactionRestart verifies that calls which indicate a
// transaction restart are counted as successful.
func TestNodeEventFeedTransactionRestart(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper, feed, consumers := startConsumerSet(1)
	nodefeed := status.NewNodeEventFeed(proto.NodeID(1), feed)
	ner := &nodeEventReader{}
	sub := feed.Subscribe()
	stopper.RunWorker(func() {
		ner.readEvents(sub)
	})
	nodeID := proto.NodeID(1)

	nodefeed.CallComplete(&proto.GetRequest{}, &proto.GetResponse{
		ResponseHeader: proto.ResponseHeader{
			Error: &proto.Error{
				TransactionRestart: proto.TransactionRestart_BACKOFF,
			},
		},
	})
	nodefeed.CallComplete(&proto.GetRequest{}, &proto.GetResponse{
		ResponseHeader: proto.ResponseHeader{
			Error: &proto.Error{
				TransactionRestart: proto.TransactionRestart_IMMEDIATE,
			},
		},
	})
	nodefeed.CallComplete(&proto.PutRequest{}, &proto.PutResponse{
		ResponseHeader: proto.ResponseHeader{
			Error: &proto.Error{
				TransactionRestart: proto.TransactionRestart_ABORT,
			},
		},
	})
	feed.Close()
	stopper.Stop()

	c := consumers[0]
	exp := []interface{}{
		&status.CallSuccessEvent{
			NodeID: nodeID,
			Method: proto.Get,
		},
		&status.CallSuccessEvent{
			NodeID: nodeID,
			Method: proto.Get,
		},
		&status.CallErrorEvent{
			NodeID: nodeID,
			Method: proto.Put,
		},
	}

	if !reflect.DeepEqual(exp, c.received) {
		t.Fatalf("received unexpected events: %s", ner.eventFeedString())
	}
}
