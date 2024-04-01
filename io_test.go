package gearmin

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestDecodeArgs(t *testing.T) {
	/*
		00 52 45 51                \0REQ        (Magic)
		00 00 00 07                7            (Packet type: SUBMIT_JOB)
		00 00 00 0d                13           (Packet length)
		72 65 76 65 72 73 65 00    reverse\0    (Function)
		00                         \0           (Unique ID)
		74 65 73 74                test         (Workload)
	*/

	data := []byte{
		0x72, 0x65, 0x76, 0x65, 0x72, 0x73, 0x65, 0x00,
		0x00,
		0x74, 0x65, 0x73, 0x74,
	}
	slice, ok := decodeArgs(packetSubmitJob, data)
	if !ok {
		t.Error("should be true")
	}

	if len(slice) != 3 {
		t.Error("arg count not match")
	}

	if !bytes.Equal(slice[0], []byte{0x72, 0x65, 0x76, 0x65, 0x72, 0x73, 0x65}) {
		t.Errorf("decode not match %+v", slice)
	}

	if !bytes.Equal(slice[1], []byte{}) {
		t.Error("decode not match")
	}

	if !bytes.Equal(slice[2], []byte{0x74, 0x65, 0x73, 0x74}) {
		t.Error("decode not match")
	}

	data = []byte{
		0x48, 0x3A, 0x2D, 0x51, 0x3A, 0x2D, 0x34, 0x37, 0x31, 0x36, 0x2D, 0x31,
		0x33, 0x39, 0x38, 0x31, 0x30, 0x36, 0x32, 0x33, 0x30, 0x2D, 0x32, 0x00, 0x00, 0x39, 0x38, 0x31,
		0x30,
	}

	slice, ok = decodeArgs(packetWorkComplete, data)
	if !ok {
		t.Error("should be true")
	}

	if len(slice[0]) == 0 || len(slice[1]) == 0 {
		t.Error("arg count not match")
	}
}

func TestQueueingWriter(t *testing.T) {
	in := make(chan []byte)
	out := make(chan []byte)
	messages := [][]byte{
		[]byte("first message"),
		[]byte("second message"),
		[]byte("third message"),
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		queueingWriter(in, out)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, msg := range messages {
			in <- msg
		}
	}()

	received := make(chan struct{})
	receivedMessages := [][]byte{}
	go func() {
		for msg := range out {
			receivedMessages = append(receivedMessages, msg)
			if len(receivedMessages) == len(messages) {
				close(received)
				return
			}
		}
	}()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for messages")
	}

	assert.DeepEqual(t, messages, receivedMessages)

	// Confirm that it stops by closing the in channel.
	close(in)
	wg.Wait()
	_, ok := <-out
	if ok {
		t.Error("Expected out channel to be closed, but it was still open")
	}
}
