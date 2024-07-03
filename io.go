package gearmin

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"time"
)

func sendReply(out chan []byte, tp packet, data [][]byte) {
	out <- constructReply(tp, data)
}

func sendReplyResult(out chan []byte, data []byte) {
	out <- data
}

func constructReply(tp packet, data [][]byte) []byte {
	buf := &bytes.Buffer{}
	buf.Write(resStr)

	_ = binary.Write(buf, binary.BigEndian, tp.Uint32())

	length := 0
	for i, arg := range data {
		length += len(arg)
		if i < len(data)-1 {
			length += 1
		}
	}

	_ = binary.Write(buf, binary.BigEndian, uint32(length))

	for i, arg := range data {
		buf.Write(arg)
		if i < len(data)-1 {
			buf.WriteByte(0x00)
		}
	}

	return buf.Bytes()
}

func readMessage(r io.Reader) (packet, []byte, error) {
	_, tp, size, err := readHeader(r)
	if err != nil {
		return tp, nil, err
	}

	if size == 0 {
		return tp, nil, nil
	}

	buf := make([]byte, size)
	_, err = io.ReadFull(r, buf)

	return tp, buf, err
}

func readHeader(r io.Reader) (magic uint32, tp packet, size uint32, err error) {
	magic, err = readUint32(r)
	if err != nil {
		return
	}

	if magic != req && magic != res {
		return
	}

	var cmd uint32
	cmd, err = readUint32(r)
	if err != nil {
		return
	}
	tp, err = newPacket(cmd)
	if err != nil {
		return
	}
	size, _ = readUint32(r)
	return
}

func writer(ctx context.Context, conn net.Conn, outbox chan []byte) {
	defer func() {
		conn.Close()

		// Drain outbox in case reader is blocked.
		for range outbox {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	b := bytes.NewBuffer(nil)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-outbox:
			if !ok {
				return
			}
			b.Write(msg)
			for n := len(outbox); n > 0; n-- {
				select {
				case msg := <-outbox:
					b.Write(msg)
				case <-ctx.Done():
					return
				default:
					n = 0
				}
			}

			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_, err := conn.Write(b.Bytes())
			if err != nil {
				return
			}

			b.Reset()
		}
	}
}

// queueingWriter transfers messages from the in channel to the out channel,
// maintaining order using an in-memory queue. It stops when in is closed and
// discards any remaining messages in the queue, then closes the out channel.
func queueingWriter(in, out chan []byte) {
	queue := make(map[int][]byte)
	head, tail := 0, 0
L:
	for {
		if head == tail {
			m, ok := <-in
			if !ok {
				break L
			}
			queue[head] = m
			head++
		} else {
			select {
			case m, ok := <-in:
				if !ok {
					break L
				}
				queue[head] = m
				head++
			case out <- queue[tail]:
				delete(queue, tail)
				tail++
			}
		}
	}

	// We throw away any messages waiting to be sent, including the
	// nil message that is automatically sent when the in channel is closed
	close(out)
}

func readUint32(r io.Reader) (uint32, error) {
	var value uint32
	err := binary.Read(r, binary.BigEndian, &value)
	return value, err
}

// ctxReader wraps an [io.Reader] to handle context cancellation. The state of
// the context is checked before very read.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *ctxReader) Read(p []byte) (n int, err error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.r.Read(p)
	}
}
