package gearmin

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"sync"
	"time"
)

type event struct {
	cmd       packet           // Command type.
	args      *cmdArgs         // Command arguments.
	result    chan interface{} // Channel used by the server to return data to the worker.
	sessionID int64            // Identifier of the worker session.
}

func newEvent(cmd packet, args *cmdArgs, sessionID int64) *event {
	return &event{
		cmd:       cmd,
		args:      args,
		sessionID: sessionID,
	}
}

func newEventWithResults(cmd packet, args *cmdArgs, sessionID int64) *event {
	event := newEvent(cmd, args, sessionID)
	event.result = make(chan any, 1)
	return event
}

// readJob attempts to receive the job from the server.
func (e *event) readJob(ctx context.Context) *job {
	if e.result == nil {
		return nil
	}

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	select {
	case r := <-e.result:
		if job, ok := r.(*job); ok {
			return job
		}
	case <-timer.C:
	case <-ctx.Done():
	}

	return nil
}

type cmdArgs struct {
	t0 interface{}
	t1 interface{}
}

func decodeArgs(cmd packet, buf []byte) ([][]byte, bool) {
	argc := cmd.ArgCount()
	if argc == 0 {
		return nil, true
	}

	args := make([][]byte, 0, argc)

	if argc == 1 {
		args = append(args, buf)
		return args, true
	}
	endPos := 0
	cnt := 0
	for ; cnt < argc-1 && endPos < len(buf); cnt++ {
		startPos := endPos
		pos := bytes.IndexByte(buf[startPos:], 0x0)
		if pos == -1 {
			return nil, false
		}
		endPos = startPos + pos
		args = append(args, buf[startPos:endPos])
		endPos++
	}
	args = append(args, buf[endPos:]) // option data
	cnt++

	if cnt != argc {
		return nil, false
	}

	return args, true
}

const (
	workerStatusRunning         = 1
	workerStatusSleep           = 2
	workerStatusPrepareForSleep = 3
)

type worker struct {
	id                  string              // Identifier set by the worker (optional).
	sessionID           int64               // Session identifier.
	status              int                 // Worker status.
	inbox               chan []byte         // Used to deliver messages to the worker.
	runningJobsByHandle map[string]*job     // Running jobs by their handles.
	canDo               map[string]struct{} // Known function names.
}

func (w *worker) wakeUp() {
	w.inbox <- wakeUpReply
}

type session struct {
	w *worker
}

func (s *session) getWorker(sessionID int64, inbox chan []byte) *worker {
	if s.w != nil {
		return s.w
	}
	s.w = &worker{
		sessionID:           sessionID,
		status:              workerStatusSleep,
		inbox:               inbox,
		runningJobsByHandle: make(map[string]*job),
		canDo:               make(map[string]struct{}),
	}
	return s.w
}

func (se *session) handleConnection(ctx context.Context, s *Server, conn net.Conn) {
	var (
		wg        sync.WaitGroup
		sessionID = s.allocSessionID()
		inbox     = make(chan []byte, 200)
		outbox    = make(chan []byte, 200)
	)

	defer func() {
		// By closing the inbox channel, we signal to the queueingWriter()
		// function that no more messages will be sent. This causes
		// queueingWriter() to exit its loop and close the outbox channel, which
		// in turn causes the writer() function, which is reading from the
		// outbox channel, to exit its loop. This ensures that both goroutines
		// complete their execution properly.
		close(inbox)

		// Wait for all goroutines to finish their execution before proceeding.
		wg.Wait()

		// Remove session from the server.
		s.handleCloseSession(sessionID)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		queueingWriter(inbox, outbox)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		writer(conn, outbox)
	}()

	r := bufio.NewReaderSize(conn, 256*1024)
	// todo:1. reuse event's result channel, create less garbage.
	// 2. heavily rely on goroutine switch, send reply in EventLoop can make it faster, but logic is not that clean
	// so i am not going to change it right now, maybe never
	fb, err := r.Peek(1)
	if err != nil {
		return
	}
	// Admin connection, we're not going to handle it.
	if fb[0] != byte(0) {
		return
	}
	se.handleBinaryConnection(ctx, s, r, sessionID, inbox)
}

func (se *session) handleBinaryConnection(ctx context.Context, s *Server, r *bufio.Reader, sessionID int64, inbox chan []byte) {
	for {
		pt, buf, err := readMessage(r)
		if err != nil {
			return
		}
		args, ok := decodeArgs(pt, buf)
		if !ok {
			return
		}
		switch pt {
		case packetCanDo, packetCanDoTimeout:
			se.w = se.getWorker(sessionID, inbox)
			s.requests <- &event{cmd: pt, args: &cmdArgs{t0: se.w, t1: string(args[0])}}
		case packetCantDo:
			s.requests <- &event{cmd: pt, sessionID: sessionID, args: &cmdArgs{t0: string(args[0])}}
		case packetEchoReq:
			sendReply(inbox, packetEchoRes, [][]byte{buf})
		case packetPreSleep:
			se.w = se.getWorker(sessionID, inbox)
			s.requests <- &event{cmd: pt, args: &cmdArgs{t0: se.w}, sessionID: sessionID}
		case packetSetClientId:
			se.w = se.getWorker(sessionID, inbox)
			s.requests <- &event{cmd: pt, args: &cmdArgs{t0: se.w, t1: string(args[0])}}
		case packetGrabJobUniq:
			if se.w == nil {
				return
			}
			e := newEventWithResults(pt, nil, sessionID)
			s.requests <- e
			job := e.readJob(ctx)
			if job == nil {
				sendReplyResult(inbox, noJobReply)
				break
			}
			sendReply(inbox, packetJobAssignUniq, [][]byte{
				[]byte(job.Handle), []byte(job.FuncName), []byte(job.ID), job.Data,
			})
		case packetWorkData, packetWorkWarning, packetWorkStatus, packetWorkComplete, packetWorkFail, packetWorkException:
			if se.w == nil {
				return
			}
			s.requests <- &event{
				cmd: pt, args: &cmdArgs{t0: args},
				sessionID: sessionID,
			}
		}
	}
}
