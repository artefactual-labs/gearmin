package gearmin

import (
	"bufio"
	"bytes"
	"net"
	"sync"
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
	event.result = make(chan interface{}, 1)
	return event
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

func (se *session) handleConnection(s *Server, conn net.Conn) {
	sessionID := s.allocSessionID()
	inbox := make(chan []byte, 200)
	outbox := make(chan []byte, 200)

	var wg sync.WaitGroup

	defer func() {
		wg.Wait()                       // Wait until we're done processing.
		s.handleCloseSession(sessionID) // Remove session from the server.
		close(inbox)                    // Notify writer to quit.
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
	se.handleBinaryConnection(s, r, sessionID, inbox)
}

func (se *session) handleBinaryConnection(s *Server, r *bufio.Reader, sessionID int64, inbox chan []byte) {
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
			job := (<-e.result).(*job)
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
