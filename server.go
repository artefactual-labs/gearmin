package gearmin

import (
	"container/list"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type JobCallback func(err error)

type Config struct {
	ListenAddr string
}

type jobWorkerMap struct {
	workers *list.List
	jobs    *list.List
}

type Server struct {
	// Configuration parameters.
	config Config

	// Network listener (TCP).
	ln net.Listener

	// Used to deliver requests to the server.
	requests chan *event

	// Map of workers indexed by their function names.
	workersByFuncName map[string]*jobWorkerMap

	// Map of workers indexed by their session identifiers.
	workersBySessionID map[int64]*worker

	// Map of jobs indexed by their handles.
	jobsByHandle map[string]*job
	jobsMu       sync.RWMutex

	// Used to generate consecutive session identifiers.
	currentSessionID int64

	// Used to generate job handles.
	jobHandleGenerator

	// Used to coordinate graceful shutdown.
	quitOnce sync.Once
	quit     chan struct{}
	wg       sync.WaitGroup
	running  bool
}

func NewServer(cfg Config) *Server {
	return &Server{
		config:             cfg,
		requests:           make(chan *event, 100),
		workersByFuncName:  make(map[string]*jobWorkerMap),
		workersBySessionID: make(map[int64]*worker),
		jobsByHandle:       make(map[string]*job),
		quit:               make(chan struct{}),
	}
}

func (s *Server) Start() error {
	addr := s.config.ListenAddr
	if addr == "" {
		addr = ":4730"
	}

	var err error
	s.ln, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case e := <-s.requests:
				s.handleRequest(e)
			case <-s.quit:
				return
			}
		}
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.ln.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
				}
				continue
			}

			session := &session{}
			go session.handleConnection(s, conn)
		}
	}()

	s.running = true

	return nil
}

type JobRequest struct {
	FuncName   string      // Function name.
	ID         string      // Job identifier.
	Data       []byte      // Job data payload.
	Background bool        // Whether this is a background job.
	Callback   JobCallback // Only used by non-background jobs.
}

// Submit a job and receive its handle.
func (s *Server) Submit(r *JobRequest) string {
	j := &job{
		Handle:     s.jobHandle(),
		FuncName:   r.FuncName,
		ID:         r.ID,
		Data:       r.Data,
		CreatedAt:  time.Now().UTC(),
		Background: r.Background,
		Callback:   r.Callback,
	}
	s.doAddJob(j)
	return j.Handle
}

func (s *Server) addWorker(l *list.List, w *worker) {
	for it := l.Front(); it != nil; it = it.Next() {
		if it.Value.(*worker).SessionID == w.SessionID {
			return
		}
	}

	l.PushBack(w) // add to worker list
}

func (s *Server) removeWorker(l *list.List, sessionID int64) {
	for it := l.Front(); it != nil; it = it.Next() {
		if it.Value.(*worker).SessionID == sessionID {
			l.Remove(it)
			return
		}
	}
}

func (s *Server) handleCanDo(funcName string, w *worker) {
	w.canDo[funcName] = struct{}{}
	jw := s.getJobWorkPair(funcName)
	s.addWorker(jw.workers, w)
	s.workersBySessionID[w.SessionID] = w
}

func (s *Server) getJobWorkPair(funcName string) *jobWorkerMap {
	jw, ok := s.workersByFuncName[funcName]
	if !ok { // create list
		jw = &jobWorkerMap{workers: list.New(), jobs: list.New()}
		s.workersByFuncName[funcName] = jw
	}

	return jw
}

func (s *Server) add2JobWorkerQueue(j *job) {
	jw := s.getJobWorkPair(j.FuncName)
	jw.jobs.PushBack(j)
}

func (s *Server) doAddJob(j *job) {
	s.jobsMu.Lock()
	s.jobsByHandle[j.Handle] = j
	s.jobsMu.Unlock()
	s.add2JobWorkerQueue(j)
	s.wakeupWorker(j.FuncName)
}

func (s *Server) popJob(sessionID int64) (j *job) {
	for funcName := range s.workersBySessionID[sessionID].canDo {
		if wj, ok := s.workersByFuncName[funcName]; ok {
			for it := wj.jobs.Front(); it != nil; it = it.Next() {
				jtmp := it.Value.(*job)
				if jtmp.Running.Load() {
					continue
				}
				j = jtmp
				wj.jobs.Remove(it)
				return
			}
		}
	}
	return
}

func (s *Server) wakeupWorker(funcName string) bool {
	wj, ok := s.workersByFuncName[funcName]
	if !ok || wj.jobs.Len() == 0 || wj.workers.Len() == 0 {
		return false
	}
	// Don't wakeup for running job
	allRunning := true
	for it := wj.jobs.Front(); it != nil; it = it.Next() {
		j := it.Value.(*job)
		if !j.Running.Load() {
			allRunning = false
			break
		}
	}
	if allRunning {
		return false
	}
	for it := wj.workers.Front(); it != nil; it = it.Next() {
		w := it.Value.(*worker)
		if w.status != workerStatusSleep {
			continue
		}
		w.in <- wakeUpReply
		return true
	}

	return false
}

func (s *Server) removeJob(j *job) {
	s.jobsMu.Lock()
	delete(s.jobsByHandle, j.Handle)
	s.jobsMu.Unlock()
	if pw, found := s.workersBySessionID[j.ProcessedBy]; found {
		delete(pw.runningJobsByHandle, j.Handle)
	}
}

func (s *Server) jobDone(j *job) {
	s.removeJob(j)
}

func (s *Server) jobFailed(j *job) {
	s.removeJob(j)
}

func (s *Server) jobFailedWithException(j *job) {
	s.removeJob(j)
}

func (s *Server) handleWorkReport(e *event) {
	slice := e.args.t0.([][]byte)
	jobHandle := string(slice[0])
	j, ok := s.getRunningJobByHandle(jobHandle)
	if !ok {
		return
	}

	var reportErr error
	switch e.cmd {
	case packetWorkStatus:
		j.Percent, _ = strconv.Atoi(string(slice[1]))
		j.Denominator, _ = strconv.Atoi(string(slice[2]))
	case packetWorkException:
		reportErr = errors.New("WORK_EXCEPTION")
		s.jobFailedWithException(j)
	case packetWorkFail:
		reportErr = errors.New("WORK_FAIL")
		s.jobFailed(j)
	case packetWorkComplete:
		s.jobDone(j)
	}

	// Stop here if the job is detached.
	if j.Background {
		return
	}

	if j.Callback == nil {
		return
	}

	go j.Callback(reportErr)
}

func (s *Server) handleRequest(e *event) {
	args, sessionID := e.args, e.sessionID
	switch e.cmd {
	case packetCanDo, packetCanDoTimeout:
		w := args.t0.(*worker)
		funcName := args.t1.(string)
		s.handleCanDo(funcName, w)
	case packetCantDo:
		funcName := args.t0.(string)
		if jw, ok := s.workersByFuncName[funcName]; ok {
			s.removeWorker(jw.workers, sessionID)
		}
		delete(s.workersBySessionID[sessionID].canDo, funcName)
	case packetSetClientId:
		w := args.t0.(*worker)
		w.id = args.t1.(string)
	case packetGrabJobUniq:
		var j *job
		if w, ok := s.workersBySessionID[sessionID]; ok {
			w.status = workerStatusRunning
			j = s.popJob(sessionID)
			if j != nil {
				j.ProcessedAt = time.Now()
				j.ProcessedBy = sessionID
				j.Running.Store(true)
				w.runningJobsByHandle[j.Handle] = j
			} else {
				w.status = workerStatusPrepareForSleep
			}
		}
		e.result <- j
	case packetPreSleep:
		w, ok := s.workersBySessionID[sessionID]
		if !ok {
			w = args.t0.(*worker)
			s.workersBySessionID[w.SessionID] = w
			break
		}
		w.status = workerStatusSleep
		for k := range w.canDo {
			if s.wakeupWorker(k) {
				break
			}
		}
	case packetWorkData, packetWorkWarning, packetWorkStatus, packetWorkComplete, packetWorkFail, packetWorkException:
		s.handleWorkReport(e)
	}
}

func (s *Server) allocSessionID() int64 {
	return atomic.AddInt64(&s.currentSessionID, 1)
}

func (s *Server) getRunningJobByHandle(handle string) (*job, bool) {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()
	for _, j := range s.jobsByHandle {
		if j.Handle == handle && j.Running.Load() {
			return j, true
		}
	}
	return nil, false
}

// closeSession removes the worker session given its identifier.
func (s *Server) closeSession(id int64) {
	if _, ok := s.workersBySessionID[id]; !ok {
		return
	}
	for _, jw := range s.workersByFuncName {
		s.removeWorker(jw.workers, id)
	}
	delete(s.workersBySessionID, id)
}

func (s *Server) Stop() {
	if s == nil {
		return
	}
	s.quitOnce.Do(func() {
		if !s.running {
			return
		}
		close(s.quit)
		s.ln.Close()
		s.wg.Wait()
	})
}
