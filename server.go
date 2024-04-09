package gearmin

import (
	"container/list"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	ListenAddr string
}

type jobWorkerMap struct {
	workers *list.List
	jobs    *list.List
}

func (m *jobWorkerMap) empty() bool {
	return m.workers.Len() == 0 || m.jobs.Len() == 0
}

func (m *jobWorkerMap) asleep() *worker {
	for it := m.workers.Front(); it != nil; it = it.Next() {
		w := it.Value.(*worker)
		if w.status == workerStatusSleep {
			return w
		}
	}
	return nil
}

// busy reports whether all jobs are busy.
func (m *jobWorkerMap) busy() bool {
	b := true
	for it := m.jobs.Front(); it != nil; it = it.Next() {
		j := it.Value.(*job)
		if !j.Running.Load() {
			b = false
			break
		}
	}
	return b
}

func (m *jobWorkerMap) addWorker(w *worker) {
	for it := m.workers.Front(); it != nil; it = it.Next() {
		if it.Value.(*worker).sessionID == w.sessionID {
			return
		}
	}
	m.workers.PushBack(w)
}

func (m *jobWorkerMap) removeWorker(sessionID int64) {
	for it := m.workers.Front(); it != nil; it = it.Next() {
		if it.Value.(*worker).sessionID == sessionID {
			m.workers.Remove(it)
			return
		}
	}
}

func (m *jobWorkerMap) addJob(j *job) {
	m.jobs.PushBack(j)
}

type Server struct {
	// Configuration parameters.
	config Config

	// Network listener (TCP).
	ln net.Listener

	// Used to deliver requests to the server.
	requests chan *event

	// Workers indexed by their function names and session identifiers.
	workersByFuncName  map[string]*jobWorkerMap
	workersBySessionID map[int64]*worker
	workersMu          sync.RWMutex

	// Jobs indexed by their handles.
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
				s.processRequest(e)
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
	FuncName   string            // Function name.
	ID         string            // Job identifier.
	Data       []byte            // Job data payload.
	Background bool              // Whether this is a background job.
	Callback   JobUpdateCallback // Only used by non-background jobs.
}

// Submit a job and receive its handle.
func (s *Server) Submit(r *JobRequest) string {
	if r == nil {
		return ""
	}
	j := &job{
		Handle:     s.jobHandle(),
		FuncName:   r.FuncName,
		ID:         r.ID,
		Data:       r.Data,
		CreatedAt:  time.Now().UTC(),
		Background: r.Background,
		Callback:   r.Callback,
	}
	s.handleAddJob(j)
	return j.Handle
}

// allocSessionID returns a new session identifier.
func (s *Server) allocSessionID() int64 {
	return atomic.AddInt64(&s.currentSessionID, 1)
}

func (s *Server) processRequest(e *event) {
	args, sessionID := e.args, e.sessionID
	switch e.cmd {
	case packetCanDo, packetCanDoTimeout:
		w := args.t0.(*worker)
		funcName := args.t1.(string)
		s.handleCanDo(funcName, w)
	case packetCantDo:
		funcName := args.t0.(string)
		s.handleCantDo(funcName, sessionID)
	case packetSetClientId:
		w := args.t0.(*worker)
		w.id = args.t1.(string)
	case packetGrabJobUniq:
		e.result <- s.handleGrabJobUniq(sessionID)
	case packetPreSleep:
		w := args.t0.(*worker)
		s.handlePreSleep(w)
	case packetWorkData, packetWorkWarning, packetWorkStatus, packetWorkComplete, packetWorkFail, packetWorkException:
		s.handleWorkReport(e)
	}
}

func (s *Server) handleAddJob(j *job) {
	s.jobsMu.Lock()
	s.jobsByHandle[j.Handle] = j
	s.jobsMu.Unlock()

	s.workersMu.Lock()
	s.jobWorkerPairs(j.FuncName).addJob(j)
	s.wakeUpWorker(j.FuncName)
	s.workersMu.Unlock()
}

func (s *Server) handleCanDo(funcName string, w *worker) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	w.canDo[funcName] = struct{}{}
	s.jobWorkerPairs(funcName).addWorker(w)
	s.workersBySessionID[w.sessionID] = w
}

func (s *Server) handleWorkReport(e *event) {
	slice := e.args.t0.([][]byte)
	jobHandle := string(slice[0])

	j, ok := s.runningJob(jobHandle)
	if !ok {
		return
	}

	jobUpdate := JobUpdate{
		Handle: jobHandle,
	}

	switch e.cmd {
	case packetWorkData:
		jobUpdate.Type = JobUpdateTypeData
		jobUpdate.Data = slice[1]
	case packetWorkWarning:
		jobUpdate.Type = JobUpdateTypeWarning
		jobUpdate.Data = slice[1]
	case packetWorkStatus:
		jobUpdate.Type = JobUpdateTypeStatus
		num, _ := strconv.Atoi(string(slice[1]))
		den, _ := strconv.Atoi(string(slice[2]))
		jobUpdate.Status = [2]int{num, den}
	case packetWorkComplete:
		jobUpdate.Type = JobUpdateTypeComplete
		jobUpdate.Data = slice[1]
		s.removeJob(j)
	case packetWorkFail:
		jobUpdate.Type = JobUpdateTypeFail
		s.removeJob(j)
	case packetWorkException:
		jobUpdate.Type = JobUpdateTypeException
		jobUpdate.Data = slice[1]
		s.removeJob(j)
	}

	// Stop here if the job is detached.
	if j.Background {
		return
	}

	if j.Callback == nil {
		return
	}

	go j.Callback(jobUpdate)
}

func (s *Server) handleCantDo(funcName string, sessionID int64) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	if jw, ok := s.workersByFuncName[funcName]; ok {
		jw.removeWorker(sessionID)
	}
	delete(s.workersBySessionID[sessionID].canDo, funcName)
}

func (s *Server) handleGrabJobUniq(sessionID int64) *job {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
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
	return j
}

func (s *Server) handlePreSleep(w *worker) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	ww, ok := s.workersBySessionID[w.sessionID]
	if !ok {
		s.workersBySessionID[w.sessionID] = w
		return
	}
	ww.status = workerStatusSleep
	for funcName := range w.canDo {
		if s.wakeUpWorker(funcName) {
			break
		}
	}
}

// handleCloseSession removes the worker session given its identifier.
func (s *Server) handleCloseSession(id int64) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	if _, ok := s.workersBySessionID[id]; !ok {
		return
	}
	for _, jw := range s.workersByFuncName {
		jw.removeWorker(id)
	}
	delete(s.workersBySessionID, id)
}

// wakeUpWorker finds and wakes the first sleeping worker for a given function.
func (s *Server) wakeUpWorker(funcName string) bool {
	wj, ok := s.workersByFuncName[funcName]
	if !ok || wj.empty() || wj.busy() {
		return false
	}
	if w := wj.asleep(); w != nil {
		w.wakeUp()
		return true
	}
	return false
}

// runningJob finds a running job by its handle.
func (s *Server) runningJob(handle string) (*job, bool) {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()
	for _, j := range s.jobsByHandle {
		if j.Handle == handle && j.Running.Load() {
			return j, true
		}
	}
	return nil, false
}

func (s *Server) jobWorkerPairs(funcName string) *jobWorkerMap {
	jw, ok := s.workersByFuncName[funcName]
	if !ok {
		jw = &jobWorkerMap{
			workers: list.New(),
			jobs:    list.New(),
		}
		s.workersByFuncName[funcName] = jw
	}

	return jw
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

func (s *Server) removeJob(j *job) {
	s.jobsMu.Lock()
	delete(s.jobsByHandle, j.Handle)
	s.jobsMu.Unlock()
	s.workersMu.Lock()
	if w, ok := s.workersBySessionID[j.ProcessedBy]; ok {
		delete(w.runningJobsByHandle, j.Handle)
	}
	s.workersMu.Unlock()
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
