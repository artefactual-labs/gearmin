// nolint: unused
package gearmin

import (
	"fmt"
)

// https://github.com/gearman/gearmand/blob/master/PROTOCOL

// Magic codes (numeric representations).
const (
	req = 5391697
	res = 5391699
)

// Magic codes (byte sequences).
var (
	reqStr = []byte("\x00REQ")
	resStr = []byte("\x00RES")
)

// Pre-built responses.
var (
	wakeUpReply = []byte{0, 82, 69, 83, 0, 0, 0, 6, 0, 0, 0, 0}  // constructReply(PT_Noop, nil)
	noJobReply  = []byte{0, 82, 69, 83, 0, 0, 0, 10, 0, 0, 0, 0} // constructReply(PT_NoJob, nil)
)

type packet uint32

const (
	packetCanDo          packet = iota + 1 //   1            REQ    Worker
	packetCantDo                           //   REQ    Worker
	packetResetAbilities                   //   REQ    Worker
	packetPreSleep                         //   REQ    Worker
	_
	packetNoop       //   RES    Worker
	packetSubmitJob  //   REQ    Client
	packetJobCreated //   RES    Client
	packetGrabJob    //   REQ    Worker
	packetNoJob      //   RES    Worker
	packetJobAssign  //   RES    Worker
	packetWorkStatus //   REQ    Worker

	//     RES    Client
	packetWorkComplete //   REQ    Worker

	//    RES    Client
	packetWorkFail //  REQ    Worker

	//    RES    Client
	packetGetStatus //   REQ    Client

	packetEchoReq       //  REQ    Client/Worker
	packetEchoRes       //  RES    Client/Worker
	packetSubmitJobBG   //   REQ    Client
	packetError         //   RES    Client/Worker
	packetStatusRes     //   RES    Client
	packetSubmitJobHigh //   REQ    Client
	packetSetClientId   //  REQ    Worker
	packetCanDoTimeout  //   REQ    Worker
	packetAllYours      //   REQ    Worker
	packetWorkException //   REQ    Worker

	//     RES    Client
	packetOptionReq //   REQ    Client/Worker

	packetOptionRes //   RES    Client/Worker
	packetWorkData  //   REQ    Worker

	//    RES    Client
	packetWorkWarning //  REQ    Worker

	//    RES    Client
	packetGrabJobUniq //   REQ    Worker

	packetJobAssignUniq   //   RES    Worker
	packetSubmitJobHighBG //  REQ    Client
	packetSubmitJobLow    //  REQ    Client
	packetSubmitJobLowBG  //  REQ    Client
	packetSubmitJobSched  //  REQ    Client
	packetSubmitJobEpoch  //   36 REQ    Client

	// New Codes (ref: https://github.com/gearman/gearmand/commit/eabf8a01030a16c80bada1e06c4162f8d129a5e8)
	packetSubmitReduceJob           // REQ    Client
	packetSubmitReduceJobBackground // REQ    Client
	packetGrabJobAll                // REQ    Worker
	packetJobAssignAll              // RES    Worker
	packetGetStatusUnique           // REQ    Client
	packetStatusResUnique           // RES    Client
)

var labels = map[packet]string{
	packetCanDo:                     "CAN_DO",
	packetCantDo:                    "CANT_DO",
	packetResetAbilities:            "RESET_ABILITIES",
	packetPreSleep:                  "PRE_SLEEP",
	packetNoop:                      "NOOP",
	packetSubmitJob:                 "SUBMIT_JOB",
	packetJobCreated:                "JOB_CREATED",
	packetGrabJob:                   "GRAB_JOB",
	packetNoJob:                     "NO_JOB",
	packetJobAssign:                 "JOB_ASSIGN",
	packetWorkStatus:                "WORK_STATUS",
	packetWorkComplete:              "WORK_COMPLETE",
	packetWorkFail:                  "WORK_FAIL",
	packetGetStatus:                 "GET_STATUS",
	packetEchoReq:                   "ECHO_REQ",
	packetEchoRes:                   "ECHO_RES",
	packetSubmitJobBG:               "SUBMIT_JOB_BG",
	packetError:                     "ERROR",
	packetStatusRes:                 "STATUS_RES",
	packetSubmitJobHigh:             "SUBMIT_JOB_HIGH",
	packetSetClientId:               "SET_CLIENT_ID",
	packetCanDoTimeout:              "CAN_DO_TIMEOUT",
	packetAllYours:                  "ALL_YOURS",
	packetWorkException:             "WORK_EXCEPTION",
	packetOptionReq:                 "OPTION_REQ",
	packetOptionRes:                 "OPTION_RES",
	packetWorkData:                  "WORK_DATA",
	packetWorkWarning:               "WORK_WARNING",
	packetGrabJobUniq:               "GRAB_JOB_UNIQ",
	packetJobAssignUniq:             "JOB_ASSIGN_UNIQ",
	packetSubmitJobHighBG:           "SUBMIT_JOB_HIGH_BG",
	packetSubmitJobLow:              "SUBMIT_JOB_LOW",
	packetSubmitJobLowBG:            "SUBMIT_JOB_LOW_BG",
	packetSubmitJobSched:            "SUBMIT_JOB_SCHED",
	packetSubmitJobEpoch:            "SUBMIT_JOB_EPOCH",
	packetSubmitReduceJob:           "SUBMIT_REDUCE_JOB",
	packetSubmitReduceJobBackground: "SUBMIT_REDUCE_JOB_BG",
	packetGrabJobAll:                "GRAB_JOB_ALL",
	packetJobAssignAll:              "JOB_ASSIGN_ALL",
	packetGetStatusUnique:           "GET_STATUS_UNIQ",
	packetStatusResUnique:           "STATUS_RES_UNIQ",
}

func (i packet) Uint32() uint32 {
	return uint32(i)
}

func (i packet) label() string {
	return labels[i]
}

func newPacket(cmd uint32) (packet, error) {
	if cmd >= packetCanDo.Uint32() && cmd <= packetSubmitJobEpoch.Uint32() {
		return packet(cmd), nil
	}
	if cmd >= packetSubmitReduceJob.Uint32() && cmd <= packetStatusResUnique.Uint32() {
		return packet(cmd), fmt.Errorf("unsupported packet type %v", cmd)
	}
	return packet(cmd), fmt.Errorf("invalid packet type %v", cmd)
}

var argc = []int{
	/* UNUSED" */ 0,
	/* CAN_DO */ 1,
	/* CANT_DO */ 1,
	/* RESET_ABILITIES */ 0,
	/* PRE_SLEEP */ 0,
	/* UNUSED */ 0,
	/* NOOP */ 0,
	/* SUBMIT_JOB */ 3,
	/* JOB_CREATED */ 1,
	/* GRAB_JOB */ 0,
	/* NO_JOB */ 0,
	/* JOB_ASSIGN */ 3,
	/* WORK_STATUS */ 3,
	/* WORK_COMPLETE */ 2,
	/* WORK_FAIL */ 1,
	/* GET_STATUS */ 1, // different from libgearman.cc
	/* ECHO_REQ */ 1,
	/* ECHO_RES */ 1,
	/* SUBMIT_JOB_BG */ 3,
	/* ERROR */ 2,
	/* STATUS_RES */ 5,
	/* SUBMIT_JOB_HIGH */ 3,
	/* SET_CLIENT_ID */ 1,
	/* CAN_DO_TIMEOUT */ 2,
	/* ALL_YOURS */ 0,
	/* WORK_EXCEPTION */ 2,
	/* OPTION_REQ */ 1,
	/* OPTION_RES */ 1,
	/* WORK_DATA */ 2,
	/* WORK_WARNING */ 2,
	/* GRAB_JOB_UNIQ */ 0,
	/* JOB_ASSIGN_UNIQ */ 4,
	/* SUBMIT_JOB_HIGH_BG */ 3,
	/* SUBMIT_JOB_LOW */ 3,
	/* SUBMIT_JOB_LOW_BG */ 3,
	/* SUBMIT_JOB_SCHED */ 8,
	/* SUBMIT_JOB_EPOCH */ 4,
}

func (i packet) ArgCount() int {
	switch {
	case 1 <= i && i <= 36:
		return argc[i]
	default:
		return 0
	}
}
