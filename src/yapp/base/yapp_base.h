#ifndef YAPP_BASE_H_
#define YAPP_BASE_H_

#include "config.h"

#include <unistd.h>

#include <iostream>
#include <string>
#include <vector>

namespace yapp {
namespace base {

using std::string;
using std::vector;

const static char * const KEY_VALUE_DELIM = "=";
const static char * const ZK_SRV_DELIM = ",";
const static char * const ZK_SRV_HOST_PORT_DELIM = ":";
const static char * const DEF_CONF_FOLDER = SYSCONFDIR;
const static char * const DEF_CONF_NAME = "yapp.cfg";

const static char * const YAPP_VERBOSE_FLAG = "--verbose";

const static char * const SYS_ROOT_PATH =
  "/prod";
const static char * const YAPP_ROOT_PATH =
  "/prod/yapp";
const static char * const YAPP_CONF_PATH =
  "/prod/yapp/conf";
const static char * const YAPP_CONF_NODES_PATH =
  "/prod/yapp/conf/nodes";
const static char * const YAPP_JOB_PATH =
  "/prod/yapp/jobs";
const static char * const YAPP_QUEUED_TASK_PATH =
  "/prod/yapp/queue";
const static char * const YAPP_QUEUED_RANGE_FILE_TASK_PATH =
  "/prod/yapp/queue/textrfin";
const static char * const YAPP_QUEUED_NEW_TASK_PATH =
  "/prod/yapp/queue/new";
const static char * const YAPP_QUEUED_READY_TASK_PATH =
  "/prod/yapp/queue/ready";
const static char * const YAPP_QUEUED_RUNNING_TASK_PATH =
  "/prod/yapp/queue/running";
const static char * const YAPP_QUEUED_PAUSED_TASK_PATH =
  "/prod/yapp/queue/paused";
const static char * const YAPP_QUEUED_TERMIN_TASK_PATH =
  "/prod/yapp/queue/terminated";
const static char * const YAPP_QUEUED_FAILED_TASK_PATH =
  "/prod/yapp/queue/failed";

const static char * const YAPP_ENV[] = {
  SYS_ROOT_PATH,
  YAPP_ROOT_PATH,
  YAPP_CONF_PATH,
  YAPP_CONF_NODES_PATH,
  YAPP_JOB_PATH,
  YAPP_QUEUED_TASK_PATH,
  YAPP_QUEUED_RANGE_FILE_TASK_PATH,
  YAPP_QUEUED_NEW_TASK_PATH,
  YAPP_QUEUED_READY_TASK_PATH,
  YAPP_QUEUED_RUNNING_TASK_PATH,
  YAPP_QUEUED_PAUSED_TASK_PATH,
  YAPP_QUEUED_TERMIN_TASK_PATH,
  YAPP_QUEUED_FAILED_TASK_PATH,
};

const static char * const YAPP_ROOT_PATH_FOR_TEST =
  "/test/yapp";
const static char * const YAPP_CONF_NODES_PATH_FOR_TEST =
  "/test/yapp/conf/nodes";
const static char * const YAPP_JOB_PATH_FOR_TEST =
  "/test/yapp/jobs";
const static char * const YAPP_QUEUED_TASK_PATH_FOR_TEST =
  "/test/yapp/queue";
const static char * const YAPP_QUEUED_RANGE_FILE_TASK_PATH_FOR_TEST =
  "/test/yapp/queue/textrfin";
const static char * const YAPP_QUEUED_NEW_TASK_PATH_FOR_TEST =
  "/test/yapp/queue/new";
const static char * const YAPP_QUEUED_READY_TASK_PATH_FOR_TEST =
  "/test/yapp/queue/ready";
const static char * const YAPP_QUEUED_RUNNING_TASK_PATH_FOR_TEST =
  "/test/yapp/queue/running";
const static char * const YAPP_QUEUED_PAUSED_TASK_PATH_FOR_TEST =
  "/test/yapp/queue/paused";
const static char * const YAPP_QUEUED_TERMIN_TASK_PATH_FOR_TEST =
  "/test/yapp/queue/terminated";
const static char * const YAPP_QUEUED_FAILED_TASK_PATH_FOR_TEST =
  "/test/yapp/queue/failed";

enum YAPP_MSG_CODE {
  YAPP_MSG_INVALID_ZKC_HNDLE_STATE = -24,
  YAPP_MSG_INVALID_NONODE = -23,

  YAPP_MSG_INVALID_FAILED_RESTARTING_JOBS = -22,
  YAPP_MSG_INVALID_FAILED_TERMINATING_JOBS = -21,
  YAPP_MSG_INVALID_FAILED_RESUMING_JOBS = -20,
  YAPP_MSG_INVALID_FAILED_PAUSING_JOBS = -19,
  YAPP_MSG_INVALID_EXCEED_MAX_BATCH_SIZE = -18,

  YAPP_MSG_CLIENT_ERROR_PAUSING_JOB = -17,
  YAPP_MSG_CLIENT_ERROR_EXCEED_MAX_JOB_CNT = -16,
  YAPP_MSG_CLIENT_ERROR_SUBMITTING_JOB = -15,

  YAPP_MSG_ERROR_ALLOCATING_YAPP_SERV = -14,
  YAPP_MSG_ERROR_JOIN_YAPP_GROUP = -13,
  YAPP_MSG_ERROR_CREATE_JOBNODE = -12,

  YAPP_MSG_INVALID_FAILED_RELEASE_LOCK = -11,
  YAPP_MSG_INVALID_FAILED_GRAB_RLOCK = -10,
  YAPP_MSG_INVALID_FAILED_GRAB_WLOCK = -9,
  YAPP_MSG_INVALID_EXCEED_MAX_TIMEOUT = -8,
  YAPP_MSG_INVALID_THREAD_CREATE = -7,
  YAPP_MSG_INVALID_EXCEED_MAX_JOB_CNT = -6,
  YAPP_MSG_INVALID_SYNC = -5,
  YAPP_MSG_INVALID_ZKCONN = -4,
  YAPP_MSG_INVALID_MASTER_STATUS = -3,
  YAPP_MSG_INVALID_ARGS = -2,
  YAPP_MSG_INVALID = -1,
  YAPP_MSG_SUCCESS = 0,
};

const static char * const YAPP_MSG_ENTRY[] = {
  "SUCCESS",
  "INVALID",
  "Wrong Param List!",
  "No Master Was Elected Online!",
  "Connection Error!",
  "Error Happened When Doing Sync!",
  "Already Reach the Maximum limit of Concurrent Sub-Tasks!",
  "Cannot create new thread for new Sub-Tasks!",
  "Already Reach the Maximum limit of Time-Out!",
  "Failed to Grab the WRITE LocK!",
  "Failed to Grab the READ LocK!",
  "Failed to Release the LocK!",

  "Error Happened When Creating Job Nodes!",
  "Error Happened When Trying to Join Yapp Service Group!(Network Partition?)",
  "Error Happened When Trying to Allocate Resource for Yapp Server",

  "Error Happened When Trying to Submit New Jobs.",
  "Error Happened When Current Job Queue is Too Long.",
  "Error Happened When Trying to Pause Jobs.",

  "Already Reach the Maximum limit of Batch Ops!",
  "Error Happened When Trying to Pause Running Jobs.",
  "Error Happened When Trying to Resume Paused Jobs.",
  "Error Happened When Trying to Terminate Running Jobs.",
  "Error Happened When Trying to Restart Terminated Jobs.",

  "Node Requested Does Not Exist.",
  "Invalid Zk Handle State, either Session Expired or Failed in Auth.",
};

enum YAPP_RUNNING_MODE_CODE {
  YAPP_MODE_INVALID = -1,
  YAPP_MODE_MASTER, YAPP_MODE_WORKER, YAPP_MODE_CLIENT, YAPP_MODE_ADMIN,
  YAPP_MODE_COUNT,
};

/**
 * Desc:
 * - This class will be used to store the information needed for connecting to
 *   single zookeeper server. 
 */
class ZkServer{
public:
  ZkServer(const string & host, const string & port);
  ~ZkServer();
  string get_connection_str();
private:
  string host_str;
  string port_str;
};

class YappBase {
public:
  YappBase(vector<string> args, bool b_verb, bool b_test = false) {
    arg_arr = args;
    b_verbose = b_verb;
    b_testing = b_test;
    def_cfg_fp = def_cfg_fp + DEF_CONF_FOLDER + "/" + DEF_CONF_NAME;
  }
  virtual ~YappBase() {}
  virtual YAPP_MSG_CODE run() = 0;
  virtual YAPP_MSG_CODE parse_arguments() = 0;
  virtual void usage() = 0;

  void print_arg_list() {
    for (size_t i = 0; i < arg_arr.size(); i++) {
      std::cout << "arg " << i << ": " << arg_arr[i] << std::endl;
    }
  }

protected:
  string def_cfg_fp;
  vector<string> arg_arr;
  bool b_verbose;
  bool b_testing;
};

class YappApp {
public:
  const static int ERROR_COMMAND_INVALID = 0;

  YappApp () {
    yapp_obj_ptr = NULL;
    yapp_mode_code = YAPP_MODE_INVALID;
    b_verbose = false;
    def_cfg_fp = def_cfg_fp + DEF_CONF_FOLDER + "/" + DEF_CONF_NAME;
  }
  virtual ~YappApp () { delete yapp_obj_ptr; }

  virtual YAPP_MSG_CODE run(int argc, char * argv[]) = 0;
  virtual YAPP_MSG_CODE parse_args(int argc, char * argv[]) = 0;

protected:
  string def_cfg_fp;
  YappBase * yapp_obj_ptr;
  vector<string> arg_arr;
  bool b_verbose;
  YAPP_RUNNING_MODE_CODE yapp_mode_code;
};

} // namespace base
} // namespace yapp

#endif
