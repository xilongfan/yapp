#ifndef YAPP_SERVICE_UTIL_H_
#define YAPP_SERVICE_UTIL_H_

#include <string>
#include <vector>
#include <list>
#include <sys/types.h>
#include <sys/stat.h>

#include "../base/yapp_base.h"
#include "../util/yapp_util.h"
#include "gen-cpp/yapp_service_types.h"
#include "gen-cpp/yapp_service_constants.h"

namespace yapp {
namespace domain {

using std::string;
using std::vector;
using std::list;

using namespace yapp::util;

extern const yapp_serviceConstants g_yapp_service_constants;

const static string YAPP_TASK_QUEUE_ITEM_FLAG = "P";
const static int YAPP_MASTER_SERV_HNDL_POLICY_ROUNDROBIN = 0;

const static string MAX_LINES_TO_CHECK_STR = "1024";
const static string DEF_LINE_AND_OFFSET_DELIM = ":";
const static int INVALID_TIME_EPOCH = -1;

/** TBD: unit testing **/
class JobDataStrParser {
public:
  /**
   * Desc:
   * - This method will parse the host str. stored in every job node.
   */
  static bool parse_proc_host_str(const string & host_str, string & host_ip,
                                        string & host_prt, string & yapp_pid);
};

class TaskHandleUtil {
public:
  const static int TASK_HANDLE_TYPE_INVALID = -1;
  /** --> job-000000001 **/
  const static int TASK_HANDLE_TYPE_JOBS    = 0;
  /** --> job-000000001_task-00000001 **/
  const static int TASK_HANDLE_TYPE_TASK    = 1;
  /** --> job-000000001_task-00000001_proc-00000001 **/
  const static int TASK_HANDLE_TYPE_PROC    = 2;

  /**
   * Desc:
   * - This method will be used to check the type of a given handle str, whether
   *   it is for an entire job, task or just a single process under a task.
   */
  static int get_task_handle_type(const string & tsk_hndl_str);

  static YAPP_MSG_CODE get_cur_anchor_pos(const string & proc_hndl_str,
                                                string & cur_anchor_str,
                                          ZkClusterProxy * zkc_proxy_ptr)
  {
    YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
    if (proc_hndl_str.empty() || NULL == zkc_proxy_ptr) { return rc; }
    rc = zkc_proxy_ptr->get_node_data(
      zkc_proxy_ptr->get_proc_full_path(proc_hndl_str) + "/anchor_prfx",
      cur_anchor_str
    );
    return rc;
  }

  /**
   * Desc:
   * - This method would automatically check the last few lines of the stdout
   *   for a process specified by the proc_hndl_str, patterns like CUR_LINE=52
   *   By calling system to execute command such as
   *   - tail -n 1024 ./log.out | grep -e "^CUR_LOG_IDX=\([0-9]\)\+$" | sed "s/^CUR_LOG_IDX=//g"
   * Params:
   * - const string & proc_hndl_str: the handle in job node to look at.
   * - ZkClusterProxy * zkc_proxy_ptr: zk handle.
   * Return:
   * - YAPP_MSG_SUCCESS if everything is ok.
   */
  static YAPP_MSG_CODE update_cur_anchor_pos(const string & proc_hndl_str,
                                             ZkClusterProxy * zkc_proxy_ptr,
                                             const string & cur_line,
                                             const string & def_anchor)
  {
    YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
    if (proc_hndl_str.empty() || NULL == zkc_proxy_ptr) { return rc; }
    string anchor_pos, log_file_path, anchor_prfx, job_type;
    string proc_path = zkc_proxy_ptr->get_proc_full_path(proc_hndl_str);
    string task_path = zkc_proxy_ptr->get_task_full_path(
      zkc_proxy_ptr->get_task_hnd_by_proc_hnd(proc_hndl_str)
    );
#ifdef DEBUG_YAPP_SERVICE_UTIL
    std::cerr << "$$$$ PROC PATH: " << proc_path << std::endl;
#endif
    rc = zkc_proxy_ptr->get_node_data(proc_path + "/std_out", log_file_path);
    if (YAPP_MSG_SUCCESS != rc) { return rc; }

#ifdef DEBUG_YAPP_SERVICE_UTIL
    std::cerr << "$$$$ TASK PATH: " << task_path << std::endl;
#endif
    rc = zkc_proxy_ptr->get_node_data(task_path + "/anchor_prfx", anchor_prfx);
    if (YAPP_MSG_SUCCESS != rc) { return rc; }
    rc = zkc_proxy_ptr->get_node_data(task_path + "/input_type", job_type);
    if (YAPP_MSG_SUCCESS != rc) { return rc; }
    string cur_job_epoch = "";
    rc = zkc_proxy_ptr->get_node_data(proc_path + "/last_updated_tmstp_sec", cur_job_epoch);
    long long cur_job_epoch_val = atoll(cur_job_epoch.c_str());
    if (YAPP_MSG_SUCCESS != rc) { return rc; }
#ifdef DEBUG_YAPP_SERVICE_UTIL
    std::cerr << "$$$$ LOG FILE PATH: " << log_file_path << std::endl
              << "$$$$ ANCHOR PREFIX: " << anchor_prfx   << std::endl;
#endif
    string new_line = "", new_anchor = "";
    get_last_pos(
      log_file_path, anchor_prfx, MAX_LINES_TO_CHECK_STR, new_line, new_anchor
    );
    long long new_line_idx = atoll(new_line.c_str());
    if (atoi(job_type.c_str()) == (int)TASK_INPUT_TYPE::DYNAMIC_RANGE) {
      string range_from = "", range_to = "", range_step = "";
      long long range_from_val = 0, range_to_val = 0, range_step_val = 0;
      if ((YAPP_MSG_SUCCESS == (rc = zkc_proxy_ptr->get_node_data(task_path + "/range_from", range_from))) &&
          (YAPP_MSG_SUCCESS == (rc = zkc_proxy_ptr->get_node_data(task_path + "/range_to", range_to))) &&
          (YAPP_MSG_SUCCESS == (rc = zkc_proxy_ptr->get_node_data(task_path + "/range_step", range_step)))) {
        range_from_val = atoll(range_from.c_str());
        range_to_val   = atoll(range_to.c_str());
        range_step_val = atoll(range_step.c_str());
        new_line_idx = (new_line_idx - range_from_val) / range_step_val;
      } else { return rc; }
      if ((new_line_idx >= range_from_val) && (new_line_idx <= range_to_val)) {
        new_anchor = (new_line + DEF_LINE_AND_OFFSET_DELIM + new_anchor);
        new_line = StringUtil::convert_int_to_str(new_line_idx);
      }
    }
    StringUtil::trim_string(new_anchor);
    vector<string> path_arr, data_arr;
    if ((cur_line.empty() && !new_anchor.empty() &&
         (new_anchor != DEF_LINE_AND_OFFSET_DELIM)) ||
        (!cur_line.empty() && new_line == cur_line && !new_anchor.empty() &&
         (new_anchor != DEF_LINE_AND_OFFSET_DELIM))) {
      if (cur_job_epoch_val < TaskHandleUtil::get_last_modified_epoch_in_sec(log_file_path)) {
        path_arr.push_back(proc_path + "/cur_anchor");
        data_arr.push_back(new_anchor);
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        path_arr.push_back(proc_path + "/last_updated_tmstp_sec"); 
        data_arr.push_back(StringUtil::convert_int_to_str(ts.tv_sec));
        rc = zkc_proxy_ptr->batch_set(path_arr, data_arr);
      }
    }

    return rc;
  }

  static int execute_fencing_script_for_host(const string & script_path,
                                             const string & host_ipaddr,
                                                   string & stdout_strm) {
    stdout_strm = "";
    int ret = -1;
    FILE * pipe = popen((script_path + " " + host_ipaddr).c_str(), "r");
    if (NULL == pipe) { return ret; }
    char path_buf[128];
    while (!feof(pipe)) {
      if (NULL != fgets(path_buf, sizeof(path_buf), pipe)) {
        stdout_strm += path_buf;
      }
    }
    ret = pclose(pipe);

#ifdef DEBUG_YAPP_SERVICE_UTIL
    std::cerr << "$$$$ COMMAND:     " << script_path + " " + host_ipaddr << std::endl;
    std::cerr << "$$$$ FENCING RET: " << ret << std::endl;
    std::cerr << "$$$$ STDOUT:      " << stdout_strm << std::endl;
#endif
    return ret;
  }

  static void get_last_pos(const string & log_file_path,
                           const string & anchor_prfx,
                           const string & lines_to_check,
                                 string & cur_line_to_proc,
                                 string & cur_ahor_to_proc) {
    string cur_anchor = "";
    cur_line_to_proc = "", cur_ahor_to_proc = "";

    if (log_file_path.empty() || anchor_prfx.empty() ||
        lines_to_check.empty()) { return; }

    string cmd = "tail -n " + lines_to_check + " " + log_file_path +
                 " | grep -e \"^" + anchor_prfx +
                 "\\([0-9]\\)\\+:[-+]\\?\\([a-zA-Z0-9]\\)\\+$\" | sed \"s/^" + anchor_prfx +
                 "//g\" | tail -1";
#ifdef DEBUG_YAPP_SERVICE_UTIL
    std::cerr << "$$$$ COMMAND LINE: " << cmd << std::endl;
#endif
    FILE * pipe = popen(cmd.c_str(), "r");
    if (NULL == pipe) { return; }
    char path_buf[128];
    while (!feof(pipe)) {
      if (NULL != fgets(path_buf, sizeof(path_buf), pipe)) {
        cur_anchor += path_buf;
      }
    }
    pclose(pipe);

    if (!cur_anchor.empty()) {
      int anchor_delim_pos = cur_anchor.find(DEF_LINE_AND_OFFSET_DELIM);
      cur_line_to_proc = StringUtil::convert_int_to_str(
        atoll(cur_anchor.substr(0, anchor_delim_pos).c_str())
      );
      //cur_ahor_to_proc = StringUtil::convert_int_to_str(
      //  atoll(cur_anchor.substr(anchor_delim_pos + 1).c_str())
      //);
      cur_ahor_to_proc = cur_anchor.substr(anchor_delim_pos + 1);
      StringUtil::trim_string(cur_line_to_proc);
      StringUtil::trim_string(cur_ahor_to_proc);
    }
  }

  static long long get_current_job_anchor_epoch(const string & proc_hndl_str,
                                                ZkClusterProxy * zkc_proxy_ptr) {
    YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
    long long last_modified_epoch = INVALID_TIME_EPOCH;
    string proc_path = zkc_proxy_ptr->get_proc_full_path(proc_hndl_str);
    string job_anchor_epoch;
    rc = zkc_proxy_ptr->get_node_data(proc_path + "/last_updated_tmstp_sec", job_anchor_epoch);
    if (YAPP_MSG_SUCCESS == rc) { last_modified_epoch = atoll(job_anchor_epoch.c_str()); }
    return last_modified_epoch;
  }
 
  static long long get_last_modified_epoch_in_sec(const string & log_file_path) {
    struct stat finfo;
    long long last_modified_epoch = INVALID_TIME_EPOCH;
    if (0 == stat(log_file_path.c_str(), &finfo)) {
      last_modified_epoch = finfo.st_mtime;
    }
    return last_modified_epoch;
  }
};

class RangeFileTaskDataParser {
public:
  static bool is_range_file_task_hnd(const string & proc_hnd) {
    return (string::npos != proc_hnd.find(NXT_LINE_STR));
  }

  /**
   * - Basically a simplified version of parsing the range file task handle str.
   */
  static bool get_running_proc_hnd_in_job_node(const string & proc_hnd_in_queue,
                                               string & proc_hnd_in_job_node);

  static bool get_helper_proc_hnd_in_job_node(const string & proc_hnd_in_queue,
                                              string & helper_proc_hnd);

  static bool parse_rfile_task_data_str(
    const string & task_str, string & tskhd_in_job, string & max_line_idx,
                             string & tskhd_in_run, string & cur_line_idx,
                             string & nxt_line_idx, string & tot_proc_cnt
  );
};

class RangeFileTaskScheduleOp {
public:
  RangeFileTaskScheduleOp(int op_type, const string & path, const string & val) {
    schedule_op_type = op_type; op_str_prev = path; op_str_next = val;
  }
  ~RangeFileTaskScheduleOp() {}
  const static int TASK_SCHEDULE_OP_CREATE = 0;
  const static int TASK_SCHEDULE_OP_DELETE = 1;
  const static int TASK_SCHEDULE_OP_MOVE   = 2;
  /**
   * op will be either create/del/move a node
   * for create, op_str_prev is the path, op_str_next is the value
   * for delete, op_str_prev is the path, op_str_next should be empty
   * for move, op_str_prev is the old path, op_str_next should be new path
   */
  string op_str_prev, op_str_next;
  int schedule_op_type;
};

class RangeFileTaskScheduleList{
public:
  RangeFileTaskScheduleList(int batch_size_limit) {
    max_batch_schedule_ops_size = batch_size_limit;
  }
  ~RangeFileTaskScheduleList() {}
  bool is_schedule_list_exceed_batch_limit() {
    return ((int)task_schedule_ops.size() <= max_batch_schedule_ops_size);
  }
  bool append_task_schedule_op(RangeFileTaskScheduleOp & schedule_op) {
    if (!is_schedule_list_exceed_batch_limit()) { return false; }
    task_schedule_ops.push_back(schedule_op);
    return true;
  }
  vector<RangeFileTaskScheduleOp> task_schedule_ops;
  int max_batch_schedule_ops_size;
};

class YappServerInfo {
public:
  YappServerInfo(string host, int port = YAPP_COMMUNICATION_PORT) {
    host_str = host;
    port_num = port;
  }
  ~YappServerInfo() {}
  string host_str;
  int port_num;
};

class YappWorkerList {
public:
  YappWorkerList(const vector<YappServerInfo> & worker_arr,
                 int flag = YAPP_MASTER_SERV_HNDL_POLICY_ROUNDROBIN);
  ~YappWorkerList();

  bool reset_cur_yapp_worker(const vector<YappServerInfo> & worker_arr,
                             int flag =YAPP_MASTER_SERV_HNDL_POLICY_ROUNDROBIN);
  bool fetch_cur_yapp_worker(YappServerInfo & yw_srv_ret);
  bool shift_cur_yapp_worker();
  int size();
  int remove_and_shift_cur_yapp_worker();

private:
  list<YappServerInfo> yw_list;
  list<YappServerInfo>::iterator cur_itr;
  int fetch_policy;
};

class YappSubtaskQueue {
public:
  YappSubtaskQueue();
  YappSubtaskQueue(const string & qpath);
  ~YappSubtaskQueue();

  void set_queue_path(const string & q_path);

  /**
   * Desc:
   * - For the initial design of Yapp service, we start from having a single
   *   master so that only a lock within the process level is needed in case
   *   there are race condition among threads, and master process will be the
   *   only one to manage that task queue.
   * - These 2 methods would be the wrapper for the lock/unlock. In the future
   *   design, distributed lock will be implemented if we need multi-master.
   */
  int acquire_task_queue_lock();
  int release_task_queue_lock();

  YAPP_MSG_CODE push_back_task_queue(const string & subtask_hnd,
                                     ZkClusterProxy * zkc_proxy_ptr);
  YAPP_MSG_CODE push_back_task_queue(const vector<string> & subtask_hnd_arr,
                                     ZkClusterProxy * zkc_proxy_ptr);
  YAPP_MSG_CODE push_back_and_lock_task(const string & subtask_hnd,
                                        ZkClusterProxy * zkc_proxy_ptr);
  YAPP_MSG_CODE push_back_and_lock_task(const vector<string> & subtask_hnd_arr,
                                        ZkClusterProxy * zkc_proxy_ptr);
  YAPP_MSG_CODE unlock_and_remv_from_task_queue(const vector<string> & tsk_hnds,
                                     ZkClusterProxy * zkc_proxy_ptr);
  YAPP_MSG_CODE unlock_and_remv_from_task_queue(const string & task_hnd_del,
                                     ZkClusterProxy * zkc_proxy_ptr);

  YAPP_MSG_CODE try_acquire_ex_lock(string & lock_hnd_ret,
                                    const string task_hnd,
                                    ZkClusterProxy * zkc_proxy_ptr);

  YAPP_MSG_CODE release_ex_lock(const string & lock_hnd, 
                                ZkClusterProxy * zkc_proxy_ptr);

  bool is_task_existed(const string & task_hnd, ZkClusterProxy * zkc_proxy_ptr);

  YAPP_MSG_CODE get_task_queue(vector<string> & task_queue_ret,
                               ZkClusterProxy * zkc_proxy_ptr);

  YAPP_MSG_CODE mark_task_in_queue(const string & task_hnd,
                                   ZkClusterProxy * zkc_proxy_ptr);
  bool is_task_marked(const string & task_hnd,
                      ZkClusterProxy * zkc_proxy_ptr);

  void decrement_queue_cnt(int dec = 1);
  void increment_queue_cnt(int inc = 1);

  YAPP_MSG_CODE sync_and_get_queue(vector<string> & node_arr,
                                   ZkClusterProxy * zkc_proxy_ptr);

  int size();
  string get_queue_path();

private:
  string queue_path;
  int task_queued_cnt;
  pthread_mutex_t yapp_task_queue_mutex;
};

}
}

#endif
