#ifndef YAPP_MASTER_SERVICE_HANDLER_H_
#define YAPP_MASTER_SERVICE_HANDLER_H_

#include <vector>
#include <cassert>
#include <iostream>
#include <string>
#include <sstream>
#include <pthread.h>
#include <map>
#include <utility>

#include "../util/yapp_util.h"
#include "yapp_domain_factory.h"
#include "yapp_service_util.h"
#include "gen-cpp/YappMasterService.h"

namespace yapp {
namespace domain {

using std::vector;
using std::list;
using std::string;
using std::ostringstream;
using std::map;
using std::pair;

class YappMasterServiceHandler : public YappMasterServiceIf {
  friend class YappServiceHandler;
public:
  YappMasterServiceHandler(
    ZkClusterProxy * zkc_ptr,
    int max_queued_size = YAPP_MASTER_SERV_MAX_QUEUED_TASK_CNT,
    bool b_verb = true, bool b_test = false
  );

  ~YappMasterServiceHandler();

  void set_max_queued_task(int qsz);
  int  get_max_queued_task();

  void set_zkc_proxy_ptr(ZkClusterProxy * zk_ptr) { zkc_proxy_ptr = zk_ptr; }

  /**
   * Desc:
   * - following 14 methods would be the RPC calls that a Yapp Master instance
   *   exposes to outside, admin & submit RPC for cilent, notify for worker.
   */
  void submit_job_rpc(Job & job_obj, const Job & job_to_sub);

  /**
   * Desc:
   * - This method would be fired by a yapp worker instance upon the termination
   *   of a sub-task, and does the following:
   *   1) Update the task status on zookeeper cluster from running to terminated
   *      along with the info of return value and signal id. 
   *   2) check if any sub-task terminated abnormally needs to be restarted.
   */
  bool notify_proc_completion_rpc(const string & proc_handle,
                                  int ret_val, int term_sig);


  /**
   * Desc:
   * - following 12 methods are intended for job management includes {
   *   - pause any process tree for a job, task or running sub_task(proc)
   *   - resume any process tree for a job, task or running sub_task(proc)
   *   - terminate any process tree for a job, task or running sub_task(proc)
   *   - purge any process tree for a job, task or running sub_task(proc)
   *   - print any process tree for a job, task or running sub_task(proc)
   * - All these methods will be based on the recursive api provided in YappUtil
   * NOTE!!!
   * - BE VERY CAREFUL TO USE purge api, WHICH IS INTENDED ONLY FOR CLEAN-UP
   *   WHEN RUNNING THE UNIT-TESTING OR NECESSARY ADMINISTRATION.
   */
  void print_job_tree_rpc(string & tree_str, const string & job_handle);
  void print_task_tree_rpc(string & tree_str, const string & task_handle);
  void print_proc_tree_rpc(string & tree_str, const string & proc_handle);

  void print_queue_stat(string & tree_str, const string & hndl_str);

  void pause_proc_arr_rpc(vector<bool> & ret_arr,
                    const vector<string> & proc_hndl_arr);
  void resume_proc_arr_rpc(vector<bool> & ret_arr,
                     const vector<string> & proc_hndl_arr);
  void terminate_proc_arr_rpc(vector<bool> & ret_arr,
                        const vector<string> & proc_hndl_arr);

  void restart_failed_proc_arr_rpc(vector<bool> & ret_arr,
                             const vector<string> & fail_proc_hndls);

  void fully_restart_proc_arr_rpc(vector<bool> & ret_arr,
                            const vector<string> & term_proc_hndls,
                            const vector<string> & fail_proc_hndls);

  void restart_from_last_anchor_proc_arr_rpc(vector<bool> & ret_arr,
                                       const vector<string> & term_proc_hndls,
                                       const vector<string> & fail_proc_hndls);

  bool purge_job_tree_rpc(const string & job_handle);
protected:

  void fully_restart_proc_arr_in_batch(vector<bool> & ret_arr,
                                 const vector<string>& proc_hndls,
                                 bool b_restart_terminated = true,
                                 bool b_reset_anchor = false);

  void fully_restart_proc_arr_atomic(vector<bool> & ret_arr,
                               const vector<string> & term_proc_hndls,
                               const vector<string> & fail_proc_hndls,
                               bool b_reset_anchor_for_termin_proc = true,
                               bool b_reset_anchor_for_failed_proc = false);

  void restart_failed_proc_arr_atomic(vector<bool> & ret_arr,
                                const vector<string> & fail_proc_hndls);

  bool generate_job_node_initial_value(vector<string> & node_to_set,
                                       vector<string> & data_to_set,
                                       const string & cur_proc_path,
                                       const string & cur_hndl,
                                       bool b_reset_anchor = false);

  bool generate_rftask_hndl_with_initial_value(string & job_node_hndl,
                                               string & new_hndl,
                                               string & cur_hndl,
                                               ZkClusterProxy * zkc_proxy_ptr);

  /**
   * Desc:
   * - This method will be responsible for sending the request to those given
   *   across different worker nodes, notify them to signal the corresponding
   *   running subtasks(processes), SIGSTOP, STGCONT, SIGKILL or SIGTERM
   * Params:
   * - const vector<string>& proc_hndls, the array of proc. hndl to signal
   * - vector<bool> ret_arr, the array of bool value for signaling each proc.
   * - int signal, the actual signal value.
   * Note:
   * - This method is only supposed to be used by the RPCs above.
   */
  void signal_worker_for_proc_arr(const vector<string>& proc_hndls,
                                        vector<bool> & ret_arr, int signal);

public:

  /**
   * Desc:
   * - The following method will do everything needed to initiate a new job,
   *   includes the most basic things like generating a seq. string for the job
   *   and all its tasks with all processes within each task.
   *
   * - This method will do the logging for all the produced sub-tasks upon the
   *   successful return of splitting a main task. Note that the logging would
   *   happen and block until the zookeeper cluster actually finsih the flushing
   *   before any sub-tasks be initiated on any worker node.
   *
   * - This would be used as a WAL for YAPP.
   */
  YAPP_MSG_CODE initiate_new_job(Job & job_obj);

  void query_running_task_basic_info(
    map<string, map<string, vector<string> > > & tsk_grp_arr,
    const vector<string> & host_arr, const vector<string> & owner_arr);

  void query_yappd_envs(map<string, map<string, string> > & host_to_kv_map,
                        const string & host_to_skip);

  /**
   * Desc:
   * - These 2 methods will schedule a set of sub-tasks across any available
   *   worker nodes based on the given policy for scheduling.
   */
  bool schedule_job(const Job & job_obj,
                    int flag = YAPP_MASTER_SERV_HNDL_POLICY_ROUNDROBIN);

  bool schedule_job(const vector<Task> & subtask_arr,
                    YappWorkerList & yw_srv_list,
                    int flag = YAPP_MASTER_SERV_HNDL_POLICY_ROUNDROBIN);

  void split_task(Task & ret_task);

  void split_range_task(Task & ret_task);
  void split_rfile_task(Task & ret_task);
  void split_dyrng_task(Task & task);

  /**
   * TODO:
   * - unit testing
   * Desc:
   * - This method will be used to get the corresponding host str & pid_t for
   *   a given set of running processes indicated by a vector of proc. handles.
   * Params:
   * - const vector<string> & proc_hndl_arr: proc. handle arr for those procs.
   * -       vector<string> & host_strs_arr: where all the host str. returned.
   * -       vector<string> & host_pidt_arr: where all the pidt arr. returned.
   * Note:
   * - Here, every job handle in proc_hndl_arr is supposed to have a handle type
   *   of TASK_HANDLE_TYPE_PROC, which specifically points to a process(subtask)
   */
  bool get_procs_host_and_pid_info(const vector<string> & proc_hndl_arr,
                                         vector<string> & host_strs_arr,
                                         vector<int>    & proc_pidt_arr);
  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - This method is going to group [ $host_str_1, $host_str_1 ... ]
   *                                 [ $proc_hnd_1, $proc_hnd_2 ... ]
   *                                 [ $proc_pid_1, $proc_pid_2 ... ]
   *   into         { $host_str_1 => [ $proc_hnd_1, $proc_hnd_2 ] ... }
   *                { $host_str_1 => [ $proc_pid_1, $proc_pid_2 ] ... }
   * Params:
   * - const vector<string> & host_strs_arr: [ $host_str_1, $host_str_1 ... ]
   * - const vector<string> & proc_hndl_arr: [ $proc_hnd_1, $proc_hnd_2 ... ]
   * - const vector<int>    & proc_pidt_arr: [ $proc_pid_1, $proc_pid_2 ... ]
   * - map<string, vector<string>> host_str_to_hndl_map: where hndl map returned
   * - map<string, vector<int>> host_str_to_pidt_map: where the pid map returned
   * - map<string, vector<int>> host_str_to_idxs_map: store the original idx map
   */
  bool group_procsinfo_by_host(const vector<string> & host_strs_arr,
                               const vector<string> & proc_hndl_arr,
                               const vector<int>    & proc_pidt_arr,
                               map<string, vector<string> > & host_to_hndl_map,
                               map<string, vector<int> >    & host_to_pidt_map,
                               map<string, vector<int> >    & host_to_idxs_map);

  /**
   * TODO:
   * - unit-testing
   * Desc:
   * - This func will return the current array of woker nodes in Yapp Service.
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   */
  YAPP_MSG_CODE get_worker_hosts(vector<YappServerInfo> & yapp_worker_arr);

  /**
   * Desc:
   * - printing functions for testing & debugging usage, would print the main
   *   task string (without partitioning) when i set to -1.
   */
  static void print_task(const Task & task, int i = -1);
  static string get_sub_task_string(const Task & task, int i);
  static string get_task_string(const Task & task);

private:
  YAPP_MSG_CODE sync_newtsk_queue();

  int max_queued_tsk_cnt;
  /** supposed to be immutable, managed by YappServiceHandler **/ 
  ZkClusterProxy * zkc_proxy_ptr;
  bool   b_verbose;
  bool   b_testing;
  YappSubtaskQueue newly_created_task_set;
  // YappSubtaskQueue ready_task_set;
};

}
}
#endif
