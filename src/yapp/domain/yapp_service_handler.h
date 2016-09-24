#ifndef YAPP_SERVICE_HANDLER_H_
#define YAPP_SERVICE_HANDLER_H_
/**
 * Desc:
 * - This is actually a interface which combines both YappWorkerServiceHandler
 *   and YappMasterServiceHandler, this would enable the automaitc failover
 *   for the entire yapp system.
 */
#include <climits>
#include <string>
#include <vector>
#include <cerrno>
#include <unistd.h>
#include <pthread.h>

#include "yapp_master_service_handler.h"
#include "yapp_worker_service_handler.h"
#include "gen-cpp/YappService.h"
#include "../util/yapp_util.h"

namespace yapp {
namespace domain {

using std::vector;
using std::string;

using namespace yapp::util;

const static int YAPPD_EXIT_MAX_RETRY = 5;
const static int YAPPD_EXIT_RETRY_WAT = 2;

const static pthread_t YAPP_SERVICE_THREAD_ID_INVALID = INT_MAX;

class YappServiceHandler : public YappServiceIf {
public:
  YappServiceHandler(const ConfigureUtil & cfg_obj, bool b_master = false,
                                bool b_verb = true, bool b_test = false,
                                int  shmid  = -1,   int  shmsz = 0) :
    yapp_master_srv_obj(NULL, cfg_obj.max_queued_task, b_verb, b_test),
    yapp_worker_srv_obj(NULL, cfg_obj.thrd_pool_size, b_verb, b_test)
  {
    zkc_proxy_ptr = new ZkClusterProxy(
      b_test, cfg_obj.port_num, cfg_obj.max_queued_task,
      cfg_obj.max_zkc_timeout, cfg_obj.yappd_root_path
    );
    b_master_preferred = b_master;
    zk_conn_str = cfg_obj.zk_conn_str;
    yapp_master_srv_obj.set_zkc_proxy_ptr(zkc_proxy_ptr);
    yapp_worker_srv_obj.set_zkc_proxy_ptr(zkc_proxy_ptr);

    reschedule_thrd_id  = YAPP_SERVICE_THREAD_ID_INVALID;
    checktasks_thrd_id  = YAPP_SERVICE_THREAD_ID_INVALID;
    schedule_rfthrd_id  = YAPP_SERVICE_THREAD_ID_INVALID;
    split_rftk_thrd_id  = YAPP_SERVICE_THREAD_ID_INVALID;
    check_util_thrd_id  = YAPP_SERVICE_THREAD_ID_INVALID;
    node_lvchk_thrd_id  = YAPP_SERVICE_THREAD_ID_INVALID;
    check_point_thrd_id = YAPP_SERVICE_THREAD_ID_INVALID;

    event_hndl_thrd_id = YAPP_SERVICE_THREAD_ID_INVALID;

    rftask_scheule_polling_rate_sec = cfg_obj.rftask_scheule_polling_rate_sec;
    subtask_scheule_polling_rate_sec = cfg_obj.subtask_scheule_polling_rate_sec;
    zombie_check_polling_rate_sec = cfg_obj.zombie_check_polling_rate_sec;
    rftask_autosp_polling_rate_sec = cfg_obj.rftask_autosp_polling_rate_sec;
    utility_thread_checking_rate_sec = cfg_obj.utility_thread_checking_rate_sec;
    node_leave_check_polling_rate_sec = cfg_obj.master_check_polling_rate_sec;
    check_point_polling_rate_sec = cfg_obj.check_point_polling_rate_sec; 

    batch_task_schedule_limit = cfg_obj.batch_task_schedule_limit;

    fencing_script_path = cfg_obj.fencing_script_path;

    shrd_mem_id   = shmid;
    shrd_mem_sz   = shmsz;
    b_flag_exit   = false;
    b_flag_thrd_stop = false;

    ypconf_obj = cfg_obj;
  }

  ~YappServiceHandler() {
    delete zkc_proxy_ptr;
  }

  bool reload_conf_from_file() {
    return ypconf_obj.load_zk_cluster_cfg(
      string("") + DEF_CONF_FOLDER + "/" + DEF_CONF_NAME
    );
  }

  bool apply_dynamic_conf_atomic();

  bool join_yapp_nodes_group();

  bool start_yapp_event_handler();

private:

  static bool check_schedule_batch_limit(
    vector<string> & new_rf_subtsk_arr, int batch_limit
  );

  static YAPP_MSG_CODE set_create_and_delete_in_small_batch(
    const vector<string> & upd_path_arr, const vector<string> & upd_data_arr,
    const vector<string> & path_to_cr_arr, const vector<string> & data_to_cr_arr,
    const vector<string> & path_to_del, YappServiceHandler * ym_srv_ptr
  );

  static void commit_range_file_task_schedule_plan(
    YappServiceHandler * ym_srv_ptr,
    YappSubtaskQueue & rf_subtask_queue, YappSubtaskQueue & rf_running_subtask_queue,
    list<string> & subtsk_list_before_change, list<string> & subtsk_list_after_change,
    list<string> & fin_rf_subtsk_full_hnd_arr, vector<string> & new_rf_subtsk_arr
  );

  static bool check_and_schedule_to_help_running_processes( 
    vector<string> & new_rf_subtsk_arr, vector<int> & new_rf_subtsk_idx_arr,
    list<string> & subtsk_list_before_change, list<string> & subtsk_list_after_change,
    list<string> & fin_rf_subtsk_arr, list<string> & fin_rf_subtsk_full_hnd_arr,
    vector<string> & rf_subtask_arr, vector<int> & rf_running_subtsk_idx_arr,
    YappSubtaskQueue & rf_subtask_queue, int batch_limit
  );

  static bool check_and_schedule_to_help_terminated_processes( 
    vector<string> & new_rf_subtsk_arr, vector<int> & new_rf_subtsk_idx_arr,
    list<string> & subtsk_list_before_change, list<string> & subtsk_list_after_change,
    list<string> & fin_rf_subtsk_arr, list<string> & fin_rf_subtsk_full_hnd_arr,
    vector<string> & rf_subtask_arr, YappSubtaskQueue & rf_subtask_queue, int batch_limit
  );

  static bool check_and_schedule_jobs_directly_owned_by_each_process(
    vector<string> & rf_subtask_arr, vector<string> & terminated_subtsk_arr,
    vector<string> & new_rf_subtsk_arr, vector<int> & new_rf_subtsk_idx_arr,
    vector<int> & terminated_subtsk_idx_arr,
    list<string> & fin_rf_subtsk_arr, list<string> & fin_rf_subtsk_full_hnd_arr,
    list<string> & subtsk_list_before_change, list<string> & subtsk_list_after_change,
    YappSubtaskQueue & rf_subtask_queue, int batch_limit
  );

  static void check_and_clear_free_processes(
    vector<string> & new_rf_subtsk_arr,  vector<string> & rf_failed_subtsk_arr,
    vector<string> & rf_running_subtsk_arr, vector<string> & rf_subtask_arr,
    list<string> & fin_rf_subtsk_full_hnd_arr, YappSubtaskQueue & rf_subtask_queue,
    YappServiceHandler * ym_srv_ptr
  );

  static void find_all_current_index_for_running_and_terminated_processes(
    vector<string> & rf_subtask_arr,        vector<string> & rf_running_subtsk_arr,
    vector<string> & terminated_subtsk_arr, vector<int> & rf_running_subtsk_idx_arr,
                                            vector<int> & terminated_subtsk_idx_arr
  );

  static bool fetch_queue_and_filter_failed_rftasks(
    YappServiceHandler * ym_srv_ptr,
    vector<string> & rf_running_subtsk_arr, vector<string> & rf_subtask_arr,
    vector<string> & rf_failed_subtsk_arr, YappSubtaskQueue & rf_running_subtask_queue,
    YappSubtaskQueue & rf_subtask_queue, YappSubtaskQueue & rf_failed_task_queue
  );

  static void setup_rftasks_queue_path(YappServiceHandler * ym_srv_ptr,
                                       YappSubtaskQueue & rf_running_subtask_queue,
                                       YappSubtaskQueue & rf_subtask_queue,
                                       YappSubtaskQueue & rf_failed_task_queue,
                                       const string & rf_task_to_schedule);
  /**
   * Thread to periodically check if any subtask tasking range file as its input
   * needs more proc.s to run(due to the restart due to failure while all other
   * processes already finished their chunk).
   */
  static void * thread_auto_split_rfile_tasks(void * ym_srv_ptr);
  static bool auto_split_rfile_tasks(YappServiceHandler * ym_srv_ptr);

  /**
   * Thread to periodically initialize new subtasks and enqueue them into the
   * NEW queue for those jobs using file as its range input.
   */
  static void * thread_schedule_rfile_tasks(void * ym_srv_ptr);
  static bool schedule_rfile_tasks(YappServiceHandler * ym_srv_ptr);
  static bool schedule_rfile_tasks_in_batch(YappServiceHandler * ym_srv_ptr);

  /** thread to schedule queued subtasks when instance runs as a master. **/
  static void * thread_reschedule_tasks(void * ym_srv_ptr);
  static bool reschedule_tasks(YappServiceHandler * ym_srv_ptr);

  /**
   * Desc:
   * - These 2 methods will be the thread periodically perform the check if any
   *   queued task dies in the middle. It is possible that some nodes be offline
   *   and all running tasks(either READY OR RUNNING) will be terminated without
   *   notification. But there will be an entry in either ready or running queue
   *   for each of them, and those entries are not lock protected(nodes offline)
   *
   * - The thread will 1st try to acquire an EX lock for each node in the queue,
   *   for any node successfully latched, it will places them back to NEW queue
   *   for later re-scheduling.
   *
   * - Any task in READY or RUNNING queue without lock protection would be treat
   *   as zombie and will be removed and re-queued in the NEW queue.
   */
  static void * thread_check_zombie_task(void * srv_ptr);
  static bool check_zombie_task(YappServiceHandler * ym_srv_ptr);
  static bool check_zombie_task(YappServiceHandler * ym_srv_ptrm,
                                YappSubtaskQueue & task_queue_proxy);

  /**
   * Thread to periodically check if all scheduling related threads are working
   * well(especially for master instance), this is for fixing the problem of
   * 'ZOMBIE' master due to the unexpected termination of scheduling threads
   * in the presence of massive nodes failure. For example, the main process is
   * still holding the lease while its scheduling threads all terminated, such
   * that no new jobs could be scheduled and in the mean time mastership cannot
   * be transferred to other nodes.
   */
  static void * thread_check_utility_thread_status(void * srv_ptr);
  static bool check_utility_thread_status(YappServiceHandler * ym_srv_ptr);

  /** thread to detect master's change for yapp service, will runs on both **/
  static void * thread_node_leave_check(void * ym_srv_ptr);
  static bool node_leave_check(YappServiceHandler * ym_srv_ptr);

  /** thread to checkpoint the running processes every few secs, will runs on both **/
  static void * thread_check_point_cur_anchor(void * ym_srv_ptr);
  static void check_point_cur_anchor(YappServiceHandler * ym_srv_ptr);

  /** thread dedicated for handling events(signals), notified by a cond. var*/
  static void * thread_event_handler(void * ym_srv_ptr);

  static bool is_subtask_finished_all_its_own_chunk(const string & task_hndl);

  void stop_all_master_threads();
  int  start_all_master_threads();

  /** if b_incl_step_down is true, returns true if b_master_step_down is true */
  // bool check_exit_signal(bool b_incl_step_down = true);
  bool set_bflag_atomic(bool * flag_ptr, bool val);
  bool chk_bflag_atomic(bool * flag_ptr);
  bool get_intvl_atomic(int * ptr, int * ret);

  YappMasterServiceHandler & get_master_srv_ref(){ return yapp_master_srv_obj; }

  YappMasterServiceHandler yapp_master_srv_obj;
  YappWorkerServiceHandler yapp_worker_srv_obj;
  /**
   * - This variable is to mark if a certain process is preferred to win the
   *   election when other processes have no such priority sat, otherwise it
   *   depends on the sequence id given by zookeeper cluster for tie-breaking.
   */ 
  bool b_master_preferred;
  ZkClusterProxy * zkc_proxy_ptr;
  string zk_conn_str;

  int rftask_scheule_polling_rate_sec;
  int subtask_scheule_polling_rate_sec;
  int zombie_check_polling_rate_sec;
  int rftask_autosp_polling_rate_sec;
  int utility_thread_checking_rate_sec;
  int node_leave_check_polling_rate_sec;
  int check_point_polling_rate_sec;

  int batch_task_schedule_limit;

  /**
   * - variables holding the way to access the shared memory to communicate with
   *   controlling process.
   */
  int    shrd_mem_id;
  long   shrd_mem_sz;
  bool   b_flag_exit;
  bool   b_flag_master_step_down;
  bool   b_flag_thrd_stop;

  string fencing_script_path;

  pthread_t schedule_rfthrd_id;
  pthread_t reschedule_thrd_id;
  pthread_t checktasks_thrd_id;
  pthread_t split_rftk_thrd_id;
  pthread_t check_util_thrd_id;

  pthread_t event_hndl_thrd_id;
  pthread_t node_lvchk_thrd_id;
  pthread_t check_point_thrd_id;

  ConfigureUtil ypconf_obj; 

  YappSubtaskQueue newly_cr_task_queue_proxy;
  YappSubtaskQueue ready_task_queue_proxy;
  YappSubtaskQueue running_task_queue_proxy;
  YappSubtaskQueue paused_task_queue_proxy;
  YappSubtaskQueue rfile_task_queue_proxy;
  YappSubtaskQueue failed_task_queue_proxy;
  YappSubtaskQueue termin_task_queue_proxy;

  bool sync_task_queue_path() {
    bool status = false;
    if (NULL == zkc_proxy_ptr) { return status; }
    newly_cr_task_queue_proxy.set_queue_path(
      zkc_proxy_ptr->get_newtsk_queue_path_prefix()
    );
    ready_task_queue_proxy.set_queue_path(
      zkc_proxy_ptr->get_ready_queue_path_prefix()
    );
    running_task_queue_proxy.set_queue_path(
      zkc_proxy_ptr->get_running_queue_path_prefix()
    );
    paused_task_queue_proxy.set_queue_path(
      zkc_proxy_ptr->get_paused_queue_path_prefix()
    );
    rfile_task_queue_proxy.set_queue_path(
      zkc_proxy_ptr->get_rfile_task_queue_path_prefix()
    );
    failed_task_queue_proxy.set_queue_path(
      zkc_proxy_ptr->get_failed_task_queue_path_prefix()
    );
    termin_task_queue_proxy.set_queue_path(
      zkc_proxy_ptr->get_terminated_queue_path_prefix()
    );
    status = true;
    return status;
  }

public:

  /**
   * Desc:
   * - This will be the single entry for any ypadmin query related to listing
   *   current running jobs with the ability to fiter by host or owner, which
   *   is supposed to be invoked by current master inst.
   */
  void query_running_task_basic_info_rpc(
    map<string, map<string, vector<string > > > & tsk_grp_arr,
    const vector<string> & host_arr, const vector<string> & owner_arr);

  /**
   * Desc:
   * - This will be the entry for resolving any master requests related to the
   *   query of current running job info, which is supposed to be invoked by a
   *   certain worker instance, which return the subtask info back to master.
   */
  void get_running_task_basic_info_rpc(map<string, vector<string> > & stsk_inf,
                                             const vector<string> & owner_arr);

  void query_yappd_envs_rpc(map<string, map<string, string> > & host_to_kv_map);

  void get_running_envs_rpc(map<string, string> & env_kv_map);


  /**
   * TODO:
   * - unit-testing
   * Desc:
   * - This method will return all procs handles for those tasks indicated by
   *   the given hndl. array. Since the hndl. may be a job handle, task handle
   *   or a process handle, users should be able to get all the process handle
   *   under the given jobs or tasks.
   * Params:
   * - const vector<string> & rndm_hndl_arr, set of hndl with arbitrary type.
   * -       vector<string> & proc_hndl_arr, where all those proc hndl returned
   * -       YappSubtaskQueue & queue_obj, the queue where the method will look
   * Return:
   * - YAPP_MSG_SUCCESS if everything goes well.
   * NOTE:
   * - This method is supposed to be called by following RPCs, not for client
   */
  void get_all_subtask_hndl_arr_in_queue(vector<string> & proc_hndl_arr,
                                   const vector<string> & rndm_hndl_arr,
                                         YappSubtaskQueue & queue_obj);

  /**
   * TODO:
   * - unit-testing
   * Desc:
   * - This method will return all procs handles for running tasks indicated by
   *   the given hndl. array. Since the hndl. may be a job handle, task handle
   *   or a process handle, users should be able to get all the process handle
   *   under the given jobs or tasks.
   * Params:
   * - const vector<string> & rndm_hndl_arr, set of hndl with arbitrary type.
   * -       vector<string> & proc_hndl_arr, where all those proc hndl returned
   * Return:
   * - YAPP_MSG_SUCCESS if everything goes well.
   * NOTE:
   * - Only processes currently running could be paused!
   */
  void get_all_active_subtask_hndl_arr_rpc(vector<string> & proc_hndl_arr,
                                     const vector<string> & rndm_hndl_arr);

  /**
   * TODO:
   * - unit-testing
   * Desc:
   * - This method will return all proc handles for paused tasks indicated by
   *   the given hndl. array. Since the hndl. may be a job handle, task handle
   *   or a process handle, users should be able to get all the process handle
   *   under the given jobs or tasks.
   * Params:
   * - const vector<string> & rndm_hndl_arr, set of hndl with arbitrary type.
   * -       vector<string> & proc_hndl_arr, where all those proc hndl returned
   * Return:
   * - YAPP_MSG_SUCCESS if everything goes well.
   * NOTE:
   * - Only processes already been paused could be resumed!
   */
  void get_all_paused_subtask_hndl_arr_rpc(vector<string> & proc_hndl_arr,
                                     const vector<string> & rndm_hndl_arr);
  /**
   * TODO:
   * - unit-testing
   * Desc:
   * - This method will return all procs handles for running tasks indicated by
   *   the given hndl. array. Since the hndl. may be a job handle, task handle
   *   or a process handle, users should be able to get all the process handle
   *   under the given jobs or tasks.
   * Params:
   * - const vector<string> & rndm_hndl_arr, set of hndl with arbitrary type.
   * -       vector<string> & proc_hndl_arr, where all those proc hndl returned
   * Return:
   * - YAPP_MSG_SUCCESS if everything goes well.
   * NOTE:
   * - Only processes currently running could be paused!
   */
  void get_all_terminated_subtask_hndl_arr_rpc(vector<string> & proc_hndl_arr,
                                         const vector<string> & rndm_hndl_arr);

  /**
   * TODO:
   * - unit-testing
   * Desc:
   * - This method will return all procs handles for running tasks indicated by
   *   the given hndl. array. Since the hndl. may be a job handle, task handle
   *   or a process handle, users should be able to get all the process handle
   *   under the given jobs or tasks.
   * Params:
   * - const vector<string> & rndm_hndl_arr, set of hndl with arbitrary type.
   * -       vector<string> & proc_hndl_arr, where all those proc hndl returned
   * Return:
   * - YAPP_MSG_SUCCESS if everything goes well.
   * NOTE:
   * - Only processes currently running could be paused!
   */
  void get_all_failed_subtask_hndl_arr_rpc(vector<string> & proc_hndl_arr,
                                     const vector<string> & rndm_hndl_arr);

public:
  /**
   * Following parts are rpc service as a Yapp Worker.
   */

  /**
   * Desc:
   * - following 2 methods would be the RPC calls that a Yapp Worker instance
   *   would expose to outside world, all RPCs are supposed to be called by
   *   yapp master only.
   */
  bool execute_sub_task_async_rpc(const Task & sub_task);

  /**
   * TODO
   * Desc:
   * - This method will be invoked by a Yapp Master instance to either pause,
   *   resume or terminate a given set of sub-tasks by using a specified signal.
   * NOTE:
   * - This method does nothing but sending the signal to a set of processes
   *   running those sub-tasks using kill, which means it only guarantees the
   *   successful generation of the signal (either pending or delivered). The
   *   Yapp Master instance will be notified when each of those sub-tasks got
   *   terminated(done by the dedicated thread waiting for the child process)
   */
  void signal_sub_task_async_rpc(vector<bool> & ret_arr,
                           const vector<int> & pid_arr,
                           const vector<string> & proc_hnd_arr,
                           const int signal);
public:
  /**
   * Following parts are rpc service as a Yapp Master.
   */

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
  void print_failed_queue_stat(string & tree_str, const string & hndl_str);

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
};

}
}
#endif
