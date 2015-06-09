#ifndef YAPP_WORKER_SERVICE_HANDLER_H_
#define YAPP_WORKER_SERVICE_HANDLER_H_
/**
 * Desc:
 *
 * - This is where each Sub-Task gets done, the calling of this method would
 *   be fired upon YappWorker instance receives a task available notification
 *   from other yapp services(either master or worker node). And a YappWorker
 *   instance(a thrift server thread) will call YappWorkerServiceHandler in
 *   response to execute the task.
 *
 * - Worker do nothing but fork and run the task, Each one is essentially a
 *   dedicated thrift server listening at a 'well-known' yapp port(9527 by
 *   default). Its only task is to pre-allocate a pool of fixed number of
 *   threads, and serve every request with a dedicated thread. Each thread
 *   will do the following steps:
 *
 *   1. Check if the resources on this node are sufficient to run this task.
 *      Normally YAPP will set a maximum threshold for resource usage, like
 *      CPU, MEMORY, IOPS, DISK, FDs... monitored by a ResourceManager.
 *
 *   2. Check if the total count of jobs running concurrently execeed the max
 *      value specified by the YappWorker, same as YAPP_WORKER_THREAD_POOL_SIZE.
 *      Since there is no support for genereating the asynchronous RPC code from
 *      native thrift library, we decided to maintain a class wide counter for
 *      monitoring the number of running child processes to mimic that feature
 *      while still utilizing the thread pool provided by thrift.
 *
 *   3. If the current execution flow satisfies all the conditions, it will:
 *      - fork a child process & run the command line directly for task object.
 *      - upon the success of the fork call, parent process increase task cnt.
 *      - return from the execution flow.
 *      - NOTE!
 *        1) task cnt will be maintained by the parent process only so that fork
 *           call won't have any trouble such as waiting a lock hold by nobody.
 *        2) each thread in the pool for handling RPC call request will return
 *           right after calling fork and set the counter, the YappWorker proc.
 *           has a dedicated thread for receiving signals sent from all child
 *           processes running independent sub-tasks and reap them all together.
 *
 *   4. The child process will run the command and blocks until the return of
 *      the running command line(either EXIT_SUCCESS or EXIT_FAILURE), and then
 *      notify its parent process via either sending a signal(famous SIGCHLD?)
 *      or the thread that creates this child process will call waitpid to reap
 *      it(which is a preferred solution given that waiting for a lock at signal
 *      handler is not a clean operation).
 *
 *   5. Upon returning from the calling of waitpid from any child process, the
 *      thread dedicated for this would do the following:
 *      - check the status value returned from the waitpid, to see if the child
 *        process terminated sucessfully or faulty by checking its exit status,
 *        by mainly using WIFEXITED(status) & WEXITSTATUS(status).
 *      - notify the yapp master for the exit status of such child process(or
 *        sub-task) by making an ASYNC RPC calls.
 *
 * - Also note that the size of thread pool will limit the maximum possible
 *   concurrent task running processes on each node. Ofcourse, that will also
 *   be limited by the current resource usage on a certain node(which ever
 *   comes first).
 *
 * - Any request for executing a new task after execeeding the limits would
 *   be ommitted & passed to its neighboring worker node. And a neighboring
 *   node is any one with minimum possible seq.# bigger than current node's
 *   in zookeeper cluster(under the folder holding the election).
 *
 * - Upon successful finish of a task, the worker will return true.
 *
 */

#include <vector>
#include <map>

#include <iostream>
#include <fstream>
#include <cassert>
#include <iostream>
#include <string>
#include <sstream>
#include <cstring>
#include <cstdlib>

#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "../util/yapp_util.h"
#include "yapp_service_util.h"
#include "gen-cpp/YappWorkerService.h"
#include "gen-cpp/yapp_service_constants.h"
#include "gen-cpp/yapp_service_types.h"

namespace yapp {
namespace domain {

using std::map;
using std::vector;
using std::string;
using std::ostringstream;
using std::ofstream;
using std::ios;

using namespace yapp::util;

extern const yapp_serviceConstants g_yapp_service_constants;

const static int YAPP_WORKER_SERV_HNDL_INVAL_SIGNAL = -1;
const static int YAPP_WORKER_SERV_HNDL_INVAL_RETVAL = -1;
const static int YAPP_WORKER_MAX_ENV_CNT = 256;
const static int YAPP_WORKER_MAX_RETRY_CNT = 32;

class YappWorkerServiceHandler : public YappWorkerServiceIf {
  friend class YappServiceHandler;
public:
  YappWorkerServiceHandler(ZkClusterProxy * zkc_ptr, int max_task_num,
                           bool b_verb = true, bool b_test = false)
  {
    zkc_proxy_ptr = zkc_ptr;
    max_concur_task = max_task_num;
    yapp_worker_child_procs_cnt = 0;
    b_verbose = b_verb;
    b_testing = b_test;
  }

  ~YappWorkerServiceHandler() {}

  void set_zkc_proxy_ptr(ZkClusterProxy * zk_ptr) { zkc_proxy_ptr = zk_ptr; }

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
   * Desc:
   * - check if there is enough space to schedule another job to run. Currently
   *   the logic here will only check the max job number limit to simplify the
   *   assumption, later on will adding metrics as follows:
   *   { DB load, system load, memory usage, disk usage, IOPs/sec }
   * Return:
   * - YAPP_MSG_SUCCESS if OK.
   * Note!!!
   * - The method is NOT THREAD-SAFE! The caller would be responsible to acquire
   *   the lock for setting up the apporiate critical section.
   */
  YAPP_MSG_CODE check_resources_allocation();

  /**
   * Desc:
   * - this would dynamically reset the limit of maximum concurrent tasks for a
   *   yapp worker instance to schedule on a node.
   * Params:
   * - int max: the new limits, needs to be > 0;
   * Return:
   * - true if the value got resat, false otherwise.
   * NOTE:
   * - this call would pause a blocking call to wait for a lock at process level
   *   which is per yapp worker instance.
   */
  bool set_max_concur_task(int max);

  /**
   * Desc:
   * - Simply a to_string style method for printing a sub-task.
   */
  static string get_sub_task_cmd(const Task & sub_task);

  /**
   * TODO
   * Desc:
   * - Once upon the child process running the sub-task terminated(either normal
   *   or abnormal), the dedicated thread waiting for it would fire a RPC call
   *   to yapp master instance to notify its termination. Since master also need
   *   to know if it is needed to restart the sub-task on another available yapp
   *   worker instance and having master instance as the only one to do Re-Start
   */
  YAPP_MSG_CODE notify_master_proc_completion(const Task & sub_task,
                                              int   task_ret,
                                              int   task_sig);

  /**
   * Desc:
   * - This method will be used to initialize A Yapp Worker instance by caching
   *   the latest naming location of Yapp Master in case of master's changing.
   */
  YAPP_MSG_CODE locate_master_node();

private:

  bool sync_task_queue_path() {
    bool status = false;
    if (NULL == zkc_proxy_ptr) { return status; }
    newly_created_task_set.set_queue_path(
      zkc_proxy_ptr->get_newtsk_queue_path_prefix()
    );
    ready_task_set.set_queue_path(
      zkc_proxy_ptr->get_ready_queue_path_prefix()
    );
    running_task_set.set_queue_path(
      zkc_proxy_ptr->get_running_queue_path_prefix()
    );
    paused_task_set.set_queue_path(
      zkc_proxy_ptr->get_paused_queue_path_prefix()
    );
    termin_task_set.set_queue_path(
      zkc_proxy_ptr->get_terminated_queue_path_prefix()
    );
    failed_task_set.set_queue_path(
      zkc_proxy_ptr->get_failed_task_queue_path_prefix()
    );
    status = true;
    return status;
  }

  /**
   * Desc:
   * - This method will be doing the checking and create of the range input file
   *   managed by the yapp system, and return a bool value.
   * Params:
   * - int id_from: starting range id.
   * - int id_to: ending range id
   * - int id_step: fixed step.
   * - const string & range_file: system generated range file path, task-id.conf
   * Return:
   * - true either the file already exists or successfully re-created that file
   * - false otherwise.
   */
  map<string, bool> input_file_bflag_map;
  bool check_and_build_range_file(int id_from, int id_to, int id_step,
                                         const string & range_file);

  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - This method would mark a certain process as PROC_STATUS_READY after this
   *   process is permitted to be scheduled(pass check_resources_allocation()).
   */
  YAPP_MSG_CODE mark_process_as_ready(const string & proc_hnd, string & lock_h);

  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - This method would mark a certain process as PROC_STATUS_RUNNING after the
   *   process has actually be forked or resumed, but before waitpid is finished.
   */
  YAPP_MSG_CODE mark_process_as_running(const string & proc_hnd, int child_pid);

  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - This method would mark a certain process as PROC_STATUS_BLOCKED after the
   *   process being paused by YAPP(msg flow may be:client -> master -> worker)
   *   or by the system for un-known reason.
   */
  YAPP_MSG_CODE mark_process_as_blocked(const string & proc_hnd, int child_pid);

  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - This method would mark a certain process as PROC_STATUS_RUNNING after the
   *   process being resumed by YAPP(msg flow may be:client -> master -> worker)
   *   or by the system for un-known reason.
   */
  YAPP_MSG_CODE mark_process_as_resumed(const string & proc_hnd, int child_pid);

  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - This method would mark a certain process as PROC_STATUS_TERMINATED after
   *   the return of waitpid(hopefully with a RET of 0 indicating NO core dump)
   */
  YAPP_MSG_CODE mark_process_as_terminated(
    const string & proc_hnd, int task_ret = 0,
    int term_sig = g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_TERMIN_SIG
  );

  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - This will check if the worker instance needs to sync the job_node on the
   *   connected zookeeper server. It will sync the job node and update the var
   *   lastest_job_hnd if both of the following condition matched:
   *   {
   *   1. the job_hnd is newer than the lastest_job_hnd(bigger in lexicographic
   *      order, which means it is a job created after the one synced before)
   *   2. the job handle for indicated by job_hnd is empty or the node not found
   *   }
   * - If the job_hnd is newer, it will update the lastest_job_hnd when the sync
   *   succeeded.
   * Params:
   * - const string & job_hnd: the job handle for the node to be checked.
   */
  YAPP_MSG_CODE check_and_sync_job_node(const string & job_hnd);

  /**
   * Desc:
   * - This method will setting up the I/O redirection for the child process
   *   which actually runs the sub-task. For each non-NULL pointer spcified in
   *   the argument list (represents a string to a path of a log file), this
   *   method will redirect both stdout and stderr accordingly.
   * - If any the pointer is NULL, the method will simply turn off corresponding
   *   by default(either stdout_fptr or stderr_fptr, or both).
   * Params:
   * - const char * stdout_fptr: represents the log file for stdout.
   * - const char * stderr_fptr: represents the log file for stderr.
   * Returns:
   * - true if everything ok, false otherwise.
   */
  static bool setup_sub_task_io_redirection(const string & stdout_file,
                                            const string & stderr_file);

  /**
   * Desc:
   * - This method is supposed to be used for reaping a given child process runs
   *   any kind of sub-task(either file or range), blocks until its termination.
   *   It also tracks any process suspension and resume for debugging.
   * - Upon its return, it will also return the exit code of child process and a
   *   indicator to spcify the signal number if it was killed by certain signals
   * Params:
   * - pid_t child_pid: the id of the child process to wait for.
   * - int & task_ret: used to hold the exit value of the child process.
   *   >>> normal termination(though the program logic may not be normal)
   * - int & task_sig: shows the signal number if child process killed by signal
   *   >>> abnormal termination(like typing kill -9 when eatting up all memory).
   * return:
   * - true if ok, false otherwise.
   */ 
  static bool wait_for_sub_task_completion(pid_t child_pid,
                                           int & task_ret,
                                           int & task_sig,
                                           bool b_verbose = true);
  /**
   * Desc: 
   * - A wrapper for initiating new sub-task, firing new thread without blocking.
   */
  YAPP_MSG_CODE start_sub_task_async(const Task & sub_task);

  /**
   * Desc:
   * - This would be the actual thread to handle the full execution for a single
   *   subtask(process). Basically, it will perform the following logics {
   *   - 1. fork a child process & run that sub-task's command line directly.
   *   - 2. waits for the child's termination and gethering the status.
   *   - 3. notify the master node by issuing RPC.
   *   - 4. decrease the count for subtasks running concurrently.
   *   }
   */
  static void * thread_start_sub_task_async(void * data_ptr);

  void check_point_cur_running_jobs(); 
  void check_point_cur_running_jobs_ex(); 

  /**
   * following 3 methods and a map will be used for process to kill all jobs
   * upon the request of exit in best effort style.
   */
  map<int, Task> pid_map;
  void kil_pid_in_registry_atomic();
  void add_pid_to_registry_atomic(int pid, const Task * task_ptr);
  void del_pid_in_registry_atomic(int pid);
  int  get_pid_sz_registry_atomic();
  void get_tasks_group_atomic(map<string, vector<string> > & tskgrp_by_owner,
                                    const vector<string> & owner_arr);

  bool decrease_task_count();

  /** supposed to be immutable, managed by YappServiceHandler **/ 
  ZkClusterProxy * zkc_proxy_ptr;

  YappSubtaskQueue newly_created_task_set;
  YappSubtaskQueue ready_task_set;
  YappSubtaskQueue running_task_set;
  YappSubtaskQueue paused_task_set;
  YappSubtaskQueue termin_task_set;
  YappSubtaskQueue failed_task_set;

  string master_host;
  int    master_port;

  int max_concur_task;
  int yapp_worker_child_procs_cnt;

  string latest_job_hnd;

  bool b_verbose;
  bool b_testing;
};

class YappWorkerThreadDataBlock {
public:
  YappWorkerThreadDataBlock(YappWorkerServiceHandler * hndl_ptr,
                            Task * t_ptr) :
                            yw_serv_hndl_ptr(hndl_ptr), task_ptr(t_ptr)
  {}
  ~YappWorkerThreadDataBlock() {
    delete task_ptr;
  }
  YappWorkerServiceHandler * yw_serv_hndl_ptr;
  Task * task_ptr;
};

}
}
#endif
