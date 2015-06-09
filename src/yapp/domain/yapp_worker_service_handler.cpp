#include <cstdlib>
#include <sys/types.h>
#include <errno.h>
#include <signal.h>
#include "yapp_worker_service_handler.h"
#include <sys/wait.h>

extern char ** environ;

using namespace yapp::domain;

/* this mutex needs guaranteed to be initialized exactly once per process */
static pthread_mutex_t yapp_worker_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t yapp_worker_mutex_for_sync = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t yapp_worker_mutex_for_pids = PTHREAD_MUTEX_INITIALIZER;

static pthread_rwlock_t yappw_inst_rwlock = PTHREAD_RWLOCK_INITIALIZER;

bool YappWorkerServiceHandler::execute_sub_task_async_rpc(const Task & sub_task)
{
  if (true == b_verbose) {
    std::cout << "-- YappWorkerServiceHandler::execute_range_task" << std::endl;
    std::cout << ">>>> " << get_sub_task_cmd(sub_task) << std::endl;
  }
  bool status = false;
  /** 1. grab the lock, followed by a critical section with R/W mode. **/
  pthread_mutex_lock(&yapp_worker_mutex);

  /** 2. check if it is ok to initiate a new thread. **/
  if (YAPP_MSG_SUCCESS == check_resources_allocation()) {
  /** 3. if Yes, then start a new thread for that task. **/
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
    std::cerr << "-- Thread: " << pthread_self()  << " BEGIN TO START TASK ASYNC." << std::endl;
#endif
    if (YAPP_MSG_SUCCESS == start_sub_task_async(sub_task)) {
  /** 4. increment the task counter upon the successful creation. **/
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      std::cerr << "-- Thread: " << pthread_self()
                << " SUCCEEDED IN STARTING TASK ASYNC." << std::endl;
#endif
      yapp_worker_child_procs_cnt++;
      status = true;
    }
  }
  /** 5. release the lock. **/
  pthread_mutex_unlock(&yapp_worker_mutex);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "-- Thread: " << pthread_self()  << " FINISHED EXEC TASK ASYNC." << std::endl;
#endif
  return status;
}

string YappWorkerServiceHandler::get_sub_task_cmd(const Task & sub_task) {
  ostringstream task_cmd;
  task_cmd << sub_task.app_env << " "
           << sub_task.app_bin << " "
           << sub_task.app_src << " "
           << sub_task.proc_arr.front().range_from << " "
           << sub_task.proc_arr.front().range_to << " "
           << sub_task.input_file << " "
           << sub_task.arg_str << " >"
           << sub_task.proc_arr.front().std_out << " 2>"
           << sub_task.proc_arr.front().std_err << " & ";
  return task_cmd.str();
}

YAPP_MSG_CODE YappWorkerServiceHandler::start_sub_task_async(
  const Task & sub_task)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID_THREAD_CREATE;
  /** put the task into READY queue before actually starts it. **/
  string lock_hndl;
  rc = mark_process_as_ready(sub_task.proc_arr.front().proc_hnd, lock_hndl);

  if (YAPP_MSG_SUCCESS != rc) { return rc; }

  pthread_t task_thrd_id;
  /** never pass a pointer of a var. allocated on the stack! **/
  Task * tsk_blk_ptr = new Task();
  * tsk_blk_ptr = sub_task;
  YappWorkerThreadDataBlock * thrd_dat_blck_ptr =
    new YappWorkerThreadDataBlock(this, tsk_blk_ptr);

  if (0 != pthread_create(
             &task_thrd_id, NULL, thread_start_sub_task_async, thrd_dat_blck_ptr
           ))
  {
    /**
     * if the system fail to start the process for any reason, simply release
     * the lock in ready queue in best effort, so that it becomes the zombie
     * in "READY" queue, yapp will re-queue it in in the future.
     */
    zkc_proxy_ptr->release_lock(lock_hndl);
    rc = YAPP_MSG_INVALID_THREAD_CREATE;
  }
  return rc;
}

YAPP_MSG_CODE YappWorkerServiceHandler::check_and_sync_job_node(
  const string & job_hnd)
{
  pthread_mutex_lock(&yapp_worker_mutex_for_sync);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "-- Thread: " << pthread_self()  << " check SYNC." << std::endl;
#endif
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (!latest_job_hnd.empty() && latest_job_hnd >= job_hnd) {
    pthread_mutex_unlock(&yapp_worker_mutex_for_sync);
    return YAPP_MSG_SUCCESS;
  }
  string job_node_path = zkc_proxy_ptr->get_job_full_path(job_hnd);
  string cur_job_hnd;
  rc = zkc_proxy_ptr->get_node_data(
    job_node_path + string("/job_hnd"), cur_job_hnd
  );
  if ((YAPP_MSG_SUCCESS == rc && !cur_job_hnd.empty())) {
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
    std::cerr << "-- Thread: " << pthread_self()  << " Zk Host Is IN SYNC."
              << std::endl;
#endif
  } else {
  /** reaching here basically means job_hnd node does not get reflected yet **/
    rc = zkc_proxy_ptr->sync(job_node_path + string("/job_hnd"));
    if (YAPP_MSG_SUCCESS != rc) {
      /** sync job root if job node not get reflected yet, SHOULD BE RARE **/
      rc = zkc_proxy_ptr->sync(job_node_path);
    }
    if (YAPP_MSG_SUCCESS == rc) {
      latest_job_hnd = cur_job_hnd;
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      std::cerr << "-- Thread: " << pthread_self()  << " Zk Host OUT OF SYNC, "
                << "SYNC Update Latest Job_Hnd To " << cur_job_hnd << std::endl;
#endif
    } else {
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      std::cerr << "-- Thread: " << pthread_self()  << " Zk Host OUT OF SYNC, "
                << " Failed TO CALL THE SYNC for " << cur_job_hnd << std::endl;
#endif
    }
  }

  if ((latest_job_hnd.empty() || latest_job_hnd < cur_job_hnd) &&
                                           (!cur_job_hnd.empty())) {
    latest_job_hnd = cur_job_hnd;
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
    std::cerr << "-- Thread: " << pthread_self() << " Update the Latest Job_Hnd To "
              << cur_job_hnd << std::endl;
#endif
  }

  pthread_mutex_unlock(&yapp_worker_mutex_for_sync);
  return rc;
}

bool YappWorkerServiceHandler::check_and_build_range_file(
  int id_from, int id_to, int id_step, const string & range_file)
{
  bool status = false;
  map<string, bool>::iterator itr;
  /** 1. try read lock to see if the system range file already been checked. */
  if (0 == pthread_rwlock_rdlock(&yappw_inst_rwlock)) {
    itr = input_file_bflag_map.find(range_file);
    struct stat stat_buf;
    /** also check the existence of a file in case if sys. file got deleted. */
    if ((input_file_bflag_map.end() != itr) &&
        (0 == stat(range_file.c_str(), &stat_buf))) {
      status = itr->second;
    }
    pthread_rwlock_unlock(&yappw_inst_rwlock);
  }
  /** 2. continue to set up the range file as needed by using a write lock. */
  if ((false == status) && 0 == pthread_rwlock_wrlock(&yappw_inst_rwlock)) {
    itr = input_file_bflag_map.find(range_file);
    if ((input_file_bflag_map.end() != itr) && true == itr->second) {
      status = itr->second;
    } else {
      vector<ConfigureUtil::RangePair> rng_arr_ret;
      input_file_bflag_map[range_file] = false;
      if (true == ConfigureUtil::split_range_by_fixed_step(
                      id_from, id_to, id_step, rng_arr_ret))
      {
        ofstream f_rng(range_file.c_str(), ios::out | ios::trunc);
        if (f_rng.is_open()) {
          int size = rng_arr_ret.size();
          for (int i = 0; i < size; i++) {
            f_rng << rng_arr_ret[i].start_id << ", "
                  << rng_arr_ret[i].end_id << std::endl;
          }
          f_rng.flush();
          f_rng.close();
          input_file_bflag_map[range_file] = true;
          status = true;
        }
      }
    }
    pthread_rwlock_unlock(&yappw_inst_rwlock);
  }
  return status;
}

void * YappWorkerServiceHandler::thread_start_sub_task_async(void * data_ptr)
{
  pthread_detach(pthread_self());

  YappWorkerThreadDataBlock * dat_ptr = (YappWorkerThreadDataBlock *)data_ptr;
  YappWorkerServiceHandler * yw_serv_ptr = dat_ptr->yw_serv_hndl_ptr;

  const Task * task_ptr = dat_ptr->task_ptr;

  /**
   * Check if the necessary range file (maintained by system) is already
   * generated before initiating a new Dynamic Range Task.
   */
  bool b_range_file_ready = true;
  /*
  if (TASK_INPUT_TYPE::DYNAMIC_RANGE == task_ptr->input_type) {
    b_range_file_ready = yw_serv_ptr->check_and_build_range_file(
      task_ptr->range_from, task_ptr->range_to,
      task_ptr->range_step, task_ptr->input_file
    );
  }
  */
  /** Only one winning thread started would actually call sync if necessary. **/
  yw_serv_ptr->check_and_sync_job_node(
    task_ptr->task_hnd.substr(0, task_ptr->task_hnd.find("_"))
  );

  /** indicate which signal the child process was killed by **/
  int task_sig = g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_TERMIN_SIG;
  /** store the value returned by the child process **/
  int task_ret = g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_RETURN_VAL;

  /** 1. fork a child process & run that sub-task's command line directly. **/
  pid_t child_pid = fork();

  /**
   * 2. -> for child process, simply setup the io & run the command line.
   *    -> for parent process, wait & identify child's termination status.
   */
  if (0 == child_pid) {
    if (false == b_range_file_ready) {
      std::cerr << "-- Error happend generating range file for child process: "
                << getpid() << std::endl;
      exit(g_yapp_service_constants.PROC_CTRL_BLCK_RET_FAIL_GENRNGFLE);
    }
    /** 1st needs to close all inherited fds except stdin, stdout, stderr **/
    int fds_cnt = getdtablesize();
    for (int f = 3; f < fds_cnt; f++) { close(f); }

    /** -- child process would start here. **/
    if (true == yw_serv_ptr->b_verbose) {
      std::cout << ">> child process: " << getpid() << " will run command "
                << get_sub_task_cmd(*task_ptr).c_str() << std::endl;
    }
    /**
     * 2.1 setting up the I/O redirection to the corresponding log file. Also
     *     note that if there is no log file specified, we needs to explicitly
     *     turn off { stdout | stderr }
     */
    if (SystemIOUtil::setup_io_redirection(task_ptr->proc_arr.front().std_out,
                                           task_ptr->proc_arr.front().std_err))
    {
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      std::cerr << std::endl;
      std::cerr << ">>>>>>>>>>>>>>>> START TO PREPARE TO EXECTUE: " << " ";
      std::cerr << std::endl;
#endif

      vector<char *> env_ptr_arr;
      char * env_buf = NULL;
      if (!task_ptr->app_env.empty()) {
        env_buf = new char[task_ptr->app_env.size() + 1];
        strncpy(env_buf, task_ptr->app_env.c_str(), task_ptr->app_env.size());
        env_buf[task_ptr->app_env.size()] = '\0';
        bool b_new_env = true;
        for (size_t f = 0; f < task_ptr->app_env.size();) {
          while ((f < task_ptr->app_env.size()) && (' ' == env_buf[f])) {
            b_new_env = true; env_buf[f] = '\0'; f++;
          }
          if ((true == b_new_env) && (f < task_ptr->app_env.size())){
            env_ptr_arr.push_back(env_buf+f); b_new_env = false;
          }
          f++;
        }
      }

      char ** arg_arr = NULL;
      vector<string> arg_set;
      size_t pos = string::npos;

      string anchor_str = task_ptr->proc_arr.front().cur_anchor;
      //  StringUtil::convert_int_to_str(task_ptr->proc_arr.front().cur_anchor);

      switch ((int)task_ptr->input_type) {
        case ((int)TASK_INPUT_TYPE::DYNAMIC_RANGE): {
          /** ${app} ${src} [ $range_from $range_to $log_anchor_pos arg0, arg1, ..., argn ] **/
          string tskhd_in_job, max_line_idx, tskhd_in_run,
                 cur_line_idx, nxt_line_idx, tot_proc_cnt;
          RangeFileTaskDataParser::parse_rfile_task_data_str(
            task_ptr->proc_arr.front().proc_hnd,
              tskhd_in_job, max_line_idx, tskhd_in_run,
              cur_line_idx, nxt_line_idx, tot_proc_cnt
          );

          long long rng_beg = task_ptr->range_step * atoll(cur_line_idx.c_str()) +
            task_ptr->range_from;
          long long rng_end = ((rng_beg + task_ptr->range_step - 1) > task_ptr->range_to) ?
            task_ptr->range_to : (rng_beg + task_ptr->range_step - 1);

          arg_set.push_back(StringUtil::convert_int_to_str(rng_beg));
          arg_set.push_back(StringUtil::convert_int_to_str(rng_end));

          if (g_yapp_service_constants.PROC_CTRL_BLCK_ANCHR_START ==
              task_ptr->proc_arr.front().start_flag) {
            if (task_ptr->proc_arr.front().last_updated_tmstp_sec <
                TaskHandleUtil::get_last_modified_epoch_in_sec(task_ptr->proc_arr.front().std_out)) {
              string new_line = "", new_anchor = "";
              TaskHandleUtil::get_last_pos(
                task_ptr->proc_arr.front().std_out, task_ptr->anchor_prfx,
                yapp::domain::MAX_LINES_TO_CHECK_STR, new_line, new_anchor
              );
              int new_line_idx = (
                atoll(new_line.c_str()) - task_ptr->proc_arr.front().range_from
              ) / task_ptr->range_step;
              if ((false == new_line.empty()) && (atoll(cur_line_idx.c_str()) == new_line_idx)) {
                anchor_str = (new_line + DEF_LINE_AND_OFFSET_DELIM + new_anchor);
              }
            }
          }
          break;

        }
        case ((int)TASK_INPUT_TYPE::RANGE_FILE): {
          /** ${app} ${src} [ $input_file $lineid $log_anchor_pos arg0, arg1, ..., argn ] **/
          string tskhd_in_job, max_line_idx, tskhd_in_run,
                 cur_line_idx, nxt_line_idx, tot_proc_cnt;
          RangeFileTaskDataParser::parse_rfile_task_data_str(
            task_ptr->proc_arr.front().proc_hnd,
              tskhd_in_job, max_line_idx, tskhd_in_run,
              cur_line_idx, nxt_line_idx, tot_proc_cnt
          );
          arg_set.push_back(task_ptr->input_file);
          arg_set.push_back(cur_line_idx);
  
          /**
           * every time a subtask gets restarted, with startting flag marked as
           * PROC_CTRL_BLCK_ANCHR_START(which is by def., including zombie jobs
           * or --restart-failed),  we will pull the log from the global position
           * to get the latest anchor in best effort, compare it with anchor value
           * logged in zookeeper, update it in object if needed.
           */
          if (g_yapp_service_constants.PROC_CTRL_BLCK_ANCHR_START ==
              task_ptr->proc_arr.front().start_flag) {
            string new_line = "", new_anchor = "";
            TaskHandleUtil::get_last_pos(
              task_ptr->proc_arr.front().std_out, task_ptr->anchor_prfx,
              yapp::domain::MAX_LINES_TO_CHECK_STR, new_line, new_anchor
            );
            // if ((false == new_line.empty()) && (new_line == cur_line_idx) &&
            //     (atoll(new_anchor.c_str()) > task_ptr->proc_arr.front().cur_anchor)){
            //   anchor_str = new_anchor;
            // }
            if ((false == new_line.empty()) && (new_line == cur_line_idx)) {
              anchor_str = new_anchor;
            }
          }
          break;
        }
        case ((int)TASK_INPUT_TYPE::ID_RANGE): {
          /** ${app} ${src} [ $range_from $range_to $log_anchor_pos arg0, arg1, ..., argn ] **/
          arg_set.push_back(StringUtil::convert_int_to_str(task_ptr->proc_arr.front().range_from));
          arg_set.push_back(StringUtil::convert_int_to_str(task_ptr->proc_arr.front().range_to));
          break;
        }
      }

      arg_set.push_back(anchor_str);

#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      std::cerr << ">>>> SETTING ANCHOR TO: " << arg_set.back() << std::endl;
#endif
      string tmp_arg_lft = task_ptr->arg_str;
      string tmp_arg_var;

      StringUtil::trim_string(tmp_arg_lft);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      std::cerr << ">>>>>>>>>>>>>>>> ARG STR: " << tmp_arg_lft << std::endl;
#endif 
      if (!tmp_arg_lft.empty()) {
        do {
          pos = tmp_arg_lft.find(' ');
          tmp_arg_var = tmp_arg_lft.substr(0, pos);
          arg_set.push_back(tmp_arg_var);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
          std::cerr << ">>>>>>>>>>>>>>>> ADDING ARG: " << tmp_arg_var << std::endl;
#endif 
          if (string::npos != pos) {
            tmp_arg_lft = tmp_arg_lft.substr(pos);
            StringUtil::trim_string(tmp_arg_lft);
          }
        } while (string::npos != pos);
      }

      arg_arr = new char * [arg_set.size() + 3];

      char * app_buf = new char[task_ptr->app_bin.size() + 1];
      strncpy(app_buf, task_ptr->app_bin.c_str(), task_ptr->app_bin.size());
      app_buf[task_ptr->app_bin.size()] = '\0';
      arg_arr[0] = app_buf;

      char * src_buf = new char[task_ptr->app_src.size() + 1];
      strncpy(src_buf, task_ptr->app_src.c_str(), task_ptr->app_src.size());
      src_buf[task_ptr->app_src.size()] = '\0';
      arg_arr[1] = src_buf;

      for (size_t i = 0; i < arg_set.size(); i++) {
        char * arg_buf = new char[arg_set[i].size() + 1];
        strncpy(arg_buf, arg_set[i].c_str(), arg_set[i].size());
        arg_buf[arg_set[i].size()] = '\0';
        arg_arr[i + 2] = arg_buf;
      }
      arg_arr[arg_set.size() + 2] = NULL;

      char * env_arr[YAPP_WORKER_MAX_ENV_CNT] = { NULL };
      size_t sys_env_cnt = 0;
      while((sys_env_cnt < YAPP_WORKER_MAX_ENV_CNT - 2) &&
            (NULL != environ[sys_env_cnt])) {
        env_arr[sys_env_cnt] = environ[sys_env_cnt];
        sys_env_cnt++;
      }
      for (size_t f = 0; f < env_ptr_arr.size(); f++) {
        if ((int)(sys_env_cnt + f) < YAPP_WORKER_MAX_ENV_CNT) {
          env_arr[sys_env_cnt + f] = env_ptr_arr[f];
        }
      }

#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      std::cerr << std::endl;
      std::cerr << ">>>>>>>>>>>>>>>> START TO EXECUTE: " << " ";
      char * app_ptr = app_buf;
      char * const * arg_ptr = &arg_arr[0];
      char * const * env_ptr = &env_arr[0];
      std::cerr << app_ptr << " ";
      while (NULL != *arg_ptr) { std::cerr << *arg_ptr << " : "; arg_ptr++; }
      while (NULL != *env_ptr) { std::cerr << *env_ptr << " : "; env_ptr++; }
      std::cerr << std::endl;
#endif
      if (!task_ptr->working_dir.empty()) {
        if (0 == chdir(task_ptr->working_dir.c_str())) {
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
          std::cerr << ">>>> FINISH CHDIR TO: " << task_ptr->working_dir << std::endl;
#endif
          exit(execve(app_buf, arg_arr, env_arr));
        } else {
          std::cerr << "-- Error happend entering working folder for child process: "
                    << getpid() << std::endl;
          exit(g_yapp_service_constants.PROC_CTRL_BLCK_RET_FAIL_CHANGEDIR);
        }
      } else {
        exit(execve(app_buf, arg_arr, env_arr));
      }
    } else {
      std::cerr << "-- Error happend setting io redirection for child process: "
                << getpid() << std::endl;
      exit(g_yapp_service_constants.PROC_CTRL_BLCK_RET_FAIL_SET_IORED);
    }
    /** -- child process would end here, no more exec beyond this. **/
  } else if (0 < child_pid) {

    yw_serv_ptr->add_pid_to_registry_atomic(child_pid, task_ptr);
    /**
     * 2.1 waiting for the child's termination and gethering the status.
     */
    if (true == yw_serv_ptr->b_verbose) {
      std::cout << "-- Wait for child process: " << child_pid << std::endl;
    }
    /**
     * 2.2 update the process status to be running after forking.
     */ 
    yw_serv_ptr->mark_process_as_running(
      task_ptr->proc_arr.front().proc_hnd, child_pid
    );
    wait_for_sub_task_completion(
      child_pid, task_ret, task_sig, yw_serv_ptr->b_verbose
    );
    /**
     * 2.3 update the process status to be running after termination.
     * NOTE THAT we only want to update the zookeeper when our session is not
     * explicitly expired, which our current pid registry will be empty.
     */

    if (0 == pthread_rwlock_rdlock(&yappw_inst_rwlock)) {
      if (0 < yw_serv_ptr->get_pid_sz_registry_atomic()) {
        yw_serv_ptr->mark_process_as_terminated(
          task_ptr->proc_arr.front().proc_hnd, task_ret, task_sig
        );
        yw_serv_ptr->del_pid_in_registry_atomic(child_pid);
      }
      pthread_rwlock_unlock(&yappw_inst_rwlock);
    }
  } else {
    std::cerr << "-- Error happend when doing the fork in process: "
              << getpid() << std::endl;
  }

  /** 3. notify the master node by issuing RPC. **/
  yw_serv_ptr->notify_master_proc_completion(*task_ptr, task_ret, task_sig);

  /** 4. decrease the count for subtask running concurrently. **/
  yw_serv_ptr->decrease_task_count();

#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  string proc_tree;
  string proc_hnd_in_job_node;
  std::cerr << ">>>>>>>>>>>>>>>>>>>>>> proc_str: "
            << task_ptr->proc_arr.front().proc_hnd << std::endl;
  RangeFileTaskDataParser::get_running_proc_hnd_in_job_node(
    task_ptr->proc_arr.front().proc_hnd, proc_hnd_in_job_node
  );
  std::cerr << ">>>>>>>>>>>>>>>>>>>>>> proc_str_in_job_node: "
            << proc_hnd_in_job_node << std::endl;
  yw_serv_ptr->zkc_proxy_ptr->print_node_recur(proc_tree,
    yw_serv_ptr->zkc_proxy_ptr->get_proc_full_path(
      proc_hnd_in_job_node),
    true
  );
  std::cerr << ">>>>>>>>>>>>>>>>>>>>>> subtask tree finished: " << std::endl;
  std::cerr << " process: " << task_ptr->proc_arr.front().proc_hnd
            << " terminated: " << std::endl << proc_tree;
#endif
  delete dat_ptr;

  return NULL;
}

bool YappWorkerServiceHandler::decrease_task_count() {
  pthread_mutex_lock(&yapp_worker_mutex);
  yapp_worker_child_procs_cnt--;
  pthread_mutex_unlock(&yapp_worker_mutex);
  return true;
}

void YappWorkerServiceHandler::check_point_cur_running_jobs_ex() {
  string tskhd_in_job, max_line_idx, tskhd_in_run,
         cur_line_idx, nxt_line_idx, tot_proc_cnt;
  map<int, Task>::iterator itr = pid_map.begin();
  for (; itr != pid_map.end(); itr++) {
    if (RangeFileTaskDataParser::is_range_file_task_hnd(
          itr->second.proc_arr.front().proc_hnd)) {
      RangeFileTaskDataParser::parse_rfile_task_data_str(
        itr->second.proc_arr.front().proc_hnd,
        tskhd_in_job, max_line_idx, tskhd_in_run,
        cur_line_idx, nxt_line_idx, tot_proc_cnt
      );
      TaskHandleUtil::update_cur_anchor_pos(
        tskhd_in_job, zkc_proxy_ptr, "",
        g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_ANCHOR_POS
      );
    }
  }
}

void YappWorkerServiceHandler::check_point_cur_running_jobs() {
  pthread_mutex_lock(&yapp_worker_mutex_for_pids);
  check_point_cur_running_jobs_ex();
  pthread_mutex_unlock(&yapp_worker_mutex_for_pids);
}

void YappWorkerServiceHandler::kil_pid_in_registry_atomic() {
  /** -- insert the child process id into the map. **/
  if (0 == pthread_rwlock_wrlock(&yappw_inst_rwlock)) {
    pthread_mutex_lock(&yapp_worker_mutex_for_pids);
    map<int, Task>::iterator itr = pid_map.begin();
    for (; itr != pid_map.end(); itr++) {
      kill(itr->first, SIGKILL);
      std::cerr << ">>>> Sending " << SIGKILL
                << " to pid: " << itr->first << std::endl;
    }
    check_point_cur_running_jobs_ex();
    pid_map.clear();
    pthread_mutex_unlock(&yapp_worker_mutex_for_pids);
    pthread_rwlock_unlock(&yappw_inst_rwlock);
  }
}

void YappWorkerServiceHandler::add_pid_to_registry_atomic(int child_pid, const Task * task_ptr) {
  /** -- insert the child process id into the map. **/
  if (NULL == task_ptr) { return; }
  pthread_mutex_lock(&yapp_worker_mutex_for_pids);
  pid_map[child_pid] = * task_ptr;
  pthread_mutex_unlock(&yapp_worker_mutex_for_pids);
}

void YappWorkerServiceHandler::del_pid_in_registry_atomic(int child_pid) {
  /** -- remove child process id from the registry. **/
  pthread_mutex_lock(&yapp_worker_mutex_for_pids);
  pid_map.erase(child_pid);
  pthread_mutex_unlock(&yapp_worker_mutex_for_pids);
}

int YappWorkerServiceHandler::get_pid_sz_registry_atomic() {
  int size = 0;
  pthread_mutex_lock(&yapp_worker_mutex_for_pids);
  size = pid_map.size();
  pthread_mutex_unlock(&yapp_worker_mutex_for_pids);
  return size;
}

void YappWorkerServiceHandler::get_tasks_group_atomic(
  map<string, vector<string> > & tskgrp_by_owner, const vector<string> & owner){
  pthread_mutex_lock(&yapp_worker_mutex_for_pids);
  tskgrp_by_owner.clear();
  int owner_cnt = owner.size();
  map<int, Task>::iterator itr = pid_map.begin();
  for (; itr != pid_map.end(); itr++) {
    bool b_owner_to_query = false;
    for (int i = 0; i < owner_cnt; i++) {
      if (owner[i] == itr->second.task_owner) {
        b_owner_to_query = true; break;
      }
    }
    if ((false == b_owner_to_query) && (owner_cnt > 0)) { continue; }
    if (tskgrp_by_owner.end() == tskgrp_by_owner.find(itr->second.task_owner)) {
      vector<string> hndl_arr;
      hndl_arr.push_back(itr->second.proc_arr.front().proc_hnd);
      tskgrp_by_owner[itr->second.task_owner] = hndl_arr;
    } else {
      tskgrp_by_owner[itr->second.task_owner].push_back(
        itr->second.proc_arr.front().proc_hnd
      );
    }
  }
  pthread_mutex_unlock(&yapp_worker_mutex_for_pids);
}

YAPP_MSG_CODE YappWorkerServiceHandler::mark_process_as_ready(
  const string & proc_hnd, string & lock_hndl)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "################# hnd: " << proc_hnd << std::endl;
#endif

  bool status = false;
  string helper_proc_hnd = proc_hnd;

  status = RangeFileTaskDataParser::get_helper_proc_hnd_in_job_node(
    proc_hnd, helper_proc_hnd
  );
  assert(true == status);

  string proc_path = zkc_proxy_ptr->get_proc_full_path(helper_proc_hnd);

#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "################# path: " << proc_path << std::endl;
#endif

  string path_to = ready_task_set.get_queue_path() + "/" + proc_hnd;
  // rc = ready_task_set.push_back_and_lock_task(proc_hnd, zkc_proxy_ptr);
  // if (YAPP_MSG_SUCCESS != rc) { return rc; }

#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "#### FINISH PUSHING TO THE READY QUEUE: "
            << std::endl << proc_hnd << std::endl;
#endif

  vector<string> path_arr;
  vector<string> data_arr;
  path_arr.push_back(proc_path + "/host_str");
  data_arr.push_back(zkc_proxy_ptr->get_host_str_in_pcb());
  path_arr.push_back(proc_path + "/cur_status"); 
  data_arr.push_back(
    StringUtil::convert_int_to_str(PROC_STATUS_CODE::PROC_STATUS_READY)
  );
/*
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  path_arr.push_back(proc_path + "/last_updated_tmstp_sec"); 
  data_arr.push_back(StringUtil::convert_int_to_str(ts.tv_sec));
*/

  rc = zkc_proxy_ptr->batch_set(path_arr, data_arr);

  if (YAPP_MSG_SUCCESS == rc) {
    /** Batch the move from NEW queue to READY queue as one atomic operation **/
    string path_from = newly_created_task_set.get_queue_path() + "/" + proc_hnd;
    rc = zkc_proxy_ptr->move_and_ex_lock_node_with_no_children(
      path_from, path_to, "", lock_hndl
    );
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
    if (YAPP_MSG_SUCCESS == rc) {
      std::cerr << "#### FINISH MOVING TO THE READY QUEUE: " << std::endl
                << "---- FROM: " << path_from << std::endl
                << "---- TO  : " << path_to   << std::endl;
    }
#endif
  }
  return rc;
}

YAPP_MSG_CODE YappWorkerServiceHandler::mark_process_as_running(
  const string & proc_hnd, int child_pid)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "################# hnd: " << proc_hnd << std::endl;
#endif
  bool status = false;

  string helper_proc_hnd = proc_hnd;
  status = RangeFileTaskDataParser::get_helper_proc_hnd_in_job_node(
    proc_hnd, helper_proc_hnd
  );
  assert(true == status);

  // rc = running_task_set.push_back_and_lock_task(proc_hnd, zkc_proxy_ptr);
  string path_to = running_task_set.get_queue_path() + "/" + proc_hnd;

  // if (YAPP_MSG_SUCCESS != rc) { return rc; }


  string proc_path = zkc_proxy_ptr->get_proc_full_path(helper_proc_hnd);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "################# path: " << proc_path << std::endl;
#endif

  vector<string> path_arr;
  vector<string> data_arr;
  path_arr.push_back(proc_path + "/host_pid");
  data_arr.push_back(StringUtil::convert_int_to_str(child_pid));
  path_arr.push_back(proc_path + "/host_str");
  data_arr.push_back(zkc_proxy_ptr->get_host_str_in_pcb());
  path_arr.push_back(proc_path + "/cur_status"); 
  data_arr.push_back(
    StringUtil::convert_int_to_str(PROC_STATUS_CODE::PROC_STATUS_RUNNING)
  );
  /** reset the startting flag so that latest log anchor will gets pulled. **/
  path_arr.push_back(proc_path + "/start_flag");
  data_arr.push_back(StringUtil::convert_int_to_str(
    g_yapp_service_constants.PROC_CTRL_BLCK_ANCHR_START)    
  );   

/*
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  path_arr.push_back(proc_path + "/last_updated_tmstp_sec"); 
  data_arr.push_back(StringUtil::convert_int_to_str(ts.tv_sec));
*/

  rc = zkc_proxy_ptr->batch_set(path_arr, data_arr);

  if (YAPP_MSG_SUCCESS == rc) {
    /** Batch the move from READY to RUNNING queue as one atomic operation **/
    string lock_hndl;
    string path_from = ready_task_set.get_queue_path() + "/" + proc_hnd;
    rc = zkc_proxy_ptr->move_and_ex_lock_node_with_no_children(
      path_from, path_to, "", lock_hndl
    );
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
    if (YAPP_MSG_SUCCESS == rc) {
      std::cerr << "#### FINISH MOVING TO THE RUNNING QUEUE: " << std::endl
                << "---- FROM: " << path_from << std::endl
                << "---- TO  : " << path_to   << std::endl;
    }
#endif
  }

  return rc;
}

YAPP_MSG_CODE YappWorkerServiceHandler::mark_process_as_blocked(
  const string & proc_hnd, int child_pid)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "################# hnd: " << proc_hnd << std::endl;
#endif
  bool status = false;

  string helper_proc_hnd = proc_hnd;
  status = RangeFileTaskDataParser::get_helper_proc_hnd_in_job_node(
    proc_hnd, helper_proc_hnd
  );
  assert(true == status);

  string path_to = paused_task_set.get_queue_path() + "/" + proc_hnd;

  string proc_path = zkc_proxy_ptr->get_proc_full_path(helper_proc_hnd);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "################# path: " << proc_path << std::endl;
#endif

  vector<string> path_arr;
  vector<string> data_arr;
  path_arr.push_back(proc_path + "/host_pid");
  data_arr.push_back(StringUtil::convert_int_to_str(child_pid));
  path_arr.push_back(proc_path + "/host_str");
  data_arr.push_back(zkc_proxy_ptr->get_host_str_in_pcb());
  path_arr.push_back(proc_path + "/cur_status"); 
  data_arr.push_back(
    StringUtil::convert_int_to_str(PROC_STATUS_CODE::PROC_STATUS_BLOCKED)
  );
/*
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  path_arr.push_back(proc_path + "/last_updated_tmstp_sec"); 
  data_arr.push_back(StringUtil::convert_int_to_str(ts.tv_sec));
*/

  rc = zkc_proxy_ptr->batch_set(path_arr, data_arr);

  if (YAPP_MSG_SUCCESS == rc) {
  /** only remove it from RUNNING queue after being applied in PAUSED queue. **/
    string lock_hndl;
    string path_from = running_task_set.get_queue_path() + "/" + proc_hnd;
    rc = zkc_proxy_ptr->move_and_ex_lock_node_with_no_children(
      path_from, path_to, "", lock_hndl
    );
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
    if (YAPP_MSG_SUCCESS == rc) {
      std::cerr << "#### FINISH MOVING TO THE PAUSED QUEUE: " << std::endl
                << "---- FROM: " << path_from << std::endl
                << "---- TO  : " << path_to   << std::endl;
    }
#endif
  }
  return rc;
}

YAPP_MSG_CODE YappWorkerServiceHandler::mark_process_as_resumed(
  const string & proc_hnd, int child_pid)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "################# hnd: " << proc_hnd << std::endl;
#endif

  bool status = false;

  string helper_proc_hnd = proc_hnd;
  status = RangeFileTaskDataParser::get_helper_proc_hnd_in_job_node(
    proc_hnd, helper_proc_hnd
  );
  assert(true == status);

  string path_to = running_task_set.get_queue_path() + "/" + proc_hnd;

  string proc_path = zkc_proxy_ptr->get_proc_full_path(helper_proc_hnd);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "################# path: " << proc_path << std::endl;
#endif

  vector<string> path_arr;
  vector<string> data_arr;
  path_arr.push_back(proc_path + "/host_pid");
  data_arr.push_back(StringUtil::convert_int_to_str(child_pid));
  path_arr.push_back(proc_path + "/host_str");
  data_arr.push_back(zkc_proxy_ptr->get_host_str_in_pcb());
  path_arr.push_back(proc_path + "/cur_status"); 
  data_arr.push_back(
    StringUtil::convert_int_to_str(PROC_STATUS_CODE::PROC_STATUS_RUNNING)
  );
/*
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  path_arr.push_back(proc_path + "/last_updated_tmstp_sec"); 
  data_arr.push_back(StringUtil::convert_int_to_str(ts.tv_sec));
*/

  rc = zkc_proxy_ptr->batch_set(path_arr, data_arr);

  if (YAPP_MSG_SUCCESS == rc) {
  /** only remove it from RUNNING queue after being applied in PAUSED queue. **/
    string lock_hndl;
    string path_from = paused_task_set.get_queue_path() + "/" + proc_hnd;
    rc = zkc_proxy_ptr->move_and_ex_lock_node_with_no_children(
      path_from, path_to, "", lock_hndl
    );
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
    if (YAPP_MSG_SUCCESS == rc) {
      std::cerr << "#### FINISH ACTIVATING TO THE RUNNING QUEUE: " << std::endl
                << "---- FROM: " << path_from << std::endl
                << "---- TO  : " << path_to   << std::endl;
    }
#endif
  }
  return rc;
}

YAPP_MSG_CODE YappWorkerServiceHandler::mark_process_as_terminated(
  const string & proc_hnd, int task_ret, int term_sig)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  bool status = false;
  string proc_hnd_in_job_node = proc_hnd;
  status = RangeFileTaskDataParser::get_running_proc_hnd_in_job_node(
    proc_hnd, proc_hnd_in_job_node
  );
  assert(true == status);
  string path_to;
  vector<string> nodes_to_unlock_and_delete;
  vector<string> nodes_to_create;
  string task_hnd =
    zkc_proxy_ptr->get_task_hnd_by_proc_hnd(proc_hnd_in_job_node);

  string tskhd_in_job, max_line_idx, tskhd_in_run,
         cur_line_idx, nxt_line_idx, tot_proc_cnt;
  string proc_path, helper_proc_hnd;

  if (proc_hnd_in_job_node == proc_hnd) {
  /** if subtask's input type is ID_RANGE, simply duck them to termin. queue **/
    if (0 == task_ret) {
      // rc = termin_task_set.push_back_task_queue(proc_hnd, zkc_proxy_ptr);
      path_to = termin_task_set.get_queue_path() + "/" + proc_hnd;
    } else {
      // rc = failed_task_set.push_back_task_queue(proc_hnd, zkc_proxy_ptr);
      path_to = failed_task_set.get_queue_path() + "/" + proc_hnd;
    }
    proc_path = zkc_proxy_ptr->get_proc_full_path(proc_hnd_in_job_node);
    helper_proc_hnd = proc_hnd_in_job_node;
  } else {
  /** for tasks with RANGE_FILE input, only records under its specific path.**/
    if (0 == task_ret) {
    /** successfully returned, then remove it from its own running queue **/
      nodes_to_unlock_and_delete.push_back(
        zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
        task_hnd + "/running_procs/" + proc_hnd
      );
    } else {
    /** faulty returned, then also add it to its own failed queue **/
      nodes_to_create.push_back(
        zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
        task_hnd + "/failed_procs/" + proc_hnd
      );
      nodes_to_create.push_back(
        failed_task_set.get_queue_path() + "/" + proc_hnd
      );
    }
    RangeFileTaskDataParser::parse_rfile_task_data_str(
      proc_hnd, tskhd_in_job, max_line_idx, tskhd_in_run,
                cur_line_idx, nxt_line_idx, tot_proc_cnt
    );
    proc_path = zkc_proxy_ptr->get_proc_full_path(tskhd_in_job);
    helper_proc_hnd = tskhd_in_job;
  }

  vector<string> path_arr;
  vector<string> data_arr;
  path_arr.push_back(proc_path + "/host_str");
  data_arr.push_back(zkc_proxy_ptr->get_host_str_in_pcb());
  path_arr.push_back(proc_path + "/cur_status"); 
  data_arr.push_back(
    StringUtil::convert_int_to_str(PROC_STATUS_CODE::PROC_STATUS_TERMINATED)
  );

  if (proc_hnd_in_job_node != proc_hnd) {
    int cur_ln_idx = atoll(cur_line_idx.c_str());
    int tot_pr_cnt = atoll(tot_proc_cnt.c_str());
    int max_ln_idx = atoll(max_line_idx.c_str());
    string cur_proc_path = zkc_proxy_ptr->get_proc_full_path(tskhd_in_run);
    if (cur_ln_idx + tot_pr_cnt > max_ln_idx) {
      path_arr.push_back(cur_proc_path + "/proc_hnd"); 
      data_arr.push_back(proc_hnd);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      std::cerr << ">>>> SET: " << cur_proc_path + "/proc_hnd" << std::endl;
      std::cerr << "     TOB: " << proc_hnd << std::endl;
#endif
    }
  }

  path_arr.push_back(proc_path + "/return_val"); 
  data_arr.push_back(StringUtil::convert_int_to_str(task_ret));
  path_arr.push_back(proc_path + "/terminated_signal"); 
  data_arr.push_back(StringUtil::convert_int_to_str(term_sig));

  /** only update anchor pos by from log upon failure, otherwise re-init it. **/
  if (0 != task_ret) {
    TaskHandleUtil::update_cur_anchor_pos(
      helper_proc_hnd, zkc_proxy_ptr, cur_line_idx,
      g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_ANCHOR_POS
    );
  } else {
    path_arr.push_back(proc_path + "/cur_anchor"); 
    data_arr.push_back(
      g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_ANCHOR_POS
    );
  }

/*
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  path_arr.push_back(proc_path + "/last_updated_tmstp_sec"); 
  data_arr.push_back(StringUtil::convert_int_to_str(ts.tv_sec));
*/

  rc = zkc_proxy_ptr->batch_set(path_arr, data_arr);

  if (YAPP_MSG_SUCCESS == rc) {
    string path_from = running_task_set.get_queue_path() + "/" + proc_hnd;
    if (proc_hnd_in_job_node == proc_hnd) {
      /** only remove it from RUNNING queue after being marked as TERMINATED. **/
      // running_task_set.unlock_and_remv_from_task_queue(proc_hnd, zkc_proxy_ptr);
      rc = zkc_proxy_ptr->move_node_with_no_children(path_from, path_to, "");
    } else {
      // if (0 == task_ret) {
      /** successfully returned, then remove it from its own running queue **/
      nodes_to_unlock_and_delete.push_back(path_from);
      // }
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      if (YAPP_MSG_SUCCESS == rc) {
        std::cerr << "#### BEGIN TO DELETE FROM THE RUNNING QUEUE: " << std::endl
                  << "---- ULDEL: " << std::endl;
        for (size_t i = 0; i < nodes_to_unlock_and_delete.size(); i++) {
          std::cerr << nodes_to_unlock_and_delete[i] << std::endl;
        }
        std::cerr << "---- CREAT: " << std::endl;
        for (size_t i = 0; i < nodes_to_create.size(); i++) {
          std::cerr << nodes_to_create[i] << std::endl;
        }
      }
#endif
      rc = zkc_proxy_ptr->batch_unlock_delete_and_create(
        nodes_to_unlock_and_delete, nodes_to_create
      );
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
      if (YAPP_MSG_SUCCESS == rc) {
        std::cerr << "#### FINISH DELETE FROM THE RUNNING QUEUE: " << std::endl
                  << "---- ULDEL: " << std::endl;
        for (size_t i = 0; i < nodes_to_unlock_and_delete.size(); i++) {
          std::cerr << nodes_to_unlock_and_delete[i] << std::endl;
        }
        std::cerr << "---- CREAT: " << std::endl;
        for (size_t i = 0; i < nodes_to_create.size(); i++) {
          std::cerr << nodes_to_create[i] << std::endl;
        }
      }
#endif
    }
  }
  return rc;
}

bool YappWorkerServiceHandler::wait_for_sub_task_completion(pid_t child_pid,
                                                            int & task_ret,
                                                            int & task_sig,
                                                            bool b_verbose)
{
  bool b_status = true;
  int status;
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "==>> Start waitpid, ret: " << task_ret
            << " sig: " << task_sig << std::endl;
#endif
  do {
    int w = waitpid(child_pid, &status, WUNTRACED | WCONTINUED);
    if (w == -1) {
      std::cerr << "-- Error happend when waiting for child process: "
                << child_pid
                << " with parent process: "
                << getpid() << std::endl;
      b_status = false;
      break;
    }
    if (WIFEXITED(status)) {
      task_ret = WEXITSTATUS(status);
      if (true == b_verbose) {
        std::cout << "-- child process: " << child_pid
                  << " terminated with returned value of "
                  << task_ret << std::endl;
      } 
    } else if (WIFSIGNALED(status)) {
      if (true == b_verbose) {
        task_sig = WTERMSIG(status);
        std::cout << "-- child process: " << child_pid
                  << " got killed by signal "
                  <<  task_sig << std::endl;
      }
    } else if (WIFSTOPPED(status)) {
      if (true == b_verbose) {
        std::cout << "-- child process: " << child_pid
                  << " stopped by signal "
                  << WSTOPSIG(status) << std::endl;
      }
    } else if (WIFCONTINUED(status)) {
      if (true == b_verbose) {
        std::cout << "-- child process: " << child_pid
                  << " resumed by SIGCONT" << std::endl;
      }
    }
  } while (!WIFEXITED(status) && !WIFSIGNALED(status));
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "==>> Finish waitpid, ret: " << task_ret
            << " sig: " << task_sig << std::endl;
#endif
  return b_status;
}

YAPP_MSG_CODE YappWorkerServiceHandler::notify_master_proc_completion (
  const Task & sub_task, int task_ret, int signal)
{
  YAPP_MSG_CODE ret_val = YAPP_MSG_SUCCESS;
  return ret_val;
}

void YappWorkerServiceHandler::signal_sub_task_async_rpc(
        vector<bool>   & ret_arr,      const vector<int> & pid_arr,
  const vector<string> & proc_hnd_arr, const int signal)
{
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "$$$$ ENTER YappWorkerServiceHandler::signal_sub_task_async_rpc:" << std::endl;
#endif
  ret_arr.resize(pid_arr.size(), false);
  int  hndl_arr_size = proc_hnd_arr.size();
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (pid_arr.size() != proc_hnd_arr.size()) { return; }
  for (int i = 0; i < hndl_arr_size; i++) {
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
    std::cerr << ">>>> Going to Signal Proc: " << pid_arr[i]
              << " Using Value " << signal << std::endl;
#endif
    kill(pid_arr[i], signal);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
//    std::cerr << ">>>> RC After Signaling: " << cur_ret
//              << " ERRNO: " << errno << std::endl;
#endif
    /** THANKS TO THE FACT THAT errno IS THREAD-LOCAL! **/
    // if (0 != cur_ret && ESRCH != errno) { continue; }
    switch(signal) {
      case SIGSTOP:
        rc = mark_process_as_blocked(proc_hnd_arr[i], pid_arr[i]); break;
      case SIGCONT:
        rc = mark_process_as_resumed(proc_hnd_arr[i], pid_arr[i]); break;
      case SIGKILL:
        rc = YAPP_MSG_SUCCESS; break;
      default: break;
    }
    if (YAPP_MSG_SUCCESS == rc) { ret_arr[i] = true; }
  }
}

YAPP_MSG_CODE YappWorkerServiceHandler::locate_master_node() {
  YAPP_MSG_CODE ret_val = YAPP_MSG_INVALID;
  string ret_host, ret_port, ret_pcid;
  if (NULL == zkc_proxy_ptr) { return ret_val; }
  ret_val = zkc_proxy_ptr->get_yapp_master_addr(ret_host, ret_port, ret_pcid);
  if (YAPP_MSG_SUCCESS != ret_val) { return ret_val; }
  master_host = ret_host;
  master_port = atol(ret_port.c_str());
  return ret_val;
}

bool YappWorkerServiceHandler::set_max_concur_task(int max) {
  bool status = false;
  if (max >= 0) {
    pthread_mutex_lock(&yapp_worker_mutex);
    max_concur_task = max;
    pthread_mutex_unlock(&yapp_worker_mutex);
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
    std::cerr << ">>>> Worker Obj for Child Proc. " << getpid()
              << " Reset Max Concur Task to : " << max << std::endl;
#endif
  }
  return status;
}

YAPP_MSG_CODE YappWorkerServiceHandler::check_resources_allocation() {
  YAPP_MSG_CODE status = YAPP_MSG_SUCCESS;
  if (yapp_worker_child_procs_cnt >= max_concur_task) {
    status = YAPP_MSG_INVALID_EXCEED_MAX_JOB_CNT;
  }
  return status;
}
