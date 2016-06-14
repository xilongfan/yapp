#include <map>

#include <unistd.h>
#include <signal.h>
#include <pthread.h>

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <semaphore.h>

#include "yapp_service_handler.h"

using std::map;
using namespace yapp::domain;

static pthread_rwlock_t yappd_inst_rwlock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_mutex_t yapp_inst_mutex = PTHREAD_MUTEX_INITIALIZER;

bool YappServiceHandler::join_yapp_nodes_group() {
  bool status = false;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- process: " << getpid()
            << " start to join the yapp service group"
            << std::endl;
  std::cerr.flush();
#endif
  if (NULL != zkc_proxy_ptr)
  {
    YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
    rc = zkc_proxy_ptr->init_zk_conn(zk_conn_str);
    if (YAPP_MSG_SUCCESS == rc)
    {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "-- process: " << getpid() << " succeeded in init zk conn." << std::endl;
#endif
      rc = zkc_proxy_ptr->run_master_election(b_master_preferred);
      if (YAPP_MSG_SUCCESS == rc)
      {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
        std::cerr << "-- process: " << getpid()
                  << " succeeded in run master election." << std::endl;
#endif
        /**
         * Every time an yapp server instance joins or re-joins yapp server
         * node group, it will needs to performs the following logics:
         * - if it is a new joining, then the instance runs as master will
         *   needs to initiate a detached thread for doing the re-scheduling
         *   queued jobs periodically.
         * - if it is an action of re-joining(due to temporary network traffic
         *   or partition), the master instance will finish the thread when it
         *   detects the changes everytime it performs the status checking
         *   when it step down from master to worker and the new master also
         *   needs to start a new scheduling thread.
         * Bottom Line:
         * - Every task got queued(within a certain threshold) should be able
         *   to get scheduled and should not be lost after sending ACK to user
         */
        int ret = 0;
        status = yapp_worker_srv_obj.sync_task_queue_path();
        if (YAPP_MODE_MASTER == zkc_proxy_ptr->get_yapp_mode())
        {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          std::cerr << "-- master process: " << getpid()
                    << " succeeded in doing sync." << std::endl;
#endif
          rc = yapp_master_srv_obj.sync_newtsk_queue();
          status = sync_task_queue_path();
          if (YAPP_MSG_SUCCESS == rc && true == status) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
            std::cerr << "-- master process: " << getpid()
                      << " succeeded in init queue." << std::endl;
#endif
            if (0 == start_all_master_threads()) {
              status = true;
            } else {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
              std::cerr << "-- process: " << getpid()
                        << " failed init the master threads for yapp service"
                        << std::endl;
#endif
              stop_all_master_threads();
            }
          } else {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
            std::cerr << "-- process: " << getpid()
                      << " failed in joining the yapp service group,"
                      << " (cannot load the new tasks queue) "
                      << " running as " << zkc_proxy_ptr->get_yapp_mode()
                      << std::endl;
            std::cerr.flush();
#endif
          }
        }
        if (YAPP_SERVICE_THREAD_ID_INVALID == node_lvchk_thrd_id && status) {
          ret |= pthread_create(&node_lvchk_thrd_id, NULL,
                                 thread_node_leave_check, this);
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          if (0 != ret) {
            status = false;
            std::cerr << "-- process: " << getpid()
                      << " failed in creating the node_leaving_check thread,"
                      << " running as " << zkc_proxy_ptr->get_yapp_mode()
                      << std::endl;
            std::cerr.flush();
          } else {
             std::cerr << "-- process: " << getpid()
                      << " succeeded in creating the node_leaving_check thread,"
                      << " running as " << zkc_proxy_ptr->get_yapp_mode()
                      << std::endl;
             std::cerr.flush();
          }
#endif
        } else {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          if (!status) {
            std::cerr << "-- process: " << getpid()
                      << " failed at previous chk" << std::endl;
          } else {
            std::cerr << "-- process: " << getpid()
                      << " already having the node_leaving_check thread,"
                      << " running as " << zkc_proxy_ptr->get_yapp_mode()
                      << std::endl;
          }
          std::cerr.flush();
#endif 
        }
        if (YAPP_SERVICE_THREAD_ID_INVALID == check_point_thrd_id && status) {
          ret |= pthread_create(&check_point_thrd_id, NULL,
                                 thread_check_point_cur_anchor, this);
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          if (0 != ret) {
            status = false;
            std::cerr << "-- process: " << getpid()
                      << " failed in creating the check_point_cur_anchor thread,"
                      << " running as " << zkc_proxy_ptr->get_yapp_mode()
                      << std::endl;
            std::cerr.flush();
          } else {
             std::cerr << "-- process: " << getpid()
                      << " succeeded in creating the check_point_cur_anchor thread,"
                      << " running as " << zkc_proxy_ptr->get_yapp_mode()
                      << std::endl;
             std::cerr.flush();
          }
#endif
        } else {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          if (!status) {
            std::cerr << "-- process: " << getpid()
                      << " failed at previous chk" << std::endl;
          } else {
            std::cerr << "-- process: " << getpid()
                      << " already having the check_point_cur_anchor thread,"
                      << " running as " << zkc_proxy_ptr->get_yapp_mode()
                      << std::endl;
          }
          std::cerr.flush();
#endif 
        }
#ifdef DEBUG_YAPP_SERVICE_HANDLER
        if (true == status) {
          std::cerr << "-- process: " << getpid()
                    << " succeeded in joining the yapp service group,"
                    << " running as " << zkc_proxy_ptr->get_yapp_mode()
                    << std::endl;
        } else {
          std::cerr << "-- cannot setup env failed joining group" << std::endl;
        }
        std::cerr.flush();
#endif
      } else {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
        std::cerr << "-- cannot run Election failed joining group" << std::endl;
#endif
      }
    } else {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "-- cannot init. zkc, failed in joining group." << std::endl;
#endif
    }
  } else {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
    std::cerr << "-- nil zkc connection, failed in joining group." << std::endl;
#endif
  }
  return status;
}

bool YappServiceHandler::chk_bflag_atomic(bool * flag_ptr) {
  bool b_val = false;
  if (NULL == flag_ptr) { return b_val; }
  if (0 != pthread_rwlock_rdlock(&yappd_inst_rwlock)) {
    std::cerr << ">>>> Error Happend In Grabbing RD Lock." << std::endl;
    return false;
  }
  b_val = * flag_ptr;
  if (0 != pthread_rwlock_unlock(&yappd_inst_rwlock)) {
    std::cerr << ">>>> Error Happend In Releasing RD Lock." << std::endl;
    return false;
  }
  return b_val;
}

bool YappServiceHandler::apply_dynamic_conf_atomic() {
  if (0 != pthread_rwlock_wrlock(&yappd_inst_rwlock)) {
    std::cerr << ">>>> Error Happend Grabbing WR Lock for Reload" << std::endl;
    return false;
  }

  rftask_scheule_polling_rate_sec   = ypconf_obj.rftask_scheule_polling_rate_sec;
  rftask_autosp_polling_rate_sec    = ypconf_obj.rftask_autosp_polling_rate_sec;
  subtask_scheule_polling_rate_sec  = ypconf_obj.subtask_scheule_polling_rate_sec;
  zombie_check_polling_rate_sec     = ypconf_obj.zombie_check_polling_rate_sec;
  utility_thread_checking_rate_sec  = ypconf_obj.utility_thread_checking_rate_sec;
  node_leave_check_polling_rate_sec = ypconf_obj.master_check_polling_rate_sec;
  check_point_polling_rate_sec      = ypconf_obj.check_point_polling_rate_sec;

  fencing_script_path = ypconf_obj.fencing_script_path;

#ifdef DEBUG_YAPP_DAEMON_APP
  std::cerr << "---- Set Fencing Script: " << fencing_script_path << std::endl;
  std::cerr << "---- Set rftask_scheule_polling_rate_sec : "
            << rftask_scheule_polling_rate_sec << std::endl;
  std::cerr << "---- Set rftask_autosp_polling_rate_sec: "
            << rftask_autosp_polling_rate_sec << std::endl;
  std::cerr << "---- Set subtask_scheule_polling_rate_sec: "
            << subtask_scheule_polling_rate_sec << std::endl;
  std::cerr << "---- Set zombie_check_polling_rate_sec: "
            << zombie_check_polling_rate_sec << std::endl;
  std::cerr << "---- Set utility_thread_checking_rate_sec: "
            << utility_thread_checking_rate_sec << std::endl;
  std::cerr << "---- Set node_leave_check_polling_rate_sec: "
            << node_leave_check_polling_rate_sec << std::endl;
  std::cerr << "---- Set check_point_polling_rate_sec: "
            << check_point_polling_rate_sec << std::endl;
#endif

  yapp_master_srv_obj.set_max_queued_task(ypconf_obj.max_queued_task);
  yapp_worker_srv_obj.set_max_concur_task(ypconf_obj.thrd_pool_size);

  if (0 != pthread_rwlock_unlock(&yappd_inst_rwlock)) {
    std::cerr << ">>>> Error Happend Releasing WR Lock for Reload" << std::endl;
    return false;
  }
  return true;
}


bool YappServiceHandler::set_bflag_atomic(bool * flag_ptr, bool val) {
  if (NULL == flag_ptr) { return false; }
  if (0 != pthread_rwlock_wrlock(&yappd_inst_rwlock)) {
    std::cerr << ">>>> Error Happend In Grabbing WR Lock." << std::endl;
    return false;
  }
  * flag_ptr = val;
  if (0 != pthread_rwlock_unlock(&yappd_inst_rwlock)) {
    std::cerr << ">>>> Error Happend In Releasing WR Lock." << std::endl;
    return false;
  }
  return true;
}

bool YappServiceHandler::get_intvl_atomic(int * ptr, int * ret) {
  if (NULL == ptr || NULL == ret) { return false; }
  if (0 != pthread_rwlock_rdlock(&yappd_inst_rwlock)) {
    std::cerr << ">>>> Error Happend In Grabbing WR Lock." << std::endl;
    return false;
  }
  * ret = * ptr;
  if (0 != pthread_rwlock_unlock(&yappd_inst_rwlock)) {
    std::cerr << ">>>> Error Happend In Releasing WR Lock." << std::endl;
    return false;
  }
  return true;
}

void * YappServiceHandler::thread_check_point_cur_anchor(void * srv_ptr) {
  YappServiceHandler * ym_srv_ptr = (YappServiceHandler *)srv_ptr;
  bool b_exit = false;
  if (NULL != ym_srv_ptr) {
    while (false == b_exit) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      if (YAPP_MODE_WORKER == ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) {
        std::cerr << std::endl << "-- START LOG CHECKING IN THREAD: "
                  << pthread_self() << " ON WORKER INSTANCE: "
                  << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
                  << std::endl;
      } else {
        std::cerr << std::endl << "-- START LOG CHECKING IN THREAD: "
                  << pthread_self() << " ON MASTER INSTANCE: "
                  << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
                  << std::endl;
      }
#endif
      check_point_cur_anchor(ym_srv_ptr);
      int intval = 30;
      ym_srv_ptr->get_intvl_atomic(&(ym_srv_ptr->check_point_polling_rate_sec),&intval);
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "---- thread_check_point_cur_anchor Polling Rate: "
                << intval << " secs." << std::endl;
#endif
      for (int i = 0; i < intval; i++) {
        if (ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_exit)))
        { b_exit = true; break; } else { sleep(1); }
      }
    } // while (false == b_exit)
  }
  if (true == b_exit) {
    std::cout << "---- thread_check_point_cur_anchor terminated." << std::endl;
  }
  return NULL;
}

void YappServiceHandler::check_point_cur_anchor(YappServiceHandler * ym_srv_ptr) {
  return ym_srv_ptr->yapp_worker_srv_obj.check_point_cur_running_jobs();
}

void * YappServiceHandler::thread_node_leave_check(void * srv_ptr) {
  /** here, it does not detach itself since event handler will join it **/
  YappServiceHandler * ym_srv_ptr = (YappServiceHandler *)srv_ptr;
  bool b_exit = false;
  if (NULL != ym_srv_ptr) {
    while (false == b_exit) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      if (YAPP_MODE_WORKER == ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) {
      /** the instance runs as a master would check for rescheduling **/
        std::cerr << std::endl << "-- START MASTER CHECKING IN THREAD: "
                  << pthread_self() << " ON WORKER INSTANCE: "
                  << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
                  << std::endl;
      } else {
        std::cerr << std::endl << "-- START MASTER CHECKING IN THREAD: "
                  << pthread_self() << " ON MASTER INSTANCE: "
                  << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
                  << std::endl;
      }
#endif
      node_leave_check(ym_srv_ptr);
      int intval = 30;
      ym_srv_ptr->get_intvl_atomic(&(ym_srv_ptr->node_leave_check_polling_rate_sec),&intval);
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "---- thread_node_leave_check Polling Rate: "
                << intval << " secs." << std::endl;
#endif
      for (int i = 0; i < intval; i++) {
        if (ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_exit)))
        { b_exit = true; break; } else { sleep(1); }
      }
    } // while (false == b_exit)
  }
  if (true == b_exit) {
    std::cout << "---- thread_node_leave_check terminated." << std::endl;
  }
  return NULL;

}

int YappServiceHandler::start_all_master_threads() {
  int ret = 0;
  ret |= pthread_create(&split_rftk_thrd_id, NULL,
                         thread_auto_split_rfile_tasks, this);
  ret |= pthread_create(&reschedule_thrd_id, NULL,
                         thread_reschedule_tasks, this);
  ret |= pthread_create(&checktasks_thrd_id, NULL,
                         thread_check_zombie_task, this);
  ret |= pthread_create(&schedule_rfthrd_id, NULL,
                         thread_schedule_rfile_tasks, this);
  ret |= pthread_create(&check_util_thrd_id, NULL,
                         thread_check_utility_thread_status, this);
  return ret;
}

void YappServiceHandler::stop_all_master_threads() {
  if (set_bflag_atomic(&b_flag_thrd_stop, true)) {
    if (check_util_thrd_id != YAPP_SERVICE_THREAD_ID_INVALID) {
      pthread_join(check_util_thrd_id, NULL);
    }
    if (reschedule_thrd_id != YAPP_SERVICE_THREAD_ID_INVALID) {
      pthread_join(reschedule_thrd_id, NULL);
    }
    if (checktasks_thrd_id != YAPP_SERVICE_THREAD_ID_INVALID) {
      pthread_join(checktasks_thrd_id, NULL);
    }
    if (schedule_rfthrd_id != YAPP_SERVICE_THREAD_ID_INVALID) {
      pthread_join(schedule_rfthrd_id, NULL);
    }
    if (split_rftk_thrd_id != YAPP_SERVICE_THREAD_ID_INVALID) {
      pthread_join(split_rftk_thrd_id, NULL);
    }
    check_util_thrd_id = YAPP_SERVICE_THREAD_ID_INVALID;
    reschedule_thrd_id = YAPP_SERVICE_THREAD_ID_INVALID;
    checktasks_thrd_id = YAPP_SERVICE_THREAD_ID_INVALID;
    schedule_rfthrd_id = YAPP_SERVICE_THREAD_ID_INVALID;
    split_rftk_thrd_id = YAPP_SERVICE_THREAD_ID_INVALID;
  }
  set_bflag_atomic(&b_flag_thrd_stop, false);
}

bool YappServiceHandler::node_leave_check(YappServiceHandler * ym_srv_ptr) {
  bool status = false;
  if (NULL == ym_srv_ptr || NULL == ym_srv_ptr->zkc_proxy_ptr){ return status; }
  YAPP_RUNNING_MODE_CODE prev_mode = ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode();
  string prev_node = ym_srv_ptr->zkc_proxy_ptr->get_node_path();

  /** 1. check if its lease expired or session expired, re-join if needed. **/
  YAPP_MSG_CODE rc = ym_srv_ptr->zkc_proxy_ptr->node_exists(prev_node);
  if (YAPP_MSG_INVALID_NONODE == rc || YAPP_MSG_INVALID_ZKC_HNDLE_STATE == rc){
    if (YAPP_MODE_MASTER == prev_mode) {
      /** enter here means master step down, stop all procs. if needed. **/
      if (ym_srv_ptr->set_bflag_atomic(&(ym_srv_ptr->b_flag_master_step_down), true)) {
        ym_srv_ptr->stop_all_master_threads();
      }
      ym_srv_ptr->set_bflag_atomic(&(ym_srv_ptr->b_flag_master_step_down), false);
    }// else if (YAPP_MODE_WORKER == prev_mode) {
    /** here we will do a best effort to kill all jobs already lost the HB.**/
    if (YAPP_MSG_INVALID_ZKC_HNDLE_STATE == rc) {
      ym_srv_ptr->yapp_worker_srv_obj.kil_pid_in_registry_atomic();
    }
    //}
    status = ym_srv_ptr->join_yapp_nodes_group();
  } else if (YAPP_MSG_SUCCESS == rc) {
  /** 2. its lease and session not expired, worker check any master changes. **/
    if (YAPP_MSG_SUCCESS == ym_srv_ptr->zkc_proxy_ptr->config_node_info()) {
    /** 1. If succeeded in syncing the nodes info., then perform checking. **/
      status = true;
      if (YAPP_MODE_MASTER == ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) {
        if (YAPP_MODE_WORKER == prev_mode) {
        /** 1-1. enter here means worker gain master, start procs. if needed. **/
          if (YAPP_MSG_SUCCESS == ym_srv_ptr->zkc_proxy_ptr->sync(ym_srv_ptr->
                                    zkc_proxy_ptr->get_queue_path_prefix()) &&
              YAPP_MSG_SUCCESS == ym_srv_ptr->
                                    get_master_srv_ref().sync_newtsk_queue() &&
              true == ym_srv_ptr->sync_task_queue_path())
          {
            if (0 == ym_srv_ptr->start_all_master_threads()) {
              std::cerr << "Worker "
                        << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
                        << " Gain the Mastership." << std::endl;
            } else {
              status = false;
            }
          } else {
            status = false;
          }
        }
      } else {
        if (YAPP_MODE_MASTER == prev_mode) {
          if (ym_srv_ptr->set_bflag_atomic(&(ym_srv_ptr->b_flag_master_step_down), true)) {
            ym_srv_ptr->stop_all_master_threads();
          }
          ym_srv_ptr->set_bflag_atomic(&(ym_srv_ptr->b_flag_master_step_down), false);
          std::cerr << "Master "
                    << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
                    << " Step Down." << std::endl;
        }
      }
    } else {
      std::cerr << "Operation Failed In Checking Master Chnage." << std::endl;
    }
  } else {
    std::cerr << "Operation Failed In Checking The Lease." << std::endl;
  }

  return status;
}
bool YappServiceHandler::check_utility_thread_status(
  YappServiceHandler * ym_srv_ptr)
{
  /**
   * ESRCH:  No thread could be found corresponding to the given thread ID.
   * EINVAL: The signal is an invalid or unsupported signal number.
   * NOTE:
   * - if sig is zero, error checking shall be performed but no signal shall
   *   actually be sent.
   */
  int ret = 0;
  if ((YAPP_SERVICE_THREAD_ID_INVALID != ym_srv_ptr->reschedule_thrd_id) &&
      (ESRCH == pthread_kill(ym_srv_ptr->reschedule_thrd_id, 0))) {
    ret |= pthread_create(&(ym_srv_ptr->reschedule_thrd_id), NULL,
                            thread_reschedule_tasks, ym_srv_ptr);
  }
  if ((YAPP_SERVICE_THREAD_ID_INVALID != ym_srv_ptr->checktasks_thrd_id) &&
      (ESRCH == pthread_kill(ym_srv_ptr->checktasks_thrd_id, 0))) {
    ret |= pthread_create(&(ym_srv_ptr->checktasks_thrd_id), NULL,
                            thread_check_zombie_task, ym_srv_ptr);
  }
  if ((YAPP_SERVICE_THREAD_ID_INVALID != ym_srv_ptr->schedule_rfthrd_id) &&
      (ESRCH == pthread_kill(ym_srv_ptr->schedule_rfthrd_id, 0))) {
    ret |= pthread_create(&(ym_srv_ptr->schedule_rfthrd_id), NULL,
                            thread_schedule_rfile_tasks, ym_srv_ptr);
  }
  if ((YAPP_SERVICE_THREAD_ID_INVALID != ym_srv_ptr->split_rftk_thrd_id) &&
      (ESRCH == pthread_kill(ym_srv_ptr->split_rftk_thrd_id, 0))) {
    ret |= pthread_create(&(ym_srv_ptr->split_rftk_thrd_id), NULL,
                            thread_auto_split_rfile_tasks, ym_srv_ptr);
  }
  return (YAPP_MSG_SUCCESS == ret);
}

void * YappServiceHandler::thread_check_utility_thread_status(void * srv_ptr) {
// check_util_thrd_id 
  YappServiceHandler * ym_srv_ptr = (YappServiceHandler *)srv_ptr;
  bool b_exit = false;
#ifdef DEBUG_YAPP_JOB_SUBMISSION
  std::cerr << std::endl << "-- STARTING UTILITY CHECKING THREAD: "
            << pthread_self() << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  if (NULL != ym_srv_ptr) {
    while (false == b_exit) {
      if (YAPP_MODE_MASTER == ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) {
        /**
         * using a mutex to avoid mutiple thread running at the same time which
         * may cause a fsync spike on the zookeeper.
         */
        pthread_mutex_lock(&yapp_inst_mutex);
        check_utility_thread_status(ym_srv_ptr);
        pthread_mutex_unlock(&yapp_inst_mutex);
      } else { break; }

      int intval = 30;
      ym_srv_ptr->get_intvl_atomic(
        &(ym_srv_ptr->utility_thread_checking_rate_sec),&intval
      );
#ifdef DEBUG_YAPP_JOB_SUBMISSION
      std::cerr << "---- thread_check_utility_thread_status Polling Rate: "
                << intval << " secs." << std::endl;
#endif
      for (int i = 0; i < intval; i++) {
        if (ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_thrd_stop)) ||
            ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_exit)))
        { b_exit = true; break; } else { sleep(1); }
      }
    }
  }
  if (true == b_exit) {
    std::cout << "---- thread_check_utility_thread_status terminated." << std::endl;
  }
#ifdef DEBUG_YAPP_JOB_SUBMISSION
  std::cerr << "-- TERMINATING UTILITY CHECKING THREAD: " << pthread_self()
            << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  return NULL;
}

void * YappServiceHandler::thread_schedule_rfile_tasks(void * srv_ptr)
{
  // pthread_detach(pthread_self());
  YappServiceHandler * ym_srv_ptr = (YappServiceHandler *)srv_ptr;
  bool b_exit = false;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << std::endl << "-- STARTING RFILE TASK SCHEDULING THREAD: "
            << pthread_self() << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  if (NULL != ym_srv_ptr) {
    while (false == b_exit) {
      if (YAPP_MODE_MASTER == ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) {
        /**
         * using a mutex to avoid mutiple thread running at the same time which
         * may cause a fsync spike on the zookeeper.
         */
        pthread_mutex_lock(&yapp_inst_mutex);
        // schedule_rfile_tasks(ym_srv_ptr);
        schedule_rfile_tasks_in_batch(ym_srv_ptr);
        pthread_mutex_unlock(&yapp_inst_mutex);
      } else { break; }

      int intval = 30;
      ym_srv_ptr->get_intvl_atomic(&(ym_srv_ptr->rftask_scheule_polling_rate_sec),&intval);
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "---- thread_schedule_rfile_tasks Polling Rate: "
                << intval << " secs." << std::endl;
#endif
      for (int i = 0; i < intval; i++) {
        if (ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_thrd_stop)) ||
            ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_exit)))
        { b_exit = true; break; } else { sleep(1); }
      }
    } // while (false == b_exit)
  }
  if (true == b_exit) {
    std::cout << "---- thread_schedule_rfile_tasks terminated." << std::endl;
  }
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- TERMINATING RFILE TASK SCHEDULING THREAD: " << pthread_self()
            << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  return NULL;

}

bool YappServiceHandler::fetch_queue_and_filter_failed_rftasks(
  YappServiceHandler * ym_srv_ptr,
  vector<string> & rf_running_subtsk_arr, vector<string> & rf_subtask_arr,
  vector<string> & rf_failed_subtsk_arr, YappSubtaskQueue & rf_running_subtask_queue,
  YappSubtaskQueue & rf_subtask_queue, YappSubtaskQueue & rf_failed_task_queue
)
{
  vector<string> tp_rf_subtask_arr, tp_rf_running_subtsk_arr;

  int rc = rf_running_subtask_queue.get_task_queue(tp_rf_running_subtsk_arr, ym_srv_ptr->zkc_proxy_ptr);
  if (YAPP_MSG_SUCCESS != rc) { return false; }
  rc = rf_failed_task_queue.get_task_queue(rf_failed_subtsk_arr, ym_srv_ptr->zkc_proxy_ptr);
  if (YAPP_MSG_SUCCESS != rc) { return false; }
  rc = rf_subtask_queue.get_task_queue(tp_rf_subtask_arr, ym_srv_ptr->zkc_proxy_ptr);
  if (YAPP_MSG_SUCCESS != rc) { return false; }

  /** needs to clean the task arr for those failed tasks **/
  int failed_cnt = rf_failed_subtsk_arr.size();
  if (0 < failed_cnt) {
    bool matched = false;
    int  task_size = tp_rf_running_subtsk_arr.size();
    for (int c = 0; c < task_size; c++) {
      matched = false;
      for (int i = 0; i < failed_cnt; i++) {
        if (rf_failed_subtsk_arr[i] == tp_rf_running_subtsk_arr[c]){
          matched = true;
          break;
        }
      }
      if (false == matched) {
        rf_running_subtsk_arr.push_back(tp_rf_running_subtsk_arr[c]);
      }
    }
    task_size = tp_rf_subtask_arr.size();
    for (int c = 0; c < task_size; c++) {
      matched = false;
      for (int i = 0; i < failed_cnt; i++) {
        if (0 == tp_rf_subtask_arr[c].find(rf_failed_subtsk_arr[i])){
          matched = true;
          break;
        }
      }
      if (false == matched) {
        rf_subtask_arr.push_back(tp_rf_subtask_arr[c]);
      }
    }
  } else {
    rf_running_subtsk_arr = tp_rf_running_subtsk_arr;
    rf_subtask_arr = tp_rf_subtask_arr;
  }
  
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- TRYING TO CHECK RANGE FILE TASKS IN QUEUE: "
            << rf_subtask_queue.get_queue_path() << std::endl
            << "-- THREAD: " << pthread_self() << " IN MASTER INSTANCE. "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
  std::cerr << ">> RANGE FILE TASKS: " << std::endl;
  for (size_t x = 0; x < rf_subtask_arr.size(); x++) {
    std::cerr << rf_subtask_arr[x] << " " << std::endl;
  }
  std::cerr << std::endl;
#endif
  return true;
}

void YappServiceHandler::setup_rftasks_queue_path(YappServiceHandler * ym_srv_ptr,
                                                  YappSubtaskQueue & rf_running_subtask_queue,
                                                  YappSubtaskQueue & rf_subtask_queue,
                                                  YappSubtaskQueue & rf_failed_task_queue,
                                                  const string & rf_task_to_schedule)
{
  /** setup the default path for all related queues under certain task */
  rf_running_subtask_queue.set_queue_path(
    ym_srv_ptr->zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
    rf_task_to_schedule + "/running_procs"
  );
  rf_subtask_queue.set_queue_path(
    ym_srv_ptr->zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
    rf_task_to_schedule + "/proc_arr"
  );
  rf_failed_task_queue.set_queue_path(
    ym_srv_ptr->zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
    rf_task_to_schedule + "/failed_procs"
  );
}

/**
 * Inputs:
 *   rf_subtask_arr:            p0, p1, p2
 *   rf_running_subtsk_arr:     p0
 * Output:
 *   terminated_subtsk_arr:     p1, p2
 *   rf_running_subtsk_idx_arr: 0
 *   terminated_subtsk_idx_arr: 1, 2
 */
void YappServiceHandler::find_all_current_index_for_running_and_terminated_processes(
    vector<string> & rf_subtask_arr,        vector<string> & rf_running_subtsk_arr,
    vector<string> & terminated_subtsk_arr, vector<int> & rf_running_subtsk_idx_arr,
                                            vector<int> & terminated_subtsk_idx_arr
) {
  int runtsk_cnt = rf_running_subtsk_arr.size();
  int subtsk_cnt = rf_subtask_arr.size();
  int runtsk_idx = 0;
  size_t hnd_pos = string::npos;
  /**
   * 2.1, grab sub-tasks that are already being terminated(not found under
   *      the folder /foo/yapp/queue/textrfin/${task_hnd}/running_procs
   *      and duck all into terminated_subtsk_arr);
   */
  for (int c = 0; c < subtsk_cnt; c++) {
    /**
     * TODO:
     * If this is a new task or prev. one already finished.
     * - push current task to the NEW queue
     * - increase the current line index by its offset.
     * - it is possible that subtsk_str => ${cur_tskstr}_delim_${nxt_tskstr}
     *                 while runtsk_str => ${cur_tskstr}
     *   since subtsk_str was helped by others and nxt_tskstr is next tsk.
     * - if the tasks were failed for any other reason, the remaining stops
     */
    string subtsk_str = rf_subtask_arr[c];
    if (runtsk_idx < runtsk_cnt) {
      hnd_pos = subtsk_str.find(rf_running_subtsk_arr[runtsk_idx]);
      if (0 != hnd_pos) {
        terminated_subtsk_arr.push_back(subtsk_str);
        terminated_subtsk_idx_arr.push_back(c);
      } else {
        rf_running_subtsk_idx_arr.push_back(c);
        runtsk_idx++;
      }
    } else {
      terminated_subtsk_arr.push_back(subtsk_str);
      terminated_subtsk_idx_arr.push_back(c);
    }
  }
}

bool YappServiceHandler::check_schedule_batch_limit(
  vector<string> & new_rf_subtsk_arr, int batch_limit)
{
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << ">>>> new_rf_subtsk_arr: " << new_rf_subtsk_arr.size()
            << " batch-op-limit: " << batch_limit
            << " check_schedule_batch_limit" << std::endl;
#endif
  return ((int)(new_rf_subtsk_arr.size()) <= batch_limit);
}

bool YappServiceHandler::check_and_schedule_jobs_directly_owned_by_each_process(
  vector<string> & rf_subtask_arr, vector<string> & terminated_subtsk_arr,
  vector<string> & new_rf_subtsk_arr, vector<int> & new_rf_subtsk_idx_arr,
  vector<int> & terminated_subtsk_idx_arr,
  list<string> & fin_rf_subtsk_arr, list<string> & fin_rf_subtsk_full_hnd_arr,
  list<string> & subtsk_list_before_change, list<string> & subtsk_list_after_change,
  YappSubtaskQueue & rf_subtask_queue, int batch_limit
) {
  /**
   * 2.2, check to find which set of execution already done its own portion.
   *
   * - If the current execution is processing its own part:
   *
   *   - If there are still some portion left for that partition, simply
   *     update its current and next row idx for the file to process and
   *     put it to vector new_rf_subtsk_arr.
   *
   *   - If the last chunk already being finished, then put it to another
   *     list fin_rf_subtsk_arr
   *
   * - or current execution finished helping for chunks from other procs:
   *   - also put to list fin_rf_subtsk_arr.
   */
  int tertsk_cnt = terminated_subtsk_arr.size();
  size_t hnd_pos = string::npos;
  bool is_within_schedule_limit = check_schedule_batch_limit(new_rf_subtsk_arr, batch_limit);
  for (int s = 0; ((s < tertsk_cnt) && (true == is_within_schedule_limit)); s++) {
    hnd_pos = terminated_subtsk_arr[s].find(NEW_DELM_STR);
    string prev_tsk_chunk_str = terminated_subtsk_arr[s];
    string next_tsk_chunk_str = terminated_subtsk_arr[s];
    if (string::npos != hnd_pos) {
      prev_tsk_chunk_str = prev_tsk_chunk_str.substr(0, hnd_pos);
      next_tsk_chunk_str = next_tsk_chunk_str.substr(
        hnd_pos + NEW_DELM_STR.size()
      );
    }

    string tskhd_in_job, max_line_idx, tskhd_in_run,
           cur_line_idx, nxt_line_idx, tot_proc_cnt;

    bool ret = RangeFileTaskDataParser::parse_rfile_task_data_str(
      next_tsk_chunk_str, tskhd_in_job, max_line_idx, tskhd_in_run,
                          cur_line_idx, nxt_line_idx, tot_proc_cnt
    );

    if (true != ret) { continue; }

    long long tot_proc_cot = atoll(tot_proc_cnt.c_str());
    long long max_line_idn = atoll(max_line_idx.c_str());
    long long cur_line_idn = atoll(cur_line_idx.c_str());
    long long nxt_line_idn = atoll(nxt_line_idx.c_str());

    cur_line_idn += tot_proc_cot;
    nxt_line_idn += tot_proc_cot;

    if ((tskhd_in_job == tskhd_in_run) && (cur_line_idn <= max_line_idn)) {
      new_rf_subtsk_arr.push_back(
        tskhd_in_job +
          MAX_LINE_STR + max_line_idx + CUR_HNDL_STR + tskhd_in_run +
          CUR_LINE_STR + StringUtil::convert_int_to_str(cur_line_idn) +
          NXT_LINE_STR + StringUtil::convert_int_to_str(nxt_line_idn) +
          TOT_PROC_STR + tot_proc_cnt
      );
      new_rf_subtsk_idx_arr.push_back(terminated_subtsk_idx_arr[s]);
      subtsk_list_before_change.push_back(
        rf_subtask_queue.get_queue_path() + "/" +
        rf_subtask_arr[terminated_subtsk_idx_arr[s]]
      );
      subtsk_list_after_change.push_back(
        rf_subtask_queue.get_queue_path() + "/" + new_rf_subtsk_arr.back()
      );
    } else {
      fin_rf_subtsk_arr.push_back(prev_tsk_chunk_str);
      fin_rf_subtsk_full_hnd_arr.push_back(terminated_subtsk_arr[s]);
    }
    is_within_schedule_limit = check_schedule_batch_limit(new_rf_subtsk_arr, batch_limit);
  } /* for (int s = 0; s < tertsk_cnt; s++) */
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << ">>>> " << is_within_schedule_limit << " check_and_schedule_jobs_directly_owned_by_each_process" << std::endl;
#endif
  return is_within_schedule_limit;
}

bool YappServiceHandler::check_and_schedule_to_help_terminated_processes( 
  vector<string> & new_rf_subtsk_arr, vector<int> & new_rf_subtsk_idx_arr,
  list<string> & subtsk_list_before_change, list<string> & subtsk_list_after_change,
  list<string> & fin_rf_subtsk_arr, list<string> & fin_rf_subtsk_full_hnd_arr,
  vector<string> & rf_subtask_arr, YappSubtaskQueue & rf_subtask_queue, int batch_limit)
{
  /**
   * 2.3, For every execution done its own job in fin_rf_subtsk_arr:
   *      - look for some other chunks left within the same task set.
   *        (process with proc_hnd having lower lexi order come first)
   *      - put the new subtask to vector new_rf_subtsk_arr.
   */
  /** 2.3-1, find if any finished tasks have certain partiions left. **/
  int newtsk_cnt = new_rf_subtsk_arr.size();
  int cur_subtsk_idx = 0;
  // list<string>::iterator subtsk_bf_itr = subtsk_list_before_change.begin();
  list<string>::iterator subtsk_af_itr = subtsk_list_after_change.begin();
  size_t fin_rf_arr_size = fin_rf_subtsk_arr.size();

  bool is_within_schedule_limit = check_schedule_batch_limit(new_rf_subtsk_arr, batch_limit);
  for (int z = 0; ((z < newtsk_cnt) && (!fin_rf_subtsk_arr.empty()) &&
                   (true == is_within_schedule_limit));)
  { 
    fin_rf_arr_size = fin_rf_subtsk_arr.size();
    string active_tskhd_in_job, active_max_line_idx, active_tskhd_in_run,
           active_cur_line_idx, active_nxt_line_idx, active_tot_proc_cnt;
    cur_subtsk_idx = new_rf_subtsk_idx_arr[z];
    size_t hnd_pos = rf_subtask_arr[cur_subtsk_idx].find(NEW_DELM_STR);

    string next_tsk_chunk_str = rf_subtask_arr[cur_subtsk_idx];
    if (string::npos != hnd_pos) {
      next_tsk_chunk_str = next_tsk_chunk_str.substr(
        hnd_pos + NEW_DELM_STR.size()
      );
    }
    bool ret = RangeFileTaskDataParser::parse_rfile_task_data_str(
      next_tsk_chunk_str,
      active_tskhd_in_job, active_max_line_idx, active_tskhd_in_run,
      active_cur_line_idx, active_nxt_line_idx, active_tot_proc_cnt
    );
    assert(true == ret);
    long long tot_proc_cnt = atoll(active_tot_proc_cnt.c_str());
    long long max_line_idn = atoll(active_max_line_idx.c_str());
    long long cur_line_idn = atoll(active_cur_line_idx.c_str());
    long long nxt_line_idn = atoll(active_nxt_line_idx.c_str());

    cur_line_idn += tot_proc_cnt * 2;
    nxt_line_idn += tot_proc_cnt * 2;

    /** only those tasks not finished its own part needs help. **/
    if ((active_tskhd_in_job != active_tskhd_in_run) ||
        (cur_line_idn > max_line_idn)) {
      z++; subtsk_af_itr++; continue; // subtsk_bf_itr++; 
    }

    /** start to dispatch remaining workloads over diff. processes **/
    for (string fin_subtsk_in_job = fin_rf_subtsk_arr.front();
         ((cur_line_idn <= max_line_idn) && (!fin_rf_subtsk_arr.empty()) &&
          (true == is_within_schedule_limit));
         fin_rf_subtsk_arr.pop_front(),
         fin_rf_subtsk_full_hnd_arr.pop_front())
    {
      fin_subtsk_in_job = fin_rf_subtsk_arr.front();
      string fin_tskhd_in_job, fin_max_line_idx, fin_tskhd_in_run,
             fin_cur_line_idx, fin_nxt_line_idx, fin_tot_proc_cnt;
      ret = RangeFileTaskDataParser::parse_rfile_task_data_str (
        fin_subtsk_in_job,
        fin_tskhd_in_job, fin_max_line_idx, fin_tskhd_in_run,
        fin_cur_line_idx, fin_nxt_line_idx, fin_tot_proc_cnt
      );
      assert(true == ret);
      new_rf_subtsk_arr.push_back(
        fin_tskhd_in_job + MAX_LINE_STR + active_max_line_idx +
        CUR_HNDL_STR + active_tskhd_in_run + CUR_LINE_STR +
        StringUtil::convert_int_to_str(cur_line_idn) + NXT_LINE_STR +
        StringUtil::convert_int_to_str(nxt_line_idn) + TOT_PROC_STR +
        active_tot_proc_cnt
      );
      subtsk_list_before_change.push_back(
        rf_subtask_queue.get_queue_path() + "/" + 
        fin_rf_subtsk_full_hnd_arr.front()
      );
      subtsk_list_after_change.push_back(
        rf_subtask_queue.get_queue_path() + "/" + new_rf_subtsk_arr.back()
      );

#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << ">>>> CHECK TO HELP THOSE NOT RUNNING TASKS." << std::endl;
      std::cerr << ">>>> FIN TASK: " << fin_subtsk_in_job << std::endl;
      std::cerr << "     WIL HELP: " << next_tsk_chunk_str << std::endl;
      std::cerr << "     NEW TASK: " << new_rf_subtsk_arr.back() << std::endl;
#endif
      cur_line_idn = nxt_line_idn; 
      nxt_line_idn += tot_proc_cnt;

      is_within_schedule_limit = check_schedule_batch_limit(new_rf_subtsk_arr, batch_limit);
    }
    if (fin_rf_arr_size > fin_rf_subtsk_arr.size()) {
      /** enter here means we did some re-balancing **/
      string new_task_str =
        active_tskhd_in_job + MAX_LINE_STR + active_max_line_idx +
        CUR_HNDL_STR + active_tskhd_in_run + CUR_LINE_STR +
        StringUtil::convert_int_to_str(cur_line_idn - tot_proc_cnt) + NXT_LINE_STR +
        StringUtil::convert_int_to_str(nxt_line_idn - tot_proc_cnt) + TOT_PROC_STR +
        active_tot_proc_cnt;

      *subtsk_af_itr = rf_subtask_queue.get_queue_path() + "/" +
                       new_rf_subtsk_arr[z] + NEW_DELM_STR + new_task_str;

    }
    if (cur_line_idn > max_line_idn) {
      z++; subtsk_af_itr++;
    }
  } /* for (int z = 0; z < newtsk_cnt && !fin_rf_subtsk_arr.empty();) */
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << ">>>> " << is_within_schedule_limit << " check_and_schedule_to_help_terminated_processes" << std::endl;
#endif
  return is_within_schedule_limit;
}

bool YappServiceHandler::check_and_schedule_to_help_running_processes( 
  vector<string> & new_rf_subtsk_arr, vector<int> & new_rf_subtsk_idx_arr,
  list<string> & subtsk_list_before_change, list<string> & subtsk_list_after_change,
  list<string> & fin_rf_subtsk_arr, list<string> & fin_rf_subtsk_full_hnd_arr,
  vector<string> & rf_subtask_arr, vector<int> & rf_running_subtsk_idx_arr,
  YappSubtaskQueue & rf_subtask_queue, int batch_limit
) {
  int runtsk_cnt = rf_running_subtsk_idx_arr.size();
  int cur_subtsk_idx = 0;
  bool is_within_schedule_limit = check_schedule_batch_limit(new_rf_subtsk_arr, batch_limit);
  /** 2.3-2, find if any running tasks have certain partitions left. **/
  for (int x = 0; ((x < runtsk_cnt) && (!fin_rf_subtsk_arr.empty()) &&
                   (true == is_within_schedule_limit));)
  {
    string active_tskhd_in_job, active_max_line_idx, active_tskhd_in_run,
           active_cur_line_idx, active_nxt_line_idx, active_tot_proc_cnt;

    size_t fin_rf_arr_size = fin_rf_subtsk_arr.size();
    cur_subtsk_idx = rf_running_subtsk_idx_arr[x];
    size_t hnd_pos = rf_subtask_arr[cur_subtsk_idx].find(NEW_DELM_STR);

    string next_tsk_chunk_str = rf_subtask_arr[cur_subtsk_idx];
    string prev_tsk_chunk_str = rf_subtask_arr[cur_subtsk_idx];
    if (string::npos != hnd_pos) {
      /** reaching here basically means a running task helped by others **/
      prev_tsk_chunk_str = next_tsk_chunk_str.substr(0, hnd_pos);
      next_tsk_chunk_str = next_tsk_chunk_str.substr(
        hnd_pos + NEW_DELM_STR.size()
      );
    }

    bool ret = RangeFileTaskDataParser::parse_rfile_task_data_str(
      next_tsk_chunk_str,
      active_tskhd_in_job, active_max_line_idx, active_tskhd_in_run,
      active_cur_line_idx, active_nxt_line_idx, active_tot_proc_cnt
    );

    assert(true == ret);

    long long tot_proc_cnt = atoll(active_tot_proc_cnt.c_str());
    long long max_line_idn = atoll(active_max_line_idx.c_str());
    long long cur_line_idn = atoll(active_cur_line_idx.c_str());
    long long nxt_line_idn = atoll(active_nxt_line_idx.c_str());

    cur_line_idn += tot_proc_cnt;
    nxt_line_idn += tot_proc_cnt;

    if ((active_tskhd_in_job != active_tskhd_in_run) ||
        (cur_line_idn > max_line_idn)) { x++; continue; }
#ifdef DEBUG_YAPP_SERVICE_HANDLER
    std::cerr << ">>>> START LOOPING ON A RUNNING TSK, CUR_ID:"
              << cur_line_idn << ", NXT_ID: " << nxt_line_idn
              << std::endl;
#endif
    /** start to dispatch remaining workloads over diff. processes **/
    for (string fin_subtsk_in_job = fin_rf_subtsk_arr.front();
         ((cur_line_idn <= max_line_idn) && (!fin_rf_subtsk_arr.empty()) &&
          (true == is_within_schedule_limit));
         fin_rf_subtsk_arr.pop_front(),
         fin_rf_subtsk_full_hnd_arr.pop_front())
    {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << ">>>> CUR_ID:" << cur_line_idn << ", NXT_ID: " << nxt_line_idn
                << std::endl;
#endif
      fin_subtsk_in_job = fin_rf_subtsk_arr.front();
      string fin_tskhd_in_job, fin_max_line_idx, fin_tskhd_in_run,
             fin_cur_line_idx, fin_nxt_line_idx, fin_tot_proc_cnt;
      ret = RangeFileTaskDataParser::parse_rfile_task_data_str (
        fin_subtsk_in_job,
        fin_tskhd_in_job, fin_max_line_idx, fin_tskhd_in_run,
        fin_cur_line_idx, fin_nxt_line_idx, fin_tot_proc_cnt
      );
      assert(true == ret);

      new_rf_subtsk_arr.push_back(
        fin_tskhd_in_job + MAX_LINE_STR + active_max_line_idx +
        CUR_HNDL_STR + active_tskhd_in_run + CUR_LINE_STR +
        StringUtil::convert_int_to_str(cur_line_idn) + NXT_LINE_STR +
        StringUtil::convert_int_to_str(nxt_line_idn) + TOT_PROC_STR +
        active_tot_proc_cnt
      );

      subtsk_list_before_change.push_back(
        rf_subtask_queue.get_queue_path() + "/" +
        fin_rf_subtsk_full_hnd_arr.front()
      );
      subtsk_list_after_change.push_back(
        rf_subtask_queue.get_queue_path() + "/" + new_rf_subtsk_arr.back()
      );
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << ">>>> CHECK TO HELP THOSE RUNNING TASKS." << std::endl;
      std::cerr << ">>>> FIN TASK: " << fin_subtsk_in_job << std::endl;
      std::cerr << "     WIL HELP: " << next_tsk_chunk_str << std::endl;
      std::cerr << "     NEW TASK: " << new_rf_subtsk_arr.back() << std::endl;
#endif
      cur_line_idn = nxt_line_idn; 
      nxt_line_idn += tot_proc_cnt;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << ">>>> CUR_ID:" << cur_line_idn << ", NXT_ID: " << nxt_line_idn
                << std::endl;
#endif
      is_within_schedule_limit = check_schedule_batch_limit(new_rf_subtsk_arr, batch_limit);
    }
    if (fin_rf_arr_size > fin_rf_subtsk_arr.size()) {
    /**
     * - For those subtask being helped, we only update its new task string
     *   (which indicates next row to process) as its value under proc_arr
     *   folder only, so that the workers does not need to block to wait for
     *   the thread to finish(they only update the running_proc folder).
     * - For every subtask being helped, it will be changed to the format as
     *   ${old_tskstr}_delim_${new_tskstr} under folder proc_arr;
     */
      string new_task_str =
        active_tskhd_in_job + MAX_LINE_STR + active_max_line_idx +
        CUR_HNDL_STR + active_tskhd_in_run + CUR_LINE_STR +
        StringUtil::convert_int_to_str(cur_line_idn - tot_proc_cnt) + NXT_LINE_STR +
        StringUtil::convert_int_to_str(nxt_line_idn - tot_proc_cnt) + TOT_PROC_STR +
        active_tot_proc_cnt;
      subtsk_list_before_change.push_back(
        rf_subtask_queue.get_queue_path() + "/" + rf_subtask_arr[cur_subtsk_idx]
      );
      subtsk_list_after_change.push_back(
        rf_subtask_queue.get_queue_path() + "/" +
        prev_tsk_chunk_str + NEW_DELM_STR + new_task_str
      );
    }
    if (cur_line_idn > max_line_idn) { x++; }
  } /* for (int x = 0; x < runtsk_cnt && !fin_rf_subtsk_arr.empty();) */
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << ">>>> " << is_within_schedule_limit << " check_and_schedule_to_help_running_processes" << std::endl;
#endif
  return is_within_schedule_limit;
}

void YappServiceHandler::check_and_clear_free_processes(
  vector<string> & new_rf_subtsk_arr,  vector<string> & rf_failed_subtsk_arr,
  vector<string> & rf_running_subtsk_arr, vector<string> & rf_subtask_arr,
  list<string> & fin_rf_subtsk_full_hnd_arr, YappSubtaskQueue & rf_subtask_queue,
  YappServiceHandler * ym_srv_ptr
) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- check_and_clear_free_processes"
            << " new_rf_subtsk_arr: " << new_rf_subtsk_arr.size()
            << " rf_failed_subtsk_arr: " << rf_failed_subtsk_arr.size()
            << " rf_running_subtsk_arr: " << rf_running_subtsk_arr.size()
            << " rf_subtask_arr: " << rf_subtask_arr.size()
            << " fin_rf_subtsk_full_hnd_arr: " << fin_rf_subtsk_full_hnd_arr.size()
            << std::endl;
#endif
  if (!rf_running_subtsk_arr.empty() || !new_rf_subtsk_arr.empty() ||
      !rf_failed_subtsk_arr.empty()) { return; }
  if ((rf_subtask_arr.size() + rf_failed_subtsk_arr.size()) !=
       fin_rf_subtsk_full_hnd_arr.size()) { return; }

  vector<string> path_to_del(fin_rf_subtsk_full_hnd_arr.begin(),
                             fin_rf_subtsk_full_hnd_arr.end());
   /** gethering all tasks finished and no more subtasks lefted for them **/
  int tsk_to_del_cnt = path_to_del.size();

  /**
   * for those processes finished and no other chunks left for them to help,
   * we would mark them as terminated under the job node structure.
   */
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  vector<string> upd_path_arr;
  vector<string> upd_data_arr;
  vector<string> path_to_cr_arr;
  vector<string> data_to_cr_arr;
  for (int y = 0; y < tsk_to_del_cnt; y++) {
    string upd_proc_path = ym_srv_ptr->zkc_proxy_ptr->get_proc_full_path(
      path_to_del[y].substr(0, path_to_del[y].find(MAX_LINE_STR))
    );
    upd_path_arr.push_back(upd_proc_path + "/cur_status"); 
    upd_data_arr.push_back(
      StringUtil::convert_int_to_str(PROC_STATUS_CODE::PROC_STATUS_TERMINATED)
    );
  }

  for (int y = 0; y <  tsk_to_del_cnt; y++) {
    /** for the purpose of logging, we keep last chunk for each proc **/
    path_to_cr_arr.push_back(
      ym_srv_ptr->zkc_proxy_ptr->get_terminated_queue_path_prefix() + "/" +
      path_to_del[y]
    );
    data_to_cr_arr.push_back("");
    path_to_del[y] =
      rf_subtask_queue.get_queue_path() + "/" + path_to_del[y];
  }

  YAPP_MSG_CODE ret_code = set_create_and_delete_in_small_batch(
    upd_path_arr, upd_data_arr, path_to_cr_arr, data_to_cr_arr, path_to_del, ym_srv_ptr
  );
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- set_create_and_delete_in_small_batch: " << ret_code << std::endl;
#endif
}

YAPP_MSG_CODE YappServiceHandler::set_create_and_delete_in_small_batch(
  const vector<string> & upd_path_arr, const vector<string> & upd_data_arr,
  const vector<string> & path_to_cr_arr, const vector<string> & data_to_cr_arr,
  const vector<string> & path_to_del, YappServiceHandler * ym_srv_ptr
) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;
  int tot_size = path_to_cr_arr.size();
  int itr_cnts = tot_size / yapp::util::MAX_BATCH_CREATE_CHUNK;
  int tsk_remn = tot_size - itr_cnts * yapp::util::MAX_BATCH_CREATE_CHUNK;
  if (tsk_remn > 0) { itr_cnts++; }
 
  int prev = 0;
  int next = 0;
  vector<string>::const_iterator updp_prev_itr = upd_path_arr.begin();
  vector<string>::const_iterator updp_next_itr = upd_path_arr.begin();
  vector<string>::const_iterator updd_prev_itr = upd_data_arr.begin();
  vector<string>::const_iterator updd_next_itr = upd_data_arr.begin();
  vector<string>::const_iterator delp_prev_itr = path_to_del.begin();
  vector<string>::const_iterator delp_next_itr = path_to_del.begin();

  vector<string>::const_iterator path_prev_itr = path_to_cr_arr.begin();
  vector<string>::const_iterator path_next_itr = path_to_cr_arr.begin();
  vector<string>::const_iterator data_prev_itr = data_to_cr_arr.begin();
  vector<string>::const_iterator data_next_itr = data_to_cr_arr.begin();

  for (int c = 0; c < itr_cnts; c++)
  {
    next += yapp::util::MAX_BATCH_CREATE_CHUNK;
    next = (next > tot_size) ? tot_size : next;

    path_prev_itr = path_to_cr_arr.begin() + prev;
    path_next_itr = path_to_cr_arr.begin() + next;
    data_prev_itr = data_to_cr_arr.begin() + prev;
    data_next_itr = data_to_cr_arr.begin() + next;


    updp_prev_itr = upd_path_arr.begin() + prev;
    updp_next_itr = upd_path_arr.begin() + next;
    updd_prev_itr = upd_data_arr.begin() + prev;
    updd_next_itr = upd_data_arr.begin() + next;
    delp_prev_itr = path_to_del.begin() + prev;
    delp_next_itr = path_to_del.begin() + next;

    vector<string> sub_updp_arr(updp_prev_itr, updp_next_itr);
    vector<string> sub_updd_arr(updd_prev_itr, updd_next_itr);
    vector<string> sub_path_arr(path_prev_itr, path_next_itr);
    vector<string> sub_data_arr(data_prev_itr, data_next_itr);
    vector<string> sub_delp_arr(delp_prev_itr, delp_next_itr);

    ret_code = ym_srv_ptr->zkc_proxy_ptr->batch_set_create_and_delete(
      sub_updp_arr, sub_updd_arr, sub_path_arr, sub_data_arr, sub_delp_arr
    );

    usleep(BATCH_OP_NICE_TIME_IN_MICRO_SEC);
    if (YAPP_MSG_SUCCESS != ret_code) { break; }
    prev = next;
  } /** end of for **/
  return ret_code;
}

void YappServiceHandler::commit_range_file_task_schedule_plan(
  YappServiceHandler * ym_srv_ptr, YappSubtaskQueue & rf_subtask_queue,
  YappSubtaskQueue & rf_running_subtask_queue,
  list<string> & subtsk_list_before_change, list<string> & subtsk_list_after_change,
  list<string> & fin_rf_subtsk_full_hnd_arr, vector<string> & new_rf_subtsk_arr
)
{
  /**
   * 2.4, start them all in new_rf_subtsk_arr(throw into NEW queue)
   */
  vector<string> path_to_cr_arr;
  vector<string> data_to_cr_arr;
  vector<string> path_from_arr(subtsk_list_before_change.begin(),
                               subtsk_list_before_change.end());
  vector<string> path_to_arr(subtsk_list_after_change.begin(),
                             subtsk_list_after_change.end());
  vector<string> data_arr;
  vector<string> path_to_del; /** DUMMY VARS **/
  int change_cnt = path_to_arr.size();
  for (int y = 0; y < change_cnt; y++) { data_arr.push_back(""); }
  int tsk_to_cr_cnt = new_rf_subtsk_arr.size();
  /** gethering all new tasks to the running folder & new queue **/
  for (int y = 0; y < tsk_to_cr_cnt; y++) {
    path_to_cr_arr.push_back(
      rf_running_subtask_queue.get_queue_path() + "/" + new_rf_subtsk_arr[y]
    );
    data_to_cr_arr.push_back("");
    path_to_cr_arr.push_back(
      ym_srv_ptr->newly_cr_task_queue_proxy.get_queue_path() + "/" +
      new_rf_subtsk_arr[y]
    );
    data_to_cr_arr.push_back("");
  }
  /** execute them in batch (atomic via zookeeper muti api)**/
  ym_srv_ptr->zkc_proxy_ptr->batch_move_node_with_no_children_and_create_del(
    path_from_arr,  path_to_arr,    data_arr,
    path_to_cr_arr, data_to_cr_arr, path_to_del
  );
}

bool YappServiceHandler::schedule_rfile_tasks_in_batch(YappServiceHandler * ym_srv_ptr)
{
  bool status = false;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == ym_srv_ptr) { return status; }

  /**
   * Here rf_task_to_schedule will holds all tasks(1 per each diff job) to check
   * and keep pumping the processes into NEW(newly created) queue for scheduling
   */
  vector<string> rf_task_to_schedule;
  rc = ym_srv_ptr->rfile_task_queue_proxy.get_task_queue (
    rf_task_to_schedule, ym_srv_ptr->zkc_proxy_ptr
  );
  if (YAPP_MSG_SUCCESS != rc) { return status; }

  int tsk_cnt = rf_task_to_schedule.size();
  if (tsk_cnt < 1) { return true; }

  /* for every task under a job, check all avaliable processes */
  for (int i = 0; i < tsk_cnt; i++) {
    /* initialize queue path for each task to schedule */
    YappSubtaskQueue rf_running_subtask_queue, rf_subtask_queue, rf_failed_task_queue;
    setup_rftasks_queue_path(ym_srv_ptr, rf_running_subtask_queue, rf_subtask_queue,
                             rf_failed_task_queue, rf_task_to_schedule[i]);

    vector<string> rf_subtask_arr, rf_failed_subtsk_arr, rf_running_subtsk_arr;
    /* figuer out any processes already been finished or failed. */
    if (true != fetch_queue_and_filter_failed_rftasks(
      ym_srv_ptr, rf_running_subtsk_arr, rf_subtask_arr, rf_failed_subtsk_arr,
      rf_running_subtask_queue, rf_subtask_queue, rf_failed_task_queue
    )) { continue; }

    /**
     * 2. for each task set, check & schedule appropriate # of subtasks
     *    subtsk_cnt <=0 means there is nothing in the queue to check, while when
     *    runtsk_cnt >= subtsk_cnt should never occur anyway(consistency checking)
     */
    if ((YAPP_MSG_SUCCESS != rc) || (0 >= rf_subtask_arr.size()) ||
        (rf_running_subtsk_arr.size() >= rf_subtask_arr.size())) { continue; }

    /* 2.1. try to acquire the ex lock for the task set to schedule **/
    string lock_hnd_ret;
    rc = ym_srv_ptr->rfile_task_queue_proxy.try_acquire_ex_lock(
      lock_hnd_ret, rf_task_to_schedule[i], ym_srv_ptr->zkc_proxy_ptr
    );
    if (YAPP_MSG_SUCCESS != rc) { continue; }

    vector<string> terminated_subtsk_arr;
    vector<int>    rf_running_subtsk_idx_arr, terminated_subtsk_idx_arr;
    /* 2.2. figure out the finished & running procs, record their positions */
    find_all_current_index_for_running_and_terminated_processes(
      rf_subtask_arr, rf_running_subtsk_arr, terminated_subtsk_arr,
      rf_running_subtsk_idx_arr, terminated_subtsk_idx_arr
    );
    vector<string> new_rf_subtsk_arr; vector<int> new_rf_subtsk_idx_arr;
    list<string>   fin_rf_subtsk_arr, fin_rf_subtsk_full_hnd_arr,
                   subtsk_list_before_change, subtsk_list_after_change;
    /* 2.3. 1st schedule jobs directly owned by each process. */
    check_and_schedule_jobs_directly_owned_by_each_process(
      rf_subtask_arr, terminated_subtsk_arr, new_rf_subtsk_arr,
      new_rf_subtsk_idx_arr, terminated_subtsk_idx_arr,
      fin_rf_subtsk_arr, fin_rf_subtsk_full_hnd_arr,
      subtsk_list_before_change, subtsk_list_after_change, rf_subtask_queue,
      ym_srv_ptr->batch_task_schedule_limit
    );
    /* 2.4. check to see any free processes can help terminated ones */
    check_and_schedule_to_help_terminated_processes( 
      new_rf_subtsk_arr, new_rf_subtsk_idx_arr,
      subtsk_list_before_change, subtsk_list_after_change,
      fin_rf_subtsk_arr, fin_rf_subtsk_full_hnd_arr,
      rf_subtask_arr, rf_subtask_queue,
      ym_srv_ptr->batch_task_schedule_limit
    );
    /* 2.5. check to see any free processes can help running ones */
    check_and_schedule_to_help_running_processes(
      new_rf_subtsk_arr, new_rf_subtsk_idx_arr,
      subtsk_list_before_change, subtsk_list_after_change,
      fin_rf_subtsk_arr, fin_rf_subtsk_full_hnd_arr,
      rf_subtask_arr, rf_running_subtsk_idx_arr, rf_subtask_queue,
      ym_srv_ptr->batch_task_schedule_limit
    );
    /* 2.6. update all scheduling info. in one single batch trxn */
    commit_range_file_task_schedule_plan(
      ym_srv_ptr, rf_subtask_queue, rf_running_subtask_queue, subtsk_list_before_change,
      subtsk_list_after_change, fin_rf_subtsk_full_hnd_arr, new_rf_subtsk_arr
    );
    /* 2.7. clean up all workers if all segments are done. **/
    check_and_clear_free_processes(
      new_rf_subtsk_arr,  rf_failed_subtsk_arr, rf_running_subtsk_arr,
      rf_subtask_arr, fin_rf_subtsk_full_hnd_arr, rf_subtask_queue, ym_srv_ptr
    );
    /* 2.8. release the ex lock for the task set. **/
    ym_srv_ptr->rfile_task_queue_proxy.release_ex_lock(
      lock_hnd_ret, ym_srv_ptr->zkc_proxy_ptr
    );
    usleep(BATCH_OP_NICE_TIME_IN_MICRO_SEC);
  } /** for (int i = 0; i < tsk_cnt; i++)  **/
  return true;
}

bool YappServiceHandler::schedule_rfile_tasks(YappServiceHandler * ym_srv_ptr)
{
  bool status = false;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == ym_srv_ptr) { return status; }

  vector<string> rf_task_to_schedule;
  // vector<string> rf_task_to_cleanup;

  rc = ym_srv_ptr->rfile_task_queue_proxy.get_task_queue (
    rf_task_to_schedule, ym_srv_ptr->zkc_proxy_ptr
  );
  if (YAPP_MSG_SUCCESS != rc) { return status; }

  int tsk_cnt = rf_task_to_schedule.size();

  if (tsk_cnt < 1) { return true; }

  for (int i = 0; i < tsk_cnt; i++) {
    YappSubtaskQueue rf_running_subtask_queue;
    YappSubtaskQueue rf_subtask_queue;
    YappSubtaskQueue rf_failed_task_queue;

    rf_running_subtask_queue.set_queue_path(
      ym_srv_ptr->zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
      rf_task_to_schedule[i] + "/running_procs"
    );
    rf_subtask_queue.set_queue_path(
      ym_srv_ptr->zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
      rf_task_to_schedule[i] + "/proc_arr"
    );
    rf_failed_task_queue.set_queue_path(
      ym_srv_ptr->zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
      rf_task_to_schedule[i] + "/failed_procs"
    );

    vector<string> rf_subtask_arr;
    vector<string> rf_failed_subtsk_arr;
    vector<string> rf_running_subtsk_arr;
    vector<int>    rf_running_subtsk_idx_arr;
    vector<string> terminated_subtsk_arr;
    vector<int>    terminated_subtsk_idx_arr;
    vector<string> new_rf_subtsk_arr;
    vector<int>    new_rf_subtsk_idx_arr;
    list<string>   fin_rf_subtsk_arr;
    list<string>   fin_rf_subtsk_full_hnd_arr;

    list<string> subtsk_list_before_change;
    list<string> subtsk_list_after_change;


    vector<string> tp_rf_subtask_arr;
    vector<string> tp_rf_running_subtsk_arr;
 
    rc = rf_running_subtask_queue.get_task_queue(tp_rf_running_subtsk_arr,
                                                 ym_srv_ptr->zkc_proxy_ptr);
    if (YAPP_MSG_SUCCESS != rc) { continue; }

    rc = rf_failed_task_queue.get_task_queue(rf_failed_subtsk_arr,
                                             ym_srv_ptr->zkc_proxy_ptr);
    if (YAPP_MSG_SUCCESS != rc) { continue; }

    rc = rf_subtask_queue.get_task_queue(tp_rf_subtask_arr,
                                         ym_srv_ptr->zkc_proxy_ptr);
    if (YAPP_MSG_SUCCESS != rc) { continue; }

    /** needs to clean the task arr for those failed tasks **/
    int failed_cnt = rf_failed_subtsk_arr.size();
    if (0 < failed_cnt) {
      bool matched = false;
      int  task_size = tp_rf_running_subtsk_arr.size();
      for (int c = 0; c < task_size; c++) {
        matched = false;
        for (int i = 0; i < failed_cnt; i++) {
          if (rf_failed_subtsk_arr[i] == tp_rf_running_subtsk_arr[c]){
            matched = true;
            break;
          }
        }
        if (false == matched) {
          rf_running_subtsk_arr.push_back(tp_rf_running_subtsk_arr[c]);
        }
      }
      task_size = tp_rf_subtask_arr.size();
      for (int c = 0; c < task_size; c++) {
        matched = false;
        for (int i = 0; i < failed_cnt; i++) {
          if (0 == tp_rf_subtask_arr[c].find(rf_failed_subtsk_arr[i])){
            matched = true;
            break;
          }
        }
        if (false == matched) {
          rf_subtask_arr.push_back(tp_rf_subtask_arr[c]);
        }
      }
    } else {
      rf_running_subtsk_arr = tp_rf_running_subtsk_arr;
      rf_subtask_arr = tp_rf_subtask_arr;
    }
    
#ifdef DEBUG_YAPP_SERVICE_HANDLER
    std::cerr << "-- TRYING TO CHECK RANGE FILE TASKS IN QUEUE: "
              << rf_subtask_queue.get_queue_path() << std::endl
              << "-- THREAD: " << pthread_self() << " IN MASTER INSTANCE. "
              << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
              << std::endl;
    std::cerr << ">> RANGE FILE TASKS: " << std::endl;
    for (size_t x = 0; x < rf_subtask_arr.size(); x++) {
      std::cerr << rf_subtask_arr[x] << " " << std::endl;
    }
    std::cerr << std::endl;
#endif

    /** 2. for each task set, check & schedule appropriate # of subtasks **/
    int runtsk_cnt = rf_running_subtsk_arr.size();
    int subtsk_cnt = rf_subtask_arr.size();
    int runtsk_idx = 0;
    size_t hnd_pos = string::npos;
    if (0 == subtsk_cnt && 0 == runtsk_cnt) {
/**
 * - Tends To Retain the Structure for the purpose of easy restarting.
      if (YAPP_MSG_SUCCESS == rc) {
        rf_task_to_cleanup.push_back(
          ym_srv_ptr->rfile_task_queue_proxy.get_queue_path() + "/" +
          rf_task_to_schedule[i] + "/running_procs"
        );
        rf_task_to_cleanup.push_back(
          ym_srv_ptr->rfile_task_queue_proxy.get_queue_path() + "/" +
          rf_task_to_schedule[i] + "/proc_arr"
        );
        rf_task_to_cleanup.push_back(
          ym_srv_ptr->rfile_task_queue_proxy.get_queue_path() + "/" +
          rf_task_to_schedule[i] + "/failed_procs"
        );
      }
**/
    } else if (YAPP_MSG_SUCCESS == rc && 0 < subtsk_cnt && runtsk_cnt < subtsk_cnt) {
      /** 1. try to acquire the ex lock for the task set to schedule **/
      string lock_hnd_ret;
      rc = ym_srv_ptr->rfile_task_queue_proxy.try_acquire_ex_lock(
        lock_hnd_ret, rf_task_to_schedule[i], ym_srv_ptr->zkc_proxy_ptr
      );

      if (YAPP_MSG_SUCCESS != rc) { continue; }

      /**
       * 2.1, grab sub-tasks that are already being terminated(not found under
       *      the folder /foo/yapp/queue/textrfin/${task_hnd}/running_procs
       *      and duck all into terminated_subtsk_arr);
       */
      for (int c = 0; c < subtsk_cnt; c++) {
        /**
         * TODO:
         * If this is a new task or prev. one already finished.
         * - push current task to the NEW queue
         * - increase the current line index by its offset.
         * - it is possible that subtsk_str => ${cur_tskstr}_delim_${nxt_tskstr}
         *                 while runtsk_str => ${cur_tskstr}
         *   since subtsk_str was helped by others and nxt_tskstr is next tsk.
         * - if the tasks were failed for any other reason, the remaining stops
         */
        string subtsk_str = rf_subtask_arr[c];
        if (runtsk_idx < runtsk_cnt) {
          hnd_pos = subtsk_str.find(rf_running_subtsk_arr[runtsk_idx]);
          if (0 != hnd_pos) {
            terminated_subtsk_arr.push_back(subtsk_str);
            terminated_subtsk_idx_arr.push_back(c);
          } else {
            rf_running_subtsk_idx_arr.push_back(c);
            runtsk_idx++;
          }
        } else {
          terminated_subtsk_arr.push_back(subtsk_str);
          terminated_subtsk_idx_arr.push_back(c);
        }
      }

#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << ">> FINISHED GETTING RANGE FILE TASKS LIST TO SCHEDULE: "
                << rf_running_subtask_queue.get_queue_path() << std::endl
                << ">> THREAD: " << pthread_self() << " IN MASTER INSTANCE. "
                << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
                << std::endl;
      std::cerr << ">> NOT RUNNING RANGE FILE TASKS: " << std::endl;
      for (size_t x = 0; x < terminated_subtsk_arr.size(); x++) {
        std::cerr << terminated_subtsk_arr[x] << std::endl;
      }
      std::cerr << std::endl;
      std::cerr << ">> RUNNING RANGE FILE TASKS: " << std::endl;
      for (size_t x = 0; x < rf_running_subtsk_arr.size(); x++) {
        std::cerr << rf_running_subtsk_arr[x] << std::endl;
      }
      std::cerr << std::endl;
#endif

      /**
       * 2.2, check to find which set of execution already done its own portion.
       *
       * - If the current execution is processing its own part:
       *
       *   - If there are still some portion left for that partition, simply
       *     update its current and next row idx for the file to process and
       *     put it to vector new_rf_subtsk_arr.
       *
       *   - If the last chunk already being finished, then put it to another
       *     list fin_rf_subtsk_arr
       *
       * - or current execution finished helping for chunks from other procs:
       *   - also put to list fin_rf_subtsk_arr.
       */
      int tertsk_cnt = terminated_subtsk_arr.size();
      for (int s = 0; s < tertsk_cnt; s++) {
        hnd_pos = terminated_subtsk_arr[s].find(NEW_DELM_STR);
        string prev_tsk_chunk_str = terminated_subtsk_arr[s];
        string next_tsk_chunk_str = terminated_subtsk_arr[s];
        if (string::npos != hnd_pos) {
          prev_tsk_chunk_str = prev_tsk_chunk_str.substr(0, hnd_pos);
          next_tsk_chunk_str = next_tsk_chunk_str.substr(
            hnd_pos + NEW_DELM_STR.size()
          );
        }

        string tskhd_in_job, max_line_idx, tskhd_in_run,
               cur_line_idx, nxt_line_idx, tot_proc_cnt;

        bool ret = RangeFileTaskDataParser::parse_rfile_task_data_str(
          next_tsk_chunk_str, tskhd_in_job, max_line_idx, tskhd_in_run,
                              cur_line_idx, nxt_line_idx, tot_proc_cnt
        );

        if (true != ret) { continue; }

        long long tot_proc_cot = atoll(tot_proc_cnt.c_str());
        long long max_line_idn = atoll(max_line_idx.c_str());
        long long cur_line_idn = atoll(cur_line_idx.c_str());
        long long nxt_line_idn = atoll(nxt_line_idx.c_str());

        // if (string::npos == hnd_pos) {
        cur_line_idn += tot_proc_cot;
        nxt_line_idn += tot_proc_cot;
        // }

        if ((tskhd_in_job == tskhd_in_run) && (cur_line_idn <= max_line_idn)) {
          new_rf_subtsk_arr.push_back(
            tskhd_in_job +
              MAX_LINE_STR + max_line_idx + CUR_HNDL_STR + tskhd_in_run +
              CUR_LINE_STR + StringUtil::convert_int_to_str(cur_line_idn) +
              NXT_LINE_STR + StringUtil::convert_int_to_str(nxt_line_idn) +
              TOT_PROC_STR + tot_proc_cnt
          );
          new_rf_subtsk_idx_arr.push_back(terminated_subtsk_idx_arr[s]);
          subtsk_list_before_change.push_back(
            rf_subtask_queue.get_queue_path() + "/" +
            rf_subtask_arr[terminated_subtsk_idx_arr[s]]
          );
          subtsk_list_after_change.push_back(
            rf_subtask_queue.get_queue_path() + "/" + new_rf_subtsk_arr.back()
          );
        } else {
          fin_rf_subtsk_arr.push_back(prev_tsk_chunk_str);
          fin_rf_subtsk_full_hnd_arr.push_back(terminated_subtsk_arr[s]);
        }
      }

      /**
       * 2.3, For every execution done its own job in fin_rf_subtsk_arr:
       *      - look for some other chunks left within the same task set.
       *        (process with proc_hnd having lower lexi order come first)
       *      - put the new subtask to vector new_rf_subtsk_arr.
       */

      /** 2.3-1, find if any finished tasks have certain partiions left. **/
      int newtsk_cnt = new_rf_subtsk_arr.size();
      int cur_subtsk_idx = 0;
      // list<string>::iterator subtsk_bf_itr = subtsk_list_before_change.begin();
      list<string>::iterator subtsk_af_itr = subtsk_list_after_change.begin();
      size_t fin_rf_arr_size = fin_rf_subtsk_arr.size();
      for (int z = 0; z < newtsk_cnt && !fin_rf_subtsk_arr.empty();)
      { 
        fin_rf_arr_size = fin_rf_subtsk_arr.size();
        string active_tskhd_in_job, active_max_line_idx, active_tskhd_in_run,
               active_cur_line_idx, active_nxt_line_idx, active_tot_proc_cnt;
        cur_subtsk_idx = new_rf_subtsk_idx_arr[z];
        hnd_pos = rf_subtask_arr[cur_subtsk_idx].find(NEW_DELM_STR);

        string next_tsk_chunk_str = rf_subtask_arr[cur_subtsk_idx];
        if (string::npos != hnd_pos) {
          next_tsk_chunk_str = next_tsk_chunk_str.substr(
            hnd_pos + NEW_DELM_STR.size()
          );
        }
        bool ret = RangeFileTaskDataParser::parse_rfile_task_data_str(
          next_tsk_chunk_str,
          active_tskhd_in_job, active_max_line_idx, active_tskhd_in_run,
          active_cur_line_idx, active_nxt_line_idx, active_tot_proc_cnt
        );
        assert(true == ret);
        long long tot_proc_cnt = atoll(active_tot_proc_cnt.c_str());
        long long max_line_idn = atoll(active_max_line_idx.c_str());
        long long cur_line_idn = atoll(active_cur_line_idx.c_str());
        long long nxt_line_idn = atoll(active_nxt_line_idx.c_str());

        cur_line_idn += tot_proc_cnt * 2;
        nxt_line_idn += tot_proc_cnt * 2;

        /** only those tasks not finished its own part needs help. **/
        if ((active_tskhd_in_job != active_tskhd_in_run) ||
            (cur_line_idn > max_line_idn)) {
          z++; subtsk_af_itr++; continue; // subtsk_bf_itr++; 
        }

        /** start to dispatch remaining workloads over diff. processes **/
        for (string fin_subtsk_in_job = fin_rf_subtsk_arr.front();
             cur_line_idn <= max_line_idn && !fin_rf_subtsk_arr.empty();
             fin_rf_subtsk_arr.pop_front(),
             fin_rf_subtsk_full_hnd_arr.pop_front())
        {
          fin_subtsk_in_job = fin_rf_subtsk_arr.front();
          string fin_tskhd_in_job, fin_max_line_idx, fin_tskhd_in_run,
                 fin_cur_line_idx, fin_nxt_line_idx, fin_tot_proc_cnt;
          ret = RangeFileTaskDataParser::parse_rfile_task_data_str (
            fin_subtsk_in_job,
            fin_tskhd_in_job, fin_max_line_idx, fin_tskhd_in_run,
            fin_cur_line_idx, fin_nxt_line_idx, fin_tot_proc_cnt
          );
          assert(true == ret);
          new_rf_subtsk_arr.push_back(
            fin_tskhd_in_job + MAX_LINE_STR + active_max_line_idx +
            CUR_HNDL_STR + active_tskhd_in_run + CUR_LINE_STR +
            StringUtil::convert_int_to_str(cur_line_idn) + NXT_LINE_STR +
            StringUtil::convert_int_to_str(nxt_line_idn) + TOT_PROC_STR +
            active_tot_proc_cnt
          );
          subtsk_list_before_change.push_back(
            rf_subtask_queue.get_queue_path() + "/" + 
            fin_rf_subtsk_full_hnd_arr.front()
          );
          subtsk_list_after_change.push_back(
            rf_subtask_queue.get_queue_path() + "/" + new_rf_subtsk_arr.back()
          );

#ifdef DEBUG_YAPP_SERVICE_HANDLER
          std::cerr << ">>>> CHECK TO HELP THOSE NOT RUNNING TASKS." << std::endl;
          std::cerr << ">>>> FIN TASK: " << fin_subtsk_in_job << std::endl;
          std::cerr << "     WIL HELP: " << next_tsk_chunk_str << std::endl;
          std::cerr << "     NEW TASK: " << new_rf_subtsk_arr.back() << std::endl;
#endif
          cur_line_idn = nxt_line_idn; 
          nxt_line_idn += tot_proc_cnt;
        }

        if (fin_rf_arr_size > fin_rf_subtsk_arr.size()) {
          /** enter here means we did some re-balancing **/
          string new_task_str =
            active_tskhd_in_job + MAX_LINE_STR + active_max_line_idx +
            CUR_HNDL_STR + active_tskhd_in_run + CUR_LINE_STR +
            StringUtil::convert_int_to_str(cur_line_idn - tot_proc_cnt) + NXT_LINE_STR +
            StringUtil::convert_int_to_str(nxt_line_idn - tot_proc_cnt) + TOT_PROC_STR +
            active_tot_proc_cnt;

          *subtsk_af_itr = rf_subtask_queue.get_queue_path() + "/" +
                           new_rf_subtsk_arr[z] + NEW_DELM_STR + new_task_str;

/*
          subtsk_list_before_change.pop_front();
          subtsk_list_after_change.pop_front();
          subtsk_list_before_change.push_front(
            rf_subtask_queue.get_queue_path() + "/" +
              rf_subtask_arr[cur_subtsk_idx]
          );
          subtsk_list_after_change.push_front(
            rf_subtask_queue.get_queue_path() + "/" +
            new_rf_subtsk_arr[z] + NEW_DELM_STR + new_task_str
          );
*/
        }

        if (cur_line_idn > max_line_idn) {
          z++; subtsk_af_itr++; // subtsk_bf_itr++;
        }
      }

      /** 2.3-2, find if any running tasks have certain partitions left. **/
      for (int x = 0; x < runtsk_cnt && !fin_rf_subtsk_arr.empty();)
      {
        string active_tskhd_in_job, active_max_line_idx, active_tskhd_in_run,
               active_cur_line_idx, active_nxt_line_idx, active_tot_proc_cnt;

        fin_rf_arr_size = fin_rf_subtsk_arr.size();
        cur_subtsk_idx = rf_running_subtsk_idx_arr[x];
        hnd_pos = rf_subtask_arr[cur_subtsk_idx].find(NEW_DELM_STR);

        string next_tsk_chunk_str = rf_subtask_arr[cur_subtsk_idx];
        string prev_tsk_chunk_str = rf_subtask_arr[cur_subtsk_idx];
        if (string::npos != hnd_pos) {
          /** reaching here basically means a running task helped by others **/
          prev_tsk_chunk_str = next_tsk_chunk_str.substr(0, hnd_pos);
          next_tsk_chunk_str = next_tsk_chunk_str.substr(
            hnd_pos + NEW_DELM_STR.size()
          );
        }

        bool ret = RangeFileTaskDataParser::parse_rfile_task_data_str(
          next_tsk_chunk_str, // rf_running_subtsk_arr[x],
          active_tskhd_in_job, active_max_line_idx, active_tskhd_in_run,
          active_cur_line_idx, active_nxt_line_idx, active_tot_proc_cnt
        );

        assert(true == ret);

        long long tot_proc_cnt = atoll(active_tot_proc_cnt.c_str());
        long long max_line_idn = atoll(active_max_line_idx.c_str());
        long long cur_line_idn = atoll(active_cur_line_idx.c_str());
        long long nxt_line_idn = atoll(active_nxt_line_idx.c_str());

        cur_line_idn += tot_proc_cnt;
        nxt_line_idn += tot_proc_cnt;

        if ((active_tskhd_in_job != active_tskhd_in_run) ||
            (cur_line_idn > max_line_idn)) { x++; continue; }

#ifdef DEBUG_YAPP_SERVICE_HANDLER
        std::cerr << ">>>> START LOOPING ON A RUNNING TSK, CUR_ID:"
                  << cur_line_idn << ", NXT_ID: " << nxt_line_idn
                  << std::endl;
#endif
        /** start to dispatch remaining workloads over diff. processes **/
        for (string fin_subtsk_in_job = fin_rf_subtsk_arr.front();
             cur_line_idn <= max_line_idn && !fin_rf_subtsk_arr.empty();
             fin_rf_subtsk_arr.pop_front(),
             fin_rf_subtsk_full_hnd_arr.pop_front())
        {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          std::cerr << ">>>> CUR_ID:" << cur_line_idn << ", NXT_ID: " << nxt_line_idn
                    << std::endl;
#endif
          fin_subtsk_in_job = fin_rf_subtsk_arr.front();
          string fin_tskhd_in_job, fin_max_line_idx, fin_tskhd_in_run,
                 fin_cur_line_idx, fin_nxt_line_idx, fin_tot_proc_cnt;
          ret = RangeFileTaskDataParser::parse_rfile_task_data_str (
            fin_subtsk_in_job,
            fin_tskhd_in_job, fin_max_line_idx, fin_tskhd_in_run,
            fin_cur_line_idx, fin_nxt_line_idx, fin_tot_proc_cnt
          );
          assert(true == ret);

          new_rf_subtsk_arr.push_back(
            fin_tskhd_in_job + MAX_LINE_STR + active_max_line_idx +
            CUR_HNDL_STR + active_tskhd_in_run + CUR_LINE_STR +
            StringUtil::convert_int_to_str(cur_line_idn) + NXT_LINE_STR +
            StringUtil::convert_int_to_str(nxt_line_idn) + TOT_PROC_STR +
            active_tot_proc_cnt
          );

          subtsk_list_before_change.push_back(
            rf_subtask_queue.get_queue_path() + "/" +
            fin_rf_subtsk_full_hnd_arr.front()
          );
          subtsk_list_after_change.push_back(
            rf_subtask_queue.get_queue_path() + "/" + new_rf_subtsk_arr.back()
          );
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          std::cerr << ">>>> CHECK TO HELP THOSE RUNNING TASKS." << std::endl;
          std::cerr << ">>>> FIN TASK: " << fin_subtsk_in_job << std::endl;
          std::cerr << "     WIL HELP: " << next_tsk_chunk_str << std::endl;
          std::cerr << "     NEW TASK: " << new_rf_subtsk_arr.back() << std::endl;
#endif
          cur_line_idn = nxt_line_idn; 
          nxt_line_idn += tot_proc_cnt;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          std::cerr << ">>>> CUR_ID:" << cur_line_idn << ", NXT_ID: " << nxt_line_idn
                    << std::endl;
#endif
        }
 
        if (fin_rf_arr_size > fin_rf_subtsk_arr.size()) {
        /**
         * - For those subtask being helped, we only update its new task string
         *   (which indicates next row to process) as its value under proc_arr
         *   folder only, so that the workers does not need to block to wait for
         *   the thread to finish(they only update the running_proc folder).
         * - For every subtask being helped, it will be changed to the format as
         *   ${old_tskstr}_delim_${new_tskstr} under folder proc_arr;
         */
          string new_task_str =
            active_tskhd_in_job + MAX_LINE_STR + active_max_line_idx +
            CUR_HNDL_STR + active_tskhd_in_run + CUR_LINE_STR +
            StringUtil::convert_int_to_str(cur_line_idn - tot_proc_cnt) + NXT_LINE_STR +
            StringUtil::convert_int_to_str(nxt_line_idn - tot_proc_cnt) + TOT_PROC_STR +
            active_tot_proc_cnt;
          subtsk_list_before_change.push_back(
            rf_subtask_queue.get_queue_path() + "/" + rf_subtask_arr[cur_subtsk_idx]
          );
          subtsk_list_after_change.push_back(
            rf_subtask_queue.get_queue_path() + "/" +
            prev_tsk_chunk_str + NEW_DELM_STR + new_task_str
          );
        }

        if (cur_line_idn > max_line_idn) { x++; }

      }
      /**
       * 2.4, start them all in new_rf_subtsk_arr(throw into NEW queue)
       */
      vector<string> path_to_cr_arr;
      vector<string> data_to_cr_arr;
      vector<string> path_from_arr(subtsk_list_before_change.begin(),
                                   subtsk_list_before_change.end());
      vector<string> path_to_arr(subtsk_list_after_change.begin(),
                                 subtsk_list_after_change.end());
      vector<string> path_to_del(fin_rf_subtsk_full_hnd_arr.begin(),
                                 fin_rf_subtsk_full_hnd_arr.end());
      vector<string> data_arr;
      int change_cnt = path_to_arr.size();
      for (int y = 0; y < change_cnt; y++) { data_arr.push_back(""); }
      int tsk_to_cr_cnt = new_rf_subtsk_arr.size();
      /** gethering all new tasks to the running folder & new queue **/
      for (int y = 0; y < tsk_to_cr_cnt; y++) {
        path_to_cr_arr.push_back(
          rf_running_subtask_queue.get_queue_path() + "/" + new_rf_subtsk_arr[y]
        );
        data_to_cr_arr.push_back("");
        path_to_cr_arr.push_back(
          ym_srv_ptr->newly_cr_task_queue_proxy.get_queue_path() + "/" +
          new_rf_subtsk_arr[y]
        );
        data_to_cr_arr.push_back("");
      }
      /** gethering all tasks finished and no more subtasks lefted for them **/
      int tsk_to_del_cnt = path_to_del.size();

      /**
       * for those processes finished and no other chunks left for them to help,
       * we would mark them as terminated under the job node structure.
       */
      struct timespec ts;
      clock_gettime(CLOCK_REALTIME, &ts);
      vector<string> upd_path_arr;
      vector<string> upd_data_arr;
      for (int y = 0; y < tsk_to_del_cnt; y++) {
        string upd_proc_path = ym_srv_ptr->zkc_proxy_ptr->get_proc_full_path(
          path_to_del[y].substr(0, path_to_del[y].find(MAX_LINE_STR))
        );
        upd_path_arr.push_back(upd_proc_path + "/cur_status"); 
        upd_data_arr.push_back(
          StringUtil::convert_int_to_str(PROC_STATUS_CODE::PROC_STATUS_TERMINATED)
        );
/*
        upd_path_arr.push_back(upd_proc_path + "/last_updated_tmstp_sec"); 
        upd_data_arr.push_back(StringUtil::convert_int_to_str(ts.tv_sec));
*/
      }
      rc = ym_srv_ptr->zkc_proxy_ptr->batch_set(upd_path_arr, upd_data_arr);

      for (int y = 0; y <  tsk_to_del_cnt; y++) {
        /** for the purpose of logging, we keep last chunk for each proc **/
        path_to_cr_arr.push_back(
          ym_srv_ptr->zkc_proxy_ptr->get_terminated_queue_path_prefix() + "/" +
          path_to_del[y]
        );
        path_to_del[y] =
          rf_subtask_queue.get_queue_path() + "/" + path_to_del[y];
        data_to_cr_arr.push_back("");
      }

#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << ">> FINISHED GETTING RANGE FILE CFG FOR SCHEDULING: "
                << rf_running_subtask_queue.get_queue_path() << " THREAD: "
                << pthread_self() << " IN MASTER INSTANCE. "
                << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
                << std::endl;
      std::cerr << ">>>> JOB NODES TO UPDATE: " << std::endl;
      for (size_t f = 0; f < upd_path_arr.size(); f++) {
        std::cerr << upd_path_arr[f] << std::endl;
      }
      std::cerr << std::endl;
      std::cerr << ">>>> NODES TO MV FROM: " << std::endl;
      for (size_t f = 0; f < path_from_arr.size(); f++) {
        std::cerr << path_from_arr[f] << std::endl;
      }
      std::cerr << std::endl;
      std::cerr << ">>>> NODES TO MV TO: " << std::endl;
      for (size_t f = 0; f < path_to_arr.size(); f++) {
        std::cerr << path_to_arr[f] << std::endl;
      }
      std::cerr << std::endl;
      std::cerr << ">>>> NODES TO CREATE: " << std::endl;
      for (size_t f = 0; f < path_to_cr_arr.size(); f++) {
        std::cerr << path_to_cr_arr[f] << std::endl;
      }
      std::cerr << std::endl;
      std::cerr << ">>>> NODES TO DELETE: " << std::endl;
      for (size_t f = 0; f < path_to_del.size(); f++) {
        std::cerr << path_to_del[f] << std::endl;
      }
      std::cerr << std::endl;

#endif
      /** execute them in batch (atomic via zookeeper muti api)**/
      rc = ym_srv_ptr->zkc_proxy_ptr->batch_move_node_with_no_children_and_create_del(
        path_from_arr,  path_to_arr,    data_arr,
        path_to_cr_arr, data_to_cr_arr, path_to_del
      );
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << YAPP_MSG_ENTRY[0 - rc] << std::endl;
#endif

      /** 3. release the ex lock for the task set. **/
      ym_srv_ptr->rfile_task_queue_proxy.release_ex_lock(
        lock_hnd_ret, ym_srv_ptr->zkc_proxy_ptr
      );

      usleep(BATCH_OP_NICE_TIME_IN_MICRO_SEC);
    } /** } else if (YAPP_MSG_SUCCESS == rc && 0 < subtsk_cnt && runtsk_cnt < subtsk_cnt) **/
  } /** for (int i = 0; i < tsk_cnt; i++)  **/

  /** 4. delete those entries that finished all processes successfully **/
  // ym_srv_ptr->zkc_proxy_ptr->batch_delete(rf_task_to_cleanup);
 
  status = true;
  return status;
}

bool YappServiceHandler::reschedule_tasks(YappServiceHandler * ym_srv_ptr)
{
  bool status = false;
  if (NULL == ym_srv_ptr) { return status; }
  if (YAPP_MODE_MASTER != ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) { return status; }

  vector<string> tasks_queued;
  ym_srv_ptr->yapp_master_srv_obj.newly_created_task_set.get_task_queue(
    tasks_queued, ym_srv_ptr->zkc_proxy_ptr
  );

  int subtsk_cnt = tasks_queued.size();

#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- TRYING TO RE-SCHEDULE. THREAD: " << pthread_self()
            << " IN MASTER INSTANCE. "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
  std::cerr << ">> TASKS QUEUED:" << std::endl; 
  for (int i = 0; i < subtsk_cnt; i++) {
    std::cerr << tasks_queued[i] << std::endl;
  } 
  std::cerr << std::endl;
#endif

  if (0 == tasks_queued.size()) { return true; }

  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  vector<YappServerInfo> yapp_worker_arr;
  /** get the latest list of available worker nodes in Yapp services. **/
  rc = ym_srv_ptr->yapp_master_srv_obj.get_worker_hosts(yapp_worker_arr);

  if (YAPP_MSG_SUCCESS == rc) {
    YappWorkerList yw_srv_list(yapp_worker_arr);
    for (int i = 0; i < subtsk_cnt; i++) {
      if (YAPP_MODE_MASTER != ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) { break; }
      Task subtask;
      if (1 > yw_srv_list.size()) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
        std::cerr << "==>> task no." << i << " " << tasks_queued[i]
                  << " cannot be re-scheduled, since no workers available."
                  << std::endl;
#endif
        break;
      }
      status = YappDomainFactory::get_subtask_by_proc_hnd(
        subtask, tasks_queued[i], ym_srv_ptr->zkc_proxy_ptr
      );
      if (true == status) {
        status = ym_srv_ptr->yapp_master_srv_obj.schedule_job(
          vector<Task>(1, subtask), yw_srv_list
        );
        if (true == status) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          std::cerr << "==>> task no." << i << " " << tasks_queued[i]
                    << " got re-scheduled" << std::endl;
#endif
        } else {
          /* update the worker list once it finds any node fail */
          yapp_worker_arr.clear();
          rc = ym_srv_ptr->yapp_master_srv_obj.get_worker_hosts(yapp_worker_arr);
          if (YAPP_MSG_SUCCESS == rc) {
            yw_srv_list.reset_cur_yapp_worker(yapp_worker_arr);
          }
#ifdef DEBUG_YAPP_SERVICE_HANDLER
          std::cerr << "==>> task no." << i << " " << tasks_queued[i]
                    << " cannot be re-scheduled, either currently scheduled by"
                    << " other thread or no workers available." << std::endl;
#endif
        }
      } else {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
        std::cerr << "==>> task no." << i << " " << tasks_queued[i]
                  << " cannot be loaded!" << std::endl;
#endif
      }
      if (YAPP_MSG_SUCCESS != rc) { break; }
    }
  }
  return status;
}

void * YappServiceHandler::thread_reschedule_tasks(void * srv_ptr)
{
  // pthread_detach(pthread_self());
  YappServiceHandler * ym_srv_ptr = (YappServiceHandler *)srv_ptr;
  bool b_exit = false;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << std::endl << "-- STARTING SCHEDULING THREAD: "
            << pthread_self() << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  if (NULL != ym_srv_ptr) {
    while (false == b_exit) {
      if (YAPP_MODE_MASTER == ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) {
        /**
         * using a mutex to avoid mutiple thread running at the same time which
         * may cause a fsync spike on the zookeeper.
         */
        pthread_mutex_lock(&yapp_inst_mutex);
        reschedule_tasks(ym_srv_ptr);
        pthread_mutex_unlock(&yapp_inst_mutex);
      } else { break; }
      int intval = 30;
      ym_srv_ptr->get_intvl_atomic(&(ym_srv_ptr->subtask_scheule_polling_rate_sec),&intval);
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "---- thread_reschedule_tasks Polling Rate: "
                << intval << " secs." << std::endl;
#endif
      for (int i = 0; i < intval; i++) {
        if (ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_thrd_stop)) ||
            ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_exit)))
        { b_exit = true; break; } else { sleep(1); }
      }
    } // while (false == b_exit)
  }
  std::cout << "---- thread_reschedule_tasks terminated." << std::endl;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- TERMINATING SCHEDULING THREAD: " << pthread_self()
            << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  return NULL;
}

void * YappServiceHandler::thread_check_zombie_task(void * srv_ptr)
{
  // pthread_detach(pthread_self());
  YappServiceHandler * ym_srv_ptr = (YappServiceHandler *)srv_ptr;
  bool b_exit = false;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << std::endl << "-- STARTING TASK-CHECKING THREAD: "
            << pthread_self() << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  if (NULL != ym_srv_ptr) {
    while (false == b_exit) {
      if (YAPP_MODE_MASTER == ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) {
        /** master would check to re-queue those tasks died in the middle. **/
        pthread_mutex_lock(&yapp_inst_mutex);
        check_zombie_task(ym_srv_ptr);
        pthread_mutex_unlock(&yapp_inst_mutex);
      } else { break; }
      int intval = 30;
      ym_srv_ptr->get_intvl_atomic(&(ym_srv_ptr->zombie_check_polling_rate_sec),&intval);
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "---- thread_check_zombie_task Polling Rate: "
                << intval << " secs." << std::endl;
#endif
      for (int i = 0; i < intval; i++) {
        if (ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_thrd_stop)) ||
            ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_exit)))
        { b_exit = true; break; } else { sleep(1); }
      }
    } // while (false == b_exit)
  }
  std::cout << "---- thread_check_zombie_task terminated." << std::endl;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- TERMINATING TASK-CHECKING: " << pthread_self()
            << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  return NULL;
}

bool YappServiceHandler::check_zombie_task(YappServiceHandler * ym_srv_ptr)
{
  bool status = false;
  status  = check_zombie_task(ym_srv_ptr, ym_srv_ptr->running_task_queue_proxy);
  status &= check_zombie_task(ym_srv_ptr, ym_srv_ptr->ready_task_queue_proxy);
  return status;
}

void * YappServiceHandler::thread_auto_split_rfile_tasks(void * srv_ptr) {
  // pthread_detach(pthread_self());
  YappServiceHandler * ym_srv_ptr = (YappServiceHandler *)srv_ptr;
  bool b_exit = false;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << std::endl << "-- STARTING RFTASK-AUTOSPLIT THREAD: "
            << pthread_self() << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  if (NULL != ym_srv_ptr) {
    while (false == b_exit) {
      if (YAPP_MODE_MASTER == ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) {
        /** master would check to re-queue those tasks died in the middle. **/
        pthread_mutex_lock(&yapp_inst_mutex);
        auto_split_rfile_tasks(ym_srv_ptr);
        pthread_mutex_unlock(&yapp_inst_mutex);
      } else { break; }
      int intval = 30;
      ym_srv_ptr->get_intvl_atomic(&(ym_srv_ptr->rftask_autosp_polling_rate_sec),&intval);
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "---- thread_auto_split_rfile_tasks Polling Rate: "
                << intval << " secs." << std::endl;
#endif
      for (int i = 0; i < intval; i++) {
        if (ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_thrd_stop)) ||
            ym_srv_ptr->chk_bflag_atomic(&(ym_srv_ptr->b_flag_exit)))
        { b_exit = true; break; } else { sleep(1); }
      }
    } // while (false == b_exit)
  }
  std::cout << "---- thread_auto_split_rfile_tasks terminated." << std::endl;
#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- TERMINATING RFTASK-AUTOSPLIT THREAD: " << pthread_self()
            << " IN MASTER INSTANCE: "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
#endif
  return NULL;
}

bool YappServiceHandler::auto_split_rfile_tasks(YappServiceHandler * ym_srv_ptr)
{
  bool status = false;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == ym_srv_ptr) { return status; }

  vector<string> rftask_node_arr;
  vector<string> rftask_path_arr;
  vector<string> termin_task_arr;

  rc = ym_srv_ptr->rfile_task_queue_proxy.get_task_queue (
    rftask_node_arr, ym_srv_ptr->zkc_proxy_ptr
  );
  if (YAPP_MSG_SUCCESS != rc) { return status; }

  string tskhd_in_job, max_line_idx, tskhd_in_run,
         cur_line_idx, nxt_line_idx, tot_proc_cnt;

  string hndl, hndl_in_queue, task_hndl;
  size_t hnd_pos = string::npos;

  int total_proc_cnt = 0, next_line_idx = 0, max_line_int = 0, term_stsk_cnt =0;
  int task_hndl_cnt = rftask_node_arr.size();

  string path = ym_srv_ptr->rfile_task_queue_proxy.get_queue_path();
  for (int i = 0; i < task_hndl_cnt; i++) {
    rftask_path_arr.push_back(path + "/" + rftask_node_arr[i]);
  }

  bool b_termin_queue_loaded = false;
  for (int i = 0; i < task_hndl_cnt; i++) {
    vector<string> runing_hndl_arr;
    int runing_hndl_arr_cnt = 0;

    rc = ym_srv_ptr->zkc_proxy_ptr->get_node_arr(
      rftask_path_arr[i] + "/proc_arr", runing_hndl_arr
    );
    if (YAPP_MSG_SUCCESS != rc) { break; }
    if (runing_hndl_arr.empty()) { continue; }

#ifdef DEBUG_YAPP_SERVICE_HANDLER
    std::cerr << "$$ CHECKING RFTASK-AUTOSPLIT FOR PATH: "
              << rftask_path_arr[i] << std::endl;
#endif

    vector<string> node_to_del;
    vector<string> node_to_cre;

    runing_hndl_arr_cnt = runing_hndl_arr.size();
    for (int c = 0; c < runing_hndl_arr_cnt; c++) {
      hnd_pos = runing_hndl_arr[c].find(NEW_DELM_STR);
      hndl = runing_hndl_arr[c];
      hndl_in_queue = runing_hndl_arr[c];

      if (string::npos != hnd_pos) {
        hndl = runing_hndl_arr[c].substr(hnd_pos + NEW_DELM_STR.size());
        hndl_in_queue = runing_hndl_arr[c].substr(0, hnd_pos);
      }
      status = RangeFileTaskDataParser::parse_rfile_task_data_str(
        hndl, tskhd_in_job, max_line_idx, tskhd_in_run,
              cur_line_idx, nxt_line_idx, tot_proc_cnt
      );
      if (false == status) { break; }
      total_proc_cnt = atoi(tot_proc_cnt.c_str());
      if (total_proc_cnt < runing_hndl_arr_cnt) { status = false; break; }
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "$$ TOTAL PROC SHOULD RUN:" << total_proc_cnt
                << " TOTAL PROC CURR. RUN: " << runing_hndl_arr_cnt << std::endl;
#endif
      if (total_proc_cnt == runing_hndl_arr_cnt) { break; }

      next_line_idx = atoll(nxt_line_idx.c_str());
      max_line_int = atoll(max_line_idx.c_str());
      /** if the next line exceed the max line id, then check next subtask. **/
      if (max_line_int < next_line_idx) { continue; }
      /** if not, then check if it was failed or paused **/
      rc = ym_srv_ptr->zkc_proxy_ptr->node_exists(
        rftask_path_arr[i] + "/failed_procs/" + hndl_in_queue 
      );
      if (YAPP_MSG_INVALID == rc) { status = false; break; }
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      if (YAPP_MSG_SUCCESS != rc) {
        std::cerr << "$$ PROC NOT FAILED:" << hndl_in_queue << std::endl;
      } else {
        std::cerr << "$$ PROC FAILED:"
                  << rftask_path_arr[i] + "/failed_procs/" + hndl_in_queue
                  << std::endl;
      }
#endif
      if (YAPP_MSG_SUCCESS == rc) { continue; }

      rc = ym_srv_ptr->zkc_proxy_ptr->node_exists(
        ym_srv_ptr->paused_task_queue_proxy.get_queue_path() +
        "/" + hndl_in_queue
      );
      if (YAPP_MSG_INVALID == rc) { status = false; break; }
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      if (YAPP_MSG_SUCCESS != rc) {
        std::cerr << "$$ PROC NOT PAUSED:" << hndl_in_queue << std::endl;
      } else {
        std::cerr << "$$ PROC PAUSED:"
                  << (ym_srv_ptr->zkc_proxy_ptr->get_paused_queue_path_prefix()
                      + "/" + hndl_in_queue)
                  << std::endl;
      }
#endif
      if (YAPP_MSG_SUCCESS == rc) { continue; }

      /** load the terminated queue in a lazy fashion only when needed. **/
      if (false == b_termin_queue_loaded) {
        rc = ym_srv_ptr->termin_task_queue_proxy.get_task_queue(
          termin_task_arr, ym_srv_ptr->zkc_proxy_ptr
        );
        if (YAPP_MSG_SUCCESS != rc) { status = false; break; }
        term_stsk_cnt = termin_task_arr.size();
        b_termin_queue_loaded = true;
      }

      for (int s = 0; s < term_stsk_cnt; s++) {
        if (0 == termin_task_arr[s].find(rftask_node_arr[i])) {
          node_to_del.push_back(
            ym_srv_ptr->termin_task_queue_proxy.get_queue_path() +
            "/" + termin_task_arr[s]
          );
          node_to_cre.push_back(
            rftask_path_arr[i] + "/proc_arr/" + termin_task_arr[s]
          );
        }
      }
      if (node_to_del.empty()) { continue; }
      rc = ym_srv_ptr->zkc_proxy_ptr->batch_unlock_delete_and_create(
        node_to_del, node_to_cre
      );
      if (YAPP_MSG_SUCCESS != rc) { status = false; }
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "$$ SUCCEED IN RFTASK-AUTOSPLIT FOR PATH: "
                << rftask_path_arr[i] << std::endl;
      for (size_t x = 0; x < node_to_cre.size(); x++) {
        std::cerr << ">>>> " << node_to_cre[x] << std::endl;
      }
#endif
      break;
    } 
    if (false == status) { break; }
  }

  return status;
}

bool YappServiceHandler::check_zombie_task(YappServiceHandler * ym_srv_ptr,
                                           YappSubtaskQueue & task_queue_proxy)
{
  bool status = false;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == ym_srv_ptr) { return status; }
  if (YAPP_MODE_MASTER != ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) { return status; }

  vector<string> tasks_queued;
  vector<string> tasks_zombie;
  vector<string> tasks_lockhd;

  rc = task_queue_proxy.get_task_queue (
    tasks_queued, ym_srv_ptr->zkc_proxy_ptr
  );
  if (YAPP_MSG_SUCCESS != rc) { return status; }

  int subtsk_cnt = tasks_queued.size();

  map<string, bool>   hosts_to_bfenced_map;

  string lock_hnd_ret, host_ipaddr,  stdout_str;
  string tskhd_in_job, max_line_idx, tskhd_in_run,
         cur_line_idx, nxt_line_idx, tot_proc_cnt;

  for (int i = 0; i < subtsk_cnt; i++) {
    if (YAPP_MODE_MASTER != ym_srv_ptr->zkc_proxy_ptr->get_yapp_mode()) { break; }
    rc = ym_srv_ptr->zkc_proxy_ptr->node_exists(
      task_queue_proxy.get_queue_path() + "/" + tasks_queued[i] + "/" +
      WRITE_LOCK_PREFIX_STR
    );
    if (YAPP_MSG_SUCCESS == rc) { continue; }
 
    rc = task_queue_proxy.try_acquire_ex_lock(
      lock_hnd_ret, tasks_queued[i], ym_srv_ptr->zkc_proxy_ptr
    );
    if (YAPP_MSG_SUCCESS != rc) { continue; }

    /**
     * following piece of code is solely for invoking fencing script to ban the
     * worker lost the heart-beat to zookeeper (possibly by changing the switch)
     * to make sure that before a subtask got requeued, the original running one
     * won't be able to continue to access the shared resources.
     */
    if (ym_srv_ptr->fencing_script_path.size() > 0) {
      if (RangeFileTaskDataParser::is_range_file_task_hnd(tasks_queued[i])) {
        status = RangeFileTaskDataParser::parse_rfile_task_data_str(
          tasks_queued[i], tskhd_in_job, max_line_idx, tskhd_in_run,
                           cur_line_idx, nxt_line_idx, tot_proc_cnt
        );
      } else {
        tskhd_in_job = tasks_queued[i];
      }

      rc = ym_srv_ptr->zkc_proxy_ptr->get_node_data(
        ym_srv_ptr->zkc_proxy_ptr->get_proc_full_path(tskhd_in_job)+"/host_str",
        host_ipaddr
      );
      if (YAPP_MSG_SUCCESS != rc || host_ipaddr.empty()) { continue; }

      /** /foo/yapp/conf/nodes/yapp-0000000117:192.168.1.1:9529:20876 **/
      /** calling the fencing **/
      size_t start_pos = host_ipaddr.find(':') + 1;
      size_t len       = host_ipaddr.substr(start_pos).find(':');
      host_ipaddr      = host_ipaddr.substr(start_pos, len);

      if (hosts_to_bfenced_map.end() == hosts_to_bfenced_map.find(host_ipaddr)){
        int ret = TaskHandleUtil::execute_fencing_script_for_host(
          ym_srv_ptr->fencing_script_path, host_ipaddr, stdout_str
        );
#ifdef DEBUG_YAPP_SERVICE_HANDLER
        std::cerr << "$$$$ TRYING TO CALLING FENCING SCRIPT TO BAN: "
                  << host_ipaddr << ", RET: " << ret << std::endl;
        std::cerr << stdout_str << std::endl;
#endif 
        if (0 == ret) {
          hosts_to_bfenced_map[host_ipaddr] = true;
        } else {
          task_queue_proxy.release_ex_lock(
            lock_hnd_ret, ym_srv_ptr->zkc_proxy_ptr
          );
          continue;
        }
      }
    }

    tasks_zombie.push_back(tasks_queued[i]);
    tasks_lockhd.push_back(lock_hnd_ret);
  }

#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "-- TRYING TO CHECK ZOMBIE IN QUEUE: "
            << task_queue_proxy.get_queue_path() << " THREAD: "
            << pthread_self() << " IN MASTER INSTANCE. "
            << ym_srv_ptr->zkc_proxy_ptr->get_host_str_in_pcb()
            << std::endl;
  std::cerr << ">> ZOMBIE TASKS: "; 
  for (size_t i = 0; i < tasks_zombie.size(); i++) {
    std::cerr << tasks_zombie[i] << " ";
  } 
  std::cerr << std::endl;
#endif
  subtsk_cnt = tasks_zombie.size();
  string new_qpath =
    ym_srv_ptr->yapp_master_srv_obj.newly_created_task_set.get_queue_path();
  string ready_qpath = task_queue_proxy.get_queue_path();
  for (int i = 0; i < subtsk_cnt; i++) {
    rc = ym_srv_ptr->zkc_proxy_ptr->move_node_with_no_children(
      ready_qpath + "/" + tasks_zombie[i], new_qpath + "/" + tasks_zombie[i],
      "", tasks_lockhd[i]
    );
    if (YAPP_MSG_SUCCESS == rc) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "==>> task no." << i << " " << tasks_zombie[i]
                << " got re-queued." << std::endl;
#endif
    } else {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
      std::cerr << "==>> fail to get task no." << i << " " << tasks_zombie[i]
                << " re-queued" << std::endl;
#endif
      task_queue_proxy.release_ex_lock(
        tasks_lockhd[i], ym_srv_ptr->zkc_proxy_ptr
      );
    }
  }
  status = true;
  return status;
}

bool YappServiceHandler::start_yapp_event_handler() {
  return (
    0 == pthread_create(&event_hndl_thrd_id, NULL, thread_event_handler, this)
  );
}

void * YappServiceHandler::thread_event_handler(void * srv_ptr) {
  /** 1. attach the shared resource if needed(not apply during unit testing */
  YappServiceHandler * ym_srv_ptr = (YappServiceHandler *)srv_ptr;
  if (0 > ym_srv_ptr->shrd_mem_id || NULL == ym_srv_ptr) { return NULL; }

  char * shrd_mem_ptr = (char *) shmat(ym_srv_ptr->shrd_mem_id, 0, 0);
  sem_t * shrd_mutex_ptr = sem_open(IPC_CONTEX::YAPPD_SHARED_MUTEX_NAME.c_str(), 0);
  sem_t * shrd_condv_ptr = sem_open(IPC_CONTEX::YAPPD_SHARED_CONDV_NAME.c_str(), 0);
  bool b_cont_run = true;

  if ((void *) -1 == shrd_mem_ptr || SEM_FAILED == shrd_mutex_ptr ||
                                     SEM_FAILED == shrd_condv_ptr) {
    std::cerr << ">>>> Failed In Attaching Shared Memory for Yappd Child."
              << std::endl;
    return NULL;
  }

  int cur_signo = -1;
  /** 2. listening on the conditional var., wait for incoming events **/
  while (true == b_cont_run) {
#ifdef DEBUG_YAPP_DAEMON_APP
    std::cerr << ">>>> Child Start To Wait On the Cond. Var: "
              << shrd_condv_ptr << std::endl;
#endif
    /** having new signals coming, check the shared memory **/
    if (0 != sem_wait(shrd_condv_ptr)) { break; }

#ifdef DEBUG_YAPP_DAEMON_APP
    std::cerr << ">>>> Child Start To Wait On the Mutex: "
              << shrd_mutex_ptr << std::endl;
#endif
    if (0 != sem_wait(shrd_mutex_ptr)) { break; }
    cur_signo = (int) shrd_mem_ptr[0]; 
#ifdef DEBUG_YAPP_DAEMON_APP
    std::cerr << ">>>> Child Start To Write Read the Flag: " << cur_signo << std::endl;
#endif
    if (0 != sem_post(shrd_mutex_ptr)) { break; }

    switch (cur_signo) {
      case SIGTERM: {
        b_cont_run = false;
        /**
         * After this point, master or worker will refuse to accept any request
         */
#ifdef DEBUG_YAPP_DAEMON_APP
        std::cerr << ">>>> Child Get SIGTERM" << std::endl;
#endif
        ym_srv_ptr->set_bflag_atomic(&(ym_srv_ptr->b_flag_exit), true);
        break;
      }
      case SIGHUP:  {
        /**
         * reload the config and apply them using a rw lock, also update flags
         * on shared memory so that the controlling process konw it is done so
         * that futrue reload requests can be accepted in the controlling proc.
         */
#ifdef DEBUG_YAPP_DAEMON_APP
        std::cerr << ">>>> Child Get SIGHUP" << std::endl;
#endif
        if (true == ym_srv_ptr->reload_conf_from_file() &&
            true == ym_srv_ptr->apply_dynamic_conf_atomic()) {
#ifdef DEBUG_YAPP_DAEMON_APP
          std::cerr << ">>>> Child Finished Reload, Reset the Flag" << std::endl;
#endif
          if (0 != sem_wait(shrd_mutex_ptr)) { break; }
          shrd_mem_ptr[0] = 0;
          if (0 != sem_post(shrd_mutex_ptr)) { break; }
#ifdef DEBUG_YAPP_DAEMON_APP
          std::cerr << ">>>> Child Finished Resetting the Flag" << std::endl;
#endif
        }
        break;
      }
      default: {
        std::cerr << ">>>> UnExpected Signal: " << (int) shrd_mem_ptr[0]
                  << std::endl;
        break;
      }
    } // switch (cur_signo)
  } // while (true == b_cont_run)

  /** 3. detach the shared memory(upon sigterm) and close the condional var. **/
  shmdt(shrd_mem_ptr);
  sem_close(shrd_condv_ptr);
  sem_close(shrd_mutex_ptr);

  /**
   * 4. ends thread thread_node_leave_check to keep current node identity, it
   *    will 1st try to wait for a few secs. in hope that the thread in sleep.
   */
  // pthread_cancel(ym_srv_ptr->node_lvchk_thrd_id);
  pthread_join(ym_srv_ptr->node_lvchk_thrd_id, NULL);
  pthread_join(ym_srv_ptr->check_point_thrd_id, NULL);

  /** 5. best effort cleanup and graceful exit **/
  /**
   * 5-1, worker will first stop accepting new subtask requests(filtered by
   *      setting negative job limits), and issues kill to all subtasks and
   *      wait until all threads finished waiting their child processes and
   *      updating the zookeeper.
   */
  ym_srv_ptr->yapp_worker_srv_obj.set_max_concur_task(-1);
  /**
   * 5-2, master will 1st attempt to stop all utility threads and wait for
   *      all current requests been processed.
   */
  ym_srv_ptr->stop_all_master_threads();

  /**
   * 6. kill all working processes immediately, note that master may also need
   *    to perform the cleaning since some jobs may still running during the
   *    window period after it gaining the mastership.
   */
  ym_srv_ptr->yapp_worker_srv_obj.kil_pid_in_registry_atomic();

  /** free all resources held by the zookeeper lib. **/
  delete ym_srv_ptr->zkc_proxy_ptr;
  exit(0);
}

void YappServiceHandler::get_all_subtask_hndl_arr_in_queue(
  vector<string> & proc_hndl_arr, const vector<string> & rndm_hndl_arr,
  YappSubtaskQueue & queue_obj)
{
  YAPP_MSG_CODE rc = YAPP_MSG_SUCCESS;
  int hndl_str_type = TaskHandleUtil::TASK_HANDLE_TYPE_INVALID;
  int hndl_arr_size = rndm_hndl_arr.size();
  int task_buf_size = 0;
  vector<string> task_queue_buf;
  map<string, int> uniq_task_map;

  rc = queue_obj.sync_and_get_queue(
    task_queue_buf, zkc_proxy_ptr
  );
  if (YAPP_MSG_SUCCESS != rc) { return; }
  task_buf_size = task_queue_buf.size();
  for (int i = 0; i < task_buf_size; i++) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
    std::cerr << "---- " << task_queue_buf[i] << " FOUND!" << std::endl;
#endif 
    uniq_task_map[task_queue_buf[i]] = 1;
  }
  task_queue_buf.clear();

#ifdef DEBUG_YAPP_SERVICE_HANDLER
  std::cerr << "---- going to find proc hndl satisfies:" << std::endl;
  for (size_t i = 0; i < rndm_hndl_arr.size(); i++) {
    std::cerr << "++++ " << rndm_hndl_arr[i] << std::endl;
  }
  std::cerr << "---- found proc hndls from the queue:" << std::endl;
  map<string, int>::iterator itra = uniq_task_map.begin();
  for (; itra != uniq_task_map.end(); itra++) {
    std::cerr << "++++ " << itra->first << std::endl;
  }
#endif
  map<string, int>::iterator itr;
  for (int i = 0; i < hndl_arr_size; i++) {
    hndl_str_type = TaskHandleUtil::get_task_handle_type(rndm_hndl_arr[i]);
    switch(hndl_str_type) {
    case TaskHandleUtil::TASK_HANDLE_TYPE_JOBS:
    case TaskHandleUtil::TASK_HANDLE_TYPE_TASK:
    case TaskHandleUtil::TASK_HANDLE_TYPE_PROC:
      itr = uniq_task_map.find(rndm_hndl_arr[i]);
      if (uniq_task_map.end() != itr) {
        proc_hndl_arr.push_back(itr->first);
        uniq_task_map.erase(itr);
      } else {
        vector<string> hndl_to_del;
        int hndl_to_del_cnt = 0;
        itr = uniq_task_map.begin();
        for (; itr != uniq_task_map.end(); itr++) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
    std::cerr << "---- itr: " << itr->first << std::endl;
    std::cerr << "++++ hnd: " << rndm_hndl_arr[i] << std::endl;
    std::cerr << "---- itr->first[rndm_hndl_arr[i].size()]: "
              << itr->first[rndm_hndl_arr[i].size()] << std::endl;
#endif 
          if ((0 == itr->first.find(rndm_hndl_arr[i])) &&
              ('_' == itr->first[rndm_hndl_arr[i].size()])) {
            // || ((0 == rndm_hndl_arr[i].find(itr->first)))) {
#ifdef DEBUG_YAPP_SERVICE_HANDLER
    std::cerr << "---- PUSHED: " << itr->first << std::endl;
#endif
            proc_hndl_arr.push_back(itr->first);
            hndl_to_del.push_back(itr->first);
          }
        } // for (; itr != uniq_task_map.end(); itr++) {
        hndl_to_del_cnt = hndl_to_del.size();
        for (int i = 0; i < hndl_to_del_cnt; i++) {
          uniq_task_map.erase(hndl_to_del[i]);
        }
      }
      break;
    default:
      rc = YAPP_MSG_INVALID; break;
    }
    if (uniq_task_map.empty() || YAPP_MSG_SUCCESS != rc) { break; }
  }
}

void YappServiceHandler::get_all_paused_subtask_hndl_arr_rpc(
  vector<string> & proc_hndl_arr, const vector<string> & rndm_hndl_arr)
{
  get_all_subtask_hndl_arr_in_queue(
    proc_hndl_arr, rndm_hndl_arr, paused_task_queue_proxy
  );
}

void YappServiceHandler::get_all_active_subtask_hndl_arr_rpc(
  vector<string> & proc_hndl_arr, const vector<string> & rndm_hndl_arr)
{
  get_all_subtask_hndl_arr_in_queue(
    proc_hndl_arr, rndm_hndl_arr, running_task_queue_proxy
  );
}


void YappServiceHandler::get_all_terminated_subtask_hndl_arr_rpc(
  vector<string> & proc_hndl_arr, const vector<string> & rndm_hndl_arr)
{
  get_all_subtask_hndl_arr_in_queue(
    proc_hndl_arr, rndm_hndl_arr, termin_task_queue_proxy
  );
}

void YappServiceHandler::get_all_failed_subtask_hndl_arr_rpc(
  vector<string> & proc_hndl_arr, const vector<string> & rndm_hndl_arr)
{
  get_all_subtask_hndl_arr_in_queue(
    proc_hndl_arr, rndm_hndl_arr, failed_task_queue_proxy
  );
}


/**
 * Following parts are rpc service as a Yapp Worker.
 */
bool YappServiceHandler::execute_sub_task_async_rpc(const Task & sub_task)
{
  if (true == chk_bflag_atomic(&b_flag_exit)) { return false; }
  return yapp_worker_srv_obj.execute_sub_task_async_rpc(sub_task);
}

void YappServiceHandler::signal_sub_task_async_rpc(vector<bool> & ret_arr,
  const vector<int> & pid_arr, const vector<string> & phd_arr, const int signal)
{
  if (true == chk_bflag_atomic(&b_flag_exit)) {
    ret_arr.resize(pid_arr.size(), false);
  } else {
    yapp_worker_srv_obj.signal_sub_task_async_rpc(
      ret_arr, pid_arr, phd_arr, signal
    );
  }
}

/**
 * Following parts are rpc service as a Yapp Master.
 */

void YappServiceHandler::submit_job_rpc(Job & job_obj, const Job & job_to_sub)
{
  if (true == chk_bflag_atomic(&b_flag_exit)) { return; }
  yapp_master_srv_obj.submit_job_rpc(job_obj, job_to_sub);
}

bool YappServiceHandler::notify_proc_completion_rpc(const string & proc_handle,
                                                    int ret_val, int term_sig)
{
  if (true == chk_bflag_atomic(&b_flag_exit)) { return false; }
  return yapp_master_srv_obj.notify_proc_completion_rpc(
    proc_handle, ret_val, term_sig
  );
}

void YappServiceHandler::print_job_tree_rpc(string & tree_str,
                                      const string & job_handle)
{
  if (true == chk_bflag_atomic(&b_flag_exit)) { return; }
  yapp_master_srv_obj.print_job_tree_rpc(tree_str, job_handle);
}
void YappServiceHandler::print_task_tree_rpc(string & tree_str,
                                       const string & task_handle)
{
  if (true == chk_bflag_atomic(&b_flag_exit)) { return; }
  yapp_master_srv_obj.print_task_tree_rpc(tree_str, task_handle);
}
void YappServiceHandler::print_proc_tree_rpc(string & tree_str,
                                       const string & proc_handle)
{
  if (true == chk_bflag_atomic(&b_flag_exit)) { return; }
  yapp_master_srv_obj.print_proc_tree_rpc(tree_str, proc_handle);
}

void YappServiceHandler::print_queue_stat(string & tree_str,
                                    const string & hndl_str) {
  if (true == chk_bflag_atomic(&b_flag_exit)) { return; }
  yapp_master_srv_obj.print_queue_stat(tree_str, hndl_str);
}

void YappServiceHandler::print_failed_queue_stat(string & tree_str,
                                           const string & hndl_str) {
  if (true == chk_bflag_atomic(&b_flag_exit)) { return; }
  yapp_master_srv_obj.print_failed_queue_stat(tree_str, hndl_str);
}

void YappServiceHandler::pause_proc_arr_rpc(vector<bool> & ret_arr,
                                      const vector<string>& proc_hndl_arr) {
  if (true == chk_bflag_atomic(&b_flag_exit)) {
    ret_arr.resize(proc_hndl_arr.size(), false);
  } else {
    yapp_master_srv_obj.pause_proc_arr_rpc(ret_arr, proc_hndl_arr);
  }
}

void YappServiceHandler::resume_proc_arr_rpc(vector<bool> & ret_arr,
                                       const vector<string>& proc_hndl_arr) {
  if (true == chk_bflag_atomic(&b_flag_exit)) {
    ret_arr.resize(proc_hndl_arr.size(), false);
  } else {
    yapp_master_srv_obj.resume_proc_arr_rpc(ret_arr, proc_hndl_arr);
  }
}

void YappServiceHandler::terminate_proc_arr_rpc(vector<bool> & ret_arr,
                                          const vector<string> & proc_hndl_arr){
  if (true == chk_bflag_atomic(&b_flag_exit)) {
    ret_arr.resize(proc_hndl_arr.size(), false);
  } else {
    yapp_master_srv_obj.terminate_proc_arr_rpc(ret_arr, proc_hndl_arr);
  }
}

void YappServiceHandler::restart_failed_proc_arr_rpc(
  vector<bool> & ret_arr, const vector<string> & fail_proc_hndls)
{
  if (true == chk_bflag_atomic(&b_flag_exit)) {
    ret_arr.resize(fail_proc_hndls.size(), false);
  } else {
    yapp_master_srv_obj.restart_failed_proc_arr_rpc(ret_arr, fail_proc_hndls);
  }
}

void YappServiceHandler::fully_restart_proc_arr_rpc(
  vector<bool> & ret_arr, const vector<string> & term_proc_hndls,
                          const vector<string> & fail_proc_hndls){
  if (true == chk_bflag_atomic(&b_flag_exit)) {
    ret_arr.resize(term_proc_hndls.size() + fail_proc_hndls.size(), false);
  } else {
    yapp_master_srv_obj.fully_restart_proc_arr_rpc(
      ret_arr, term_proc_hndls, fail_proc_hndls
    );
  }
}

void YappServiceHandler::restart_from_last_anchor_proc_arr_rpc(
  vector<bool> & ret_arr, const vector<string> & term_proc_hndls,
                          const vector<string> & fail_proc_hndls){
  if (true == chk_bflag_atomic(&b_flag_exit)) {
    ret_arr.resize(term_proc_hndls.size() + fail_proc_hndls.size(), false);
  } else {
    yapp_master_srv_obj.restart_from_last_anchor_proc_arr_rpc(
      ret_arr, term_proc_hndls, fail_proc_hndls
    );
  }
}

bool YappServiceHandler::purge_job_tree_rpc(const string & job_handle) {
  if (true == chk_bflag_atomic(&b_flag_exit)) { return false; }
  return yapp_master_srv_obj.purge_job_tree_rpc(job_handle);
}

void YappServiceHandler::query_running_task_basic_info_rpc(
  map<string, map<string, vector<string> > > & tsk_grp_arr,
  const vector<string> & host_arr, const vector<string> & owner_arr) {

  tsk_grp_arr.clear();
  ZkClusterProxy * zkc_hndl = zkc_proxy_ptr;
  if (NULL != zkc_hndl) {
    string cur_host = "Master: " + ZkClusterProxy::get_ip_addr_v4_lan();
    map<string, vector<string> > cur_tsk_group;
    yapp_worker_srv_obj.get_tasks_group_atomic(cur_tsk_group, owner_arr);
    tsk_grp_arr[cur_host] = cur_tsk_group;
    vector<string> worker_arr;
    int host_cnt = host_arr.size();
    for (int i = 0; i < host_cnt; i++) {
      if (cur_host != host_arr[i]) { worker_arr.push_back(host_arr[i]); }
    }
    yapp_master_srv_obj.query_running_task_basic_info(
      tsk_grp_arr, worker_arr, owner_arr
    );
  }
}

void YappServiceHandler::get_running_task_basic_info_rpc(
  map<string, vector<string> > & subtsk_info, const vector<string> & owner_arr){

  yapp_worker_srv_obj.get_tasks_group_atomic(subtsk_info, owner_arr);
}

void YappServiceHandler::query_yappd_envs_rpc(
  map<string, map<string, string> > & host_to_kv_map) {
  host_to_kv_map.clear();
  ZkClusterProxy * zkc_hndl = zkc_proxy_ptr;
  if (NULL != zkc_hndl) {
    string cur_host = ZkClusterProxy::get_ip_addr_v4_lan();
    host_to_kv_map["Master: " + cur_host] = ypconf_obj.get_envs_map(true);
    yapp_master_srv_obj.query_yappd_envs(host_to_kv_map, cur_host);
  }
}

void YappServiceHandler::get_running_envs_rpc(map<string, string> & env_kv_map){
  env_kv_map = ypconf_obj.get_envs_map(false);
}
