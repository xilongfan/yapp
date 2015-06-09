#include <cstdlib>
#include <cerrno>
#include <cassert>
#include <cstring>

#include <unistd.h>
#include <signal.h>
#include <pthread.h>

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <semaphore.h>



#include "./yappd.h"
#include "./util/yapp_util.h"
#include "./master/yapp_master.h"
#include "./worker/yapp_worker.h"

using namespace yapp;
using namespace yapp::base;
using namespace yapp::util;
using yapp::master::YappMaster;
using yapp::worker::YappWorker;

static pthread_mutex_t yappd_app_mutex = PTHREAD_MUTEX_INITIALIZER;

YAPP_MSG_CODE YappDaemonApp::parse_args(int argc, char * argv[]) {
  YAPP_MSG_CODE status = YAPP_MSG_SUCCESS;
  string opt_val;
  for (int i = 1; i < argc; i++) {
    string arg_str(argv[i]);
    bool b_opt_str = false;
    std::transform(
        arg_str.begin(), arg_str.end(), arg_str.begin(), ::tolower
    );
    StringUtil::trim_string(arg_str);
    int mode_entry_cnt = YAPP_DAEMON_OPT_COUNT;
    for (int c = 0; c < mode_entry_cnt; c++) {
      if (0 == arg_str.find(YAPP_DAEMON_OPT_ENTRY[c])) {
        opt_val = arg_str.substr(strlen(YAPP_DAEMON_OPT_ENTRY[c]));
        switch((YAPP_DAEMON_OPT_ENTRY_MAP)c) {
        case YAPP_DAEMON_OPT_MODE: {
          if ("master" == opt_val) { yapp_mode_code = YAPP_MODE_MASTER; }
          if ("worker" == opt_val) { yapp_mode_code = YAPP_MODE_WORKER; }
          if (YAPP_MODE_INVALID != yapp_mode_code) {
            cfg_obj.yapp_mode_code = yapp_mode_code;
#ifdef DEBUG_YAPP_DAEMON_APP 
            std::cerr << "<<<< Set Mode: " << cfg_obj.yapp_mode_code << std::endl;
#endif
          }
          b_opt_str = true;
          break;
        }
        case YAPP_DAEMON_OPT_PID_FILE: {
          cfg_obj.yappd_pid_file = opt_val;
#ifdef DEBUG_YAPP_DAEMON_APP 
          std::cerr << "<<<< Set Pid File: " << cfg_obj.yappd_pid_file << std::endl;
#endif
          b_opt_str = true;
          break;
        }
        case YAPP_DAEMON_OPT_LOG_STDOUT: {
          cfg_obj.yappd_log_stdout = opt_val;
#ifdef DEBUG_YAPP_DAEMON_APP 
          std::cerr << "<<<< Set stdout to File: " << cfg_obj.yappd_log_stdout << std::endl;
#endif
          b_opt_str = true;
          break;
        }
        case YAPP_DAEMON_OPT_LOG_STDERR: {
          cfg_obj.yappd_log_stderr = opt_val;
#ifdef DEBUG_YAPP_DAEMON_APP 
          std::cerr << "<<<< Set stderr to File: " << cfg_obj.yappd_log_stderr << std::endl;
#endif
          b_opt_str = true;
          break;
        }

        case YAPP_DAEMON_OPT_VERBOSE:  {
          if (opt_val.empty()) { cfg_obj.b_verbose = true; }
#ifdef DEBUG_YAPP_DAEMON_APP 
          if (opt_val.empty()) {
            std::cerr << "<<<< Set Verbose." << std::endl;
          }
#endif
          b_opt_str = true;
          break;
        }
        default: break;
        } /** switch(c) **/
      } /** if (0 == arg_str.find(YAPP_DAEMON_OPT_ENTRY[c])) **/
    } /** for (int c = 0; c < mode_entry_cnt; c++) **/
    if (true == b_opt_str) { continue; }
    arg_arr.push_back(arg_str);
  }
#ifdef DEBUG_YAPP_DAEMON_APP 
  std::cerr << "Running mode: ";
  if (YAPP_MODE_INVALID == yapp_mode_code) {
    std::cerr << "UNSPECIFIED, USE DEFAULT." << std::endl;
  } else {
    std::cerr << yapp_mode_code << std::endl;
  }
  for (size_t i = 0; i < arg_arr.size(); i++) {
    std::cerr << "arg " << i << ": " << arg_arr[i] << std::endl;
  }
#endif
  return status;
}

bool YappDaemonApp::daemonize_proc(ConfigureUtil & c_obj) {
  bool status = false;
  if(1 == getppid()) { return status; }

  status = SystemIOUtil::setup_io_redirection(
    c_obj.yappd_log_stdout, c_obj.yappd_log_stderr
  );
  if (false == status) {
    std::cerr << "<<<< Failed In Setting the IO!"
              << strerror(errno) << std::endl;
    return false;
  }

  int lock_fd = -1;
  lock_fd = open(c_obj.yappd_pid_file.c_str(), O_RDWR|O_CREAT, 0640);

  if (lock_fd < 0) {
    std::cerr << "<<<< Failed In Openning the PID Lock File!" << std::endl
              << strerror(errno) << " : " << c_obj.yappd_pid_file << std::endl;
    return false;
  }

  pid_t yapp_pid = -1;
  yapp_pid = fork();

  if (yapp_pid < 0) {
    std::cerr << "<<<< Failed In Forking the Child!" << std::endl;
    return false;
  }

  if (yapp_pid > 0) { exit(EXIT_SUCCESS); }

  if (0 > lockf(lock_fd, F_TLOCK, 0)) {
    std::cerr << "<<<< Failed In Locking the PID File!"
              << strerror(errno) << std::endl;
    return false;
  }

  /** child proc. would continue; **/
  setsid();

  string pid_str = StringUtil::convert_int_to_str(getpid()) + "\n";
  if (0 > write(lock_fd, pid_str.c_str(), pid_str.size())) {
    std::cerr << "<<<< Failed In Writing to the PID File!"
              << strerror(errno) << std::endl;
    return false;
  }

  return true;
}

YAPP_MSG_CODE YappDaemonApp::run(int argc, char * argv[]) {
  YAPP_MSG_CODE status = YAPP_MSG_INVALID;

  /**
   * 1. This line will try to load the conf from the def. place, /etc/init.d/
   *    this is solely for the convenience of putting daemon to upstart script.
   */
  if (true != cfg_obj.load_zk_cluster_cfg(def_cfg_fp)) {
    std::cerr << ">>>> Failed In Loading the Config File." << std::endl;
    return status;
  }

  /**
   * 2. If the user would ever start with some params, those params should be
   *    able to overwrite the default params in the configuration file.
   */
  if (argc > 1) {
    if (YAPP_MSG_SUCCESS != parse_args(argc, argv)) {
      return status;
    }
  }

  /**
   * 3. daemonize the controling process, which will do nothing related to any
   *    actual yappd work but handle signals and fork a yappd child as needed.
   */
  if (false == daemonize_proc(cfg_obj)) { return status; }

  /**
   * 4. setup the signal handler, which will having a new, dedicated thread to
   *    handle signal SIGTERM(clean exit) and SIGHUP(reload conf.), also note
   *    that these 2 signals will remain blocked only on controling process,
   *    child process running the actual yappd and all other subtasks forked
   *    via execve (if yappd runs as a worker) will have default signal handler
   */
  if (true != init_shared_resource() || true != setup_signal_handler()) {
    std::cerr << "<<<< Failed In Setting Signal Handler & Res." << std::endl;
    return status;
  }

  /**
   * 5. fork a child process dedicated for running yappd instance.
   */
  pid_t yappd_child_pid = -1;

  bool b_fork_needed = true;
  while (true == b_fork_needed) {
    yappd_child_pid = fork();
    if (yappd_child_pid < 0) {
      std::cerr << "<<<< Failed In Forking the Yappd Instance!" << std::endl;
      break;
    }
    /**
     * Reset Signal Mask on Yappd Instance Which Unblocks SIGTERM & SIGHUP. Note
     * that here we do not reset the handler and only reset the signal mask due
     * to the fact that the parent process runs a dedicated thread as a signal
     * handler and we did not touch the default action via sigaction, also fork
     * requires that only the calling thread is duplicated in the child process,
     * which means that signal handling thread won't exist on current child.
     */
    else if (0 == yappd_child_pid) {
      sigemptyset(&def_sig_mask);
      if (0 != pthread_sigmask(SIG_SETMASK, &def_sig_mask, NULL)) {
        std::cerr << "<<<< Failed in Restore the Signal Mask." << std::endl;
        return status;
      } else {
#ifdef DEBUG_YAPP_DAEMON_APP 
        std::cerr << "<<<< Finished ReSet Signal Mask for Proc: " << getpid() << std::endl;
#endif
      }
      switch (cfg_obj.yapp_mode_code) {
        case YAPP_MODE_MASTER:{
             yapp_obj_ptr = new YappMaster(
                 arg_arr, cfg_obj, false, shrd_mem_id, shrd_mem_sz
             );
             break;
        }
        case YAPP_MODE_WORKER:{
             yapp_obj_ptr = new YappWorker(
                 arg_arr, cfg_obj, false, shrd_mem_id, shrd_mem_sz
             );
             break;
        }
        default:              {
             yapp_obj_ptr = new YappWorker(
                 arg_arr, cfg_obj, false, shrd_mem_id, shrd_mem_sz
             );
             break;
        }
      }
      return yapp_obj_ptr->run(); /* break out of while loop */
    }
    else {
      if (true == wait_for_child_proc_termination(yappd_child_pid)) {
        status = YAPP_MSG_SUCCESS;
      } else { break; }
    } /* if (0 == yappd_child_pid) */

    /**
     * upon the term. of child proc., 1st stop the signal handler, so that no
     * further action to reset the semaphore will be affected.
     */
    // if (0 != pthread_cancel(signal_hndl_thrd_id)) {
    //  std::cerr << ">>>> Failed to Terminate Signal Handler" << std::endl;
    // }
    set_bflag_atomic(&b_flag_thrd_ends, true);
    pthread_join(signal_hndl_thrd_id, NULL);
    set_bflag_atomic(&b_flag_thrd_ends, false);

    free_shared_mutex();

    if (true != init_shared_mutex() || true != setup_signal_handler()) {
      std::cerr << "<<<< Failed In Setting Signal Handler & Res." << std::endl;
      return status;
    }

    b_fork_needed = check_fork_needed();
    if (true == b_fork_needed) {
      if (true != cfg_obj.load_zk_cluster_cfg(def_cfg_fp)) {
        std::cerr << ">>>> Failed In Loading the Config File." << std::endl;
      }
    }
  } /* while (true == check_fork_needed()) */

  /** Only parent controlling process will free all shared resources **/
  free_shared_mutex();
  free_shared_memory();
  return status;
}

bool YappDaemonApp::check_fork_needed() {
  bool b_need_fork = true;
  sem_t * shrd_mutex_ptr = sem_open(IPC_CONTEX::YAPPD_SHARED_MUTEX_NAME.c_str(), 0);
  sem_wait(shrd_mutex_ptr);
  if (shrd_mem_ptr[0]) { b_need_fork = false;}
  sem_post(shrd_mutex_ptr);
  sem_close(shrd_mutex_ptr);
  return b_need_fork;
}

bool YappDaemonApp::wait_for_child_proc_termination(pid_t child_pid)
{
  int child_ret = 0, child_sig = 0;
  bool b_status = true;
  int status;
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "==>> Start waitpid for Yappd, ret: " << child_ret
            << " sig: " << child_sig << std::endl;
#endif
  do {
    int w = waitpid(child_pid, &status, WUNTRACED | WCONTINUED);
    if (w == -1) {
      std::cerr << "-- Error happend when waiting for Yappd child process: "
                << child_pid
                << " with parent process: "
                << getpid() << std::endl;
      b_status = false;
      break;
    }
    if (WIFEXITED(status)) {
      child_ret = WEXITSTATUS(status);
      if (true == b_verbose) {
        std::cout << "-- Yappd child process: " << child_pid
                  << " terminated with returned value of "
                  << child_ret << std::endl;
      } 
    } else if (WIFSIGNALED(status)) {
      if (true == b_verbose) {
        child_sig = WTERMSIG(status);
        std::cout << "-- Yappd child process: " << child_pid
                  << " got killed by signal "
                  <<  child_sig << std::endl;
      }
    } else if (WIFSTOPPED(status)) {
      if (true == b_verbose) {
        std::cout << "-- Yappd child process: " << child_pid
                  << " stopped by signal "
                  << WSTOPSIG(status) << std::endl;
      }
    } else if (WIFCONTINUED(status)) {
      if (true == b_verbose) {
        std::cout << "-- Yappd child process: " << child_pid
                  << " resumed by SIGCONT" << std::endl;
      }
    }
  } while (!WIFEXITED(status) && !WIFSIGNALED(status));
#ifdef DEBUG_YAPP_WORKER_SERVICE_HANDLER
  std::cerr << "==>> Finish waitpid for Yappd, ret: " << child_ret
            << " sig: " << child_sig << std::endl;
#endif
  return b_status;
}

bool YappDaemonApp::process_event_async(int signal_val) {
  if (SIGTERM != signal_val && SIGHUP != signal_val) {
    return false;
  }
  bool b_new_event = true;

  sem_t * shrd_mutex_ptr = sem_open(IPC_CONTEX::YAPPD_SHARED_MUTEX_NAME.c_str(), 0);
#ifdef DEBUG_YAPP_DAEMON_APP 
  std::cerr << ">>>> Wait On the Mutex: " << shrd_mutex_ptr << std::endl;
#endif
  sem_wait(shrd_mutex_ptr);
  switch (signal_val) {
    case SIGTERM: {
      if (SIGTERM == (int)shrd_mem_ptr[0]) { b_new_event = false; break; }
#ifdef DEBUG_YAPP_DAEMON_APP
      std::cerr << ">>>> Set the Flag To Exit" << std::endl;
#endif
      shrd_mem_ptr[0] = SIGTERM;
      break;
    }
    case SIGHUP:  {
      if (SIGTERM == (int)shrd_mem_ptr[0] ||
          SIGHUP  == (int)shrd_mem_ptr[0]) { b_new_event = false; break; }
      shrd_mem_ptr[0] = SIGHUP;
#ifdef DEBUG_YAPP_DAEMON_APP
      std::cerr << ">>>> Set the Flag To Reload" << std::endl;
#endif
      break;
    }
  }
  sem_post(shrd_mutex_ptr);
  sem_close(shrd_mutex_ptr);
#ifdef DEBUG_YAPP_DAEMON_APP
  std::cerr << ">>>> Release The Mutex" << std::endl;
#endif
  /** ONLY NOTIFY THE CHILD AFTER WE DONE THE UPDATE & RELEASING THE LOCK!! **/
  sem_t * shrd_condv_ptr = sem_open(IPC_CONTEX::YAPPD_SHARED_CONDV_NAME.c_str(), 0);
  if (true == b_new_event) {
#ifdef DEBUG_YAPP_DAEMON_APP
    std::cerr << ">>>> Start To Notify the Child" << std::endl;
#endif
    if (0 == sem_post(shrd_condv_ptr)) {
#ifdef DEBUG_YAPP_DAEMON_APP
      std::cerr << ">>>> Finish Notify the Child" << std::endl;
#endif
    }
  }
  sem_close(shrd_condv_ptr);

  return true;
}

bool YappDaemonApp::set_bflag_atomic(bool * flag_ptr, bool val) {
  if (NULL == flag_ptr) { return false; }
  pthread_mutex_lock(&yappd_app_mutex);
  * flag_ptr = val;
  pthread_mutex_unlock(&yappd_app_mutex);
  return true;
}

bool YappDaemonApp::chk_bflag_atomic(bool * flag_ptr) {
  bool b_val = false;
  pthread_mutex_lock(&yappd_app_mutex);
  b_val = * flag_ptr;
  pthread_mutex_unlock(&yappd_app_mutex);
  return b_val;
}

void * YappDaemonApp::thread_signal_handler(void * app_ptr) {
  int signo;
  YappDaemonApp * yappd_ptr = (YappDaemonApp *)app_ptr;
  if (NULL == yappd_ptr) {
    std::cerr << ">>>> Invalid Yappd pointer found!" << std::endl;
    return NULL;
  }
  struct timespec ts;
  while (!yappd_ptr->chk_bflag_atomic(&(yappd_ptr->b_flag_thrd_ends))) {
    //clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec = 1;
    ts.tv_nsec = 0;
    signo = sigtimedwait(&(yappd_ptr->ctl_sig_mask), NULL, &ts);
    if (0 > signo) {
#ifdef DEBUG_YAPP_DAEMON_APP 
      std::cerr << ">>>> Failed In Waiting Signal, Ret: " << errno << std::endl;
#endif
      continue;
    }
    switch (signo) {
      case SIGTERM:
      case SIGHUP:
        /* invoke the corresponding thread to handle signal. */
#ifdef DEBUG_YAPP_DAEMON_APP 
        std::cerr << ">>>> Received Signal: " << signo << std::endl;
#endif
        yappd_ptr->process_event_async(signo);
        break;
      default:      {
        std::cerr << ">>>> Unexpected Signal: " << signo << std::endl;
        break;
      }
    } /* switch (signo) */
  } // while (yappd_ptr->signal_hndl_ends())

  return NULL;
}

bool YappDaemonApp::setup_signal_handler() {
  /**
   * - block sighup signal for all running thread so that only the dedicated one
   *   will be handling for all of them to dynamically reload the configuration.
   */
  sigemptyset(&ctl_sig_mask);
  sigfillset(&ctl_sig_mask);
  if (0 != pthread_sigmask(SIG_BLOCK, &ctl_sig_mask, NULL)) {
    std::cerr << ">>>> Failed in Setting the Mask." << getpid() << std::endl;
    return false;
  } else {
#ifdef DEBUG_YAPP_DAEMON_APP 
    std::cerr << "<<<< Finished Set Signal Mask for Proc: " << getpid() << std::endl;
#endif
  }

  /* Create a thread to handle SIGHUP and SIGTERM. */
  if (0 != pthread_create(&signal_hndl_thrd_id, NULL,
                          thread_signal_handler, (void *) this))
  {
    std::cerr << ">>>> Failed In Setting Up Signal Handler: " << errno << std::endl;
    return false;
  } else {
#ifdef DEBUG_YAPP_DAEMON_APP
    std::cerr << "<<<< Finished Setting Up Signal Handler: " << signal_hndl_thrd_id << std::endl;
#endif
  }

  return true;
}

bool YappDaemonApp::init_shared_memory(void) {
  if (0 > (shrd_mem_id = shmget(IPC_PRIVATE, shrd_mem_sz, IPC_CREAT | 0660))){
    std::cerr << ">>>> Failed In Creating Shared Memory for Yappd." << std::endl;
    return false;
  }
  if ((void *) -1 == (shrd_mem_ptr = (char *) shmat(shrd_mem_id, 0, 0))) {
    std::cerr << ">>>> Failed In Attaching Shared Memory for Yappd." << std::endl;
    return false;
  } else {
#ifdef DEBUG_YAPP_DAEMON_APP 
    std::cerr << "<<<< Finished Init Shared Memory: " << shrd_mem_id << std::endl;
#endif
  }
  memset(shrd_mem_ptr, 0, shrd_mem_sz);
  return true;
}

bool YappDaemonApp::init_shared_mutex(void) {
  sem_t * shrd_mutex_ptr = sem_open(IPC_CONTEX::YAPPD_SHARED_MUTEX_NAME.c_str(), O_CREAT, 0666, 1);
  sem_t * shrd_condv_ptr = sem_open(IPC_CONTEX::YAPPD_SHARED_CONDV_NAME.c_str(), O_CREAT, 0666, 0);
  if (SEM_FAILED == shrd_condv_ptr || SEM_FAILED == shrd_mutex_ptr) {
    std::cerr << ">>>> Failed In Creating Semaphore for Yappd." << std::endl;
    return false;
  } else {
#ifdef DEBUG_YAPP_DAEMON_APP 
    std::cerr << "<<<< Finished Init Mutex: " << shrd_mutex_ptr
              << " And Cond. Var: " << shrd_condv_ptr << std::endl;
#endif
  }
  sem_close(shrd_condv_ptr);
  sem_close(shrd_mutex_ptr);
  return true;
}

bool YappDaemonApp::free_shared_memory(void) {
  bool status = false;
  if ((0 == shmdt(shrd_mem_ptr)) &&
      (0 == shmctl(shrd_mem_id, IPC_RMID, (struct shmid_ds *) 0))) {
    status = true;
#ifdef DEBUG_YAPP_DAEMON_APP 
    std::cerr << "<<<< Succeeded in Destroying Shared Memory." << std::endl;
#endif
  } else {
    std::cerr << "<<<< Failed in Destroying Shared Memory." << std::endl;
  }
  return status;
}

bool YappDaemonApp::free_shared_mutex(void) {
  if ((0 == sem_unlink(IPC_CONTEX::YAPPD_SHARED_MUTEX_NAME.c_str())) &&
      (0 == sem_unlink(IPC_CONTEX::YAPPD_SHARED_CONDV_NAME.c_str()))) {
#ifdef DEBUG_YAPP_DAEMON_APP 
    std::cerr << "<<<< Succeeded in Destroying Shared Mutex & Cond. Var." << std::endl;
#endif
  } else {
#ifdef DEBUG_YAPP_DAEMON_APP 
    std::cerr << "<<<< Failed in Destroying Shared Mutex & Cond. Var" << std::endl;
#endif
  }
  return true;
}

bool YappDaemonApp::init_shared_resource(void) {
  free_shared_mutex();
  return (init_shared_memory() && init_shared_mutex());
}

int main(int argc, char * argv[]) {
  IPC_CONTEX::init_ipc_contex();
  YappDaemonApp yapp_obj;
  return yapp_obj.run(argc, argv);
}
