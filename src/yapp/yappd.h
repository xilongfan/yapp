/**
 * Desc:
 * - This file contains the centralized main interface for yapp server.
 *
 * Dependencies:
 * - ./util
 * - ./domain
 * - ./worker
 * - ./master
 *
 * Last Updated By: 06-AUG-2013, Xilong Fan
 */

#ifndef YAPPD_H_
#define YAPPD_H_

#include <iostream>
#include <vector>
#include <string>
#include <algorithm>

#include <semaphore.h>

#include <zookeeper/zookeeper.h>

#include "./util/yapp_util.h"
#include "./base/yapp_base.h"

namespace yapp {

using std::string;
using std::vector;
using namespace yapp::base;
using namespace yapp::util;

enum YAPP_DAEMON_OPT_ENTRY_MAP {
  YAPP_DAEMON_OPT_MODE,
  YAPP_DAEMON_OPT_PID_FILE,
  YAPP_DAEMON_OPT_LOG_STDOUT,
  YAPP_DAEMON_OPT_LOG_STDERR,
  YAPP_DAEMON_OPT_VERBOSE,
  YAPP_DAEMON_OPT_COUNT,
};

const static char * const YAPP_DAEMON_OPT_ENTRY[] = {
  "--mode=", "--pid-file=", "--log-stdout", "--log-stderr", "--verbose",
};

class YappDaemonApp : public YappApp {
public:
  YappDaemonApp() : YappApp() {
    shrd_mem_ptr = NULL;
    shrd_mem_id  = -1;
    shrd_mem_sz  = sysconf(_SC_PAGESIZE);
    b_flag_thrd_ends = false;
  }
  virtual ~YappDaemonApp() {
  }
  virtual YAPP_MSG_CODE run(int argc, char * argv[]);
  virtual YAPP_MSG_CODE parse_args(int argc, char * argv[]);
protected:

  bool set_bflag_atomic(bool * flag_ptr, bool val);
  bool chk_bflag_atomic(bool * flag_ptr);

  /**
   * Desc:
   * - This method will initialize all resources (shared memory to set flags and
   *   semaphores for synchronization) shared between parent process(responsible
   *   for control & signal handling) and child process which acutally doing the
   *   work(either runs as a master or worker).
   */
  bool init_shared_resource(void);

  bool init_shared_memory(void);
  bool init_shared_mutex(void);

  bool free_shared_memory(void);
  bool free_shared_mutex(void);

  static void * thread_signal_handler(void * app_ptr);
  bool setup_signal_handler();
  bool wait_for_child_proc_termination(pid_t child_pid);
  bool check_fork_needed();
  bool process_event_async(int signal_val);

  bool daemonize_proc(ConfigureUtil & c_obj);
  ConfigureUtil cfg_obj;
private:
  pthread_t signal_hndl_thrd_id;
  sigset_t def_sig_mask;
  sigset_t ctl_sig_mask;
  char * shrd_mem_ptr;
  int    shrd_mem_id;
  long   shrd_mem_sz;
  bool   b_flag_thrd_ends;
};

}
#endif
