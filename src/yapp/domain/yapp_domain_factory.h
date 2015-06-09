#ifndef YAPP_DOMAIN_FACTORY_H_
#define YAPP_DOMAIN_FACTORY_H_

#include <string>
#include <vector>
#include <ctime>
#include <map>

#include "yapp_service_util.h"
#include "../util/yapp_util.h"
#include "gen-cpp/yapp_service_types.h"
#include "gen-cpp/yapp_service_constants.h"

namespace yapp {
namespace domain {

using std::string;
using std::vector;
using std::map;

using namespace yapp::util;

extern const yapp_serviceConstants g_yapp_service_constants;

class YappDomainFactory {

public:
  static bool is_job_all_terminated(const string & job_hndl,
                                    ZkClusterProxy * zkc_proxy_ptr);

  static bool get_all_sub_proc_hndl(vector<string> & sub_proc_hndl_arr,
                                    vector<string> & job_proc_hndl_arr,
                                    vector<int> & sub_proc_inpt_typ,
                                    vector<string> & task_node_arr,
                                    const string & job_hndl,
                                    ZkClusterProxy * zkc_proxy_ptr);

  static Job create_job(const string & job_owner, long long created_tmstp_sec,
                        const vector<Task> & task_arr);

  static Task create_task(
    long long range_from,       long long range_to,     int proc_num,
    const string & app_env,     const string & app_bin, const string & app_src,
    const string & arg_str,     const string & out_pfx, const string & err_pfx,
    const bool & b_autosp,      const bool & b_automg,  const string & work_dir,
    const string & lsn_prfx,    const string & fpath = "",
    TASK_INPUT_TYPE::type input_type = TASK_INPUT_TYPE::ID_RANGE,
    int range_step = g_yapp_service_constants.TASK_DEFAULT_RANGE_STEP
  );

  /**
   * Desc:
   * - This method will generate a subtask(a separate excutable task for worker
   *   nodes), which effectively a task with a single definition of process in
   *   job.task_arr[task_idx].proc_arr[proc_idx].
   */
  static bool extract_subtask_from_job(
    Task & subtask, const Job & job, int task_idx, int proc_idx
  );

  static ProcessControlBlock create_pcb();

  /**
   * TODO:
   * - unit testing.
   */
  static bool get_subtasks_by_proc_hnds(vector<Task> & subtask_ret,
                                        const vector<string> & proc_hnds,
                                        ZkClusterProxy * zkc_proxy_ptr,
                                        bool b_upd_tskhnd = true);

  static bool get_subtask_by_proc_hnd(Task & task_ret, const string & proc_hnd,
                                      ZkClusterProxy * zkc_proxy_ptr,
                                      bool b_upd_tskhnd = true);
};

}
}

#endif
