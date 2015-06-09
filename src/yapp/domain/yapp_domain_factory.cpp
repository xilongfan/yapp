#include <ctime>
#include "yapp_domain_factory.h"

using namespace yapp::domain;

bool YappDomainFactory::is_job_all_terminated(const string & job_hndl,
                                              ZkClusterProxy * zkc_proxy_ptr) {
  bool status = false;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  vector<string> job_proc_hndls;
  vector<string> sub_proc_hndls;
  vector<int>    sub_proc_types;
  vector<string> task_node_arr;
  status = YappDomainFactory::get_all_sub_proc_hndl(
    sub_proc_hndls, job_proc_hndls, sub_proc_types,
    task_node_arr, job_hndl, zkc_proxy_ptr
  );
#ifdef DEBUG_YAPP_DOMAIN_FACTORY
  std::cerr << ">>>> GETTING ALL SUB PROC HNDL: " << status << std::endl;
  for (size_t i = 0; i < sub_proc_hndls.size(); i++) {
    std::cerr << ">>>> " << sub_proc_hndls[i] << std::endl;
  }
  std::cerr << ">>>> GETTING ALL JOB PROC HNDL: " << status << std::endl;
  for (size_t i = 0; i < job_proc_hndls.size(); i++) {
    std::cerr << ">>>> " << job_proc_hndls[i] << std::endl;
  }
#endif
  if ((NULL == zkc_proxy_ptr) || (false == status) ||
      (sub_proc_hndls.size() != sub_proc_types.size()) ||
      (job_proc_hndls.size() != sub_proc_types.size())) {
    return status;
  }
  string term_flag = StringUtil::convert_int_to_str(
    PROC_STATUS_CODE::PROC_STATUS_TERMINATED
  );
  string term_stat;

  string tmp_hndl;
  int proc_hndl_cnt = sub_proc_hndls.size();
  if (true == status && 0 < proc_hndl_cnt) {
    switch ((int)sub_proc_types[0]) {
    /** 1. if job's input is file related, compare its running/failed queue. **/
      case ((int)TASK_INPUT_TYPE::DYNAMIC_RANGE):
      case ((int)TASK_INPUT_TYPE::RANGE_FILE): {
        int task_cnt = task_node_arr.size();
        vector<string> runing_proc_per_tsk;
        vector<string> failed_proc_per_tsk;
        string task_path;
        for (int i = 0; i < task_cnt; i++) {
          task_path = zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
                      job_hndl + "_" + task_node_arr[i];
  #ifdef DEBUG_YAPP_DOMAIN_FACTORY
          std::cerr << ">>>> CHECK TASK-SET: " << task_path << std::endl;
  #endif
          rc = zkc_proxy_ptr->get_node_arr(
            task_path + "/running_procs", runing_proc_per_tsk
          );
          if (YAPP_MSG_SUCCESS != rc) { status = false; break; }
  #ifdef DEBUG_YAPP_DOMAIN_FACTORY
          std::cerr << ">>>> NO RUNNING PROCS FOUND!" << std::endl;
  #endif
          if (0 == runing_proc_per_tsk.size()) { continue; }
          rc = zkc_proxy_ptr->get_node_arr(
            task_path + "/failed_procs", failed_proc_per_tsk
          );
          if (YAPP_MSG_SUCCESS != rc) { status = false; break; }
  #ifdef DEBUG_YAPP_DOMAIN_FACTORY
          std::cerr << ">>>> FAILED PROCS CNT: " << failed_proc_per_tsk.size() << std::endl;
          std::cerr << ">>>> RUNING PROCS CNT: " << runing_proc_per_tsk.size() << std::endl;
  #endif
          if (runing_proc_per_tsk.size() != failed_proc_per_tsk.size()) {
            status = false; break;
          }
        }
        break;
      } /* case ((int)TASK_INPUT_TYPE::RANGE_FILE): */
      case ((int)TASK_INPUT_TYPE::ID_RANGE): {
      /** 2. if job's input is id range, check every process's status. **/
        for (int i = 0; i < proc_hndl_cnt; i++) {
          rc = zkc_proxy_ptr->get_node_data(
            zkc_proxy_ptr->get_proc_full_path(sub_proc_hndls[i]) + "/cur_status",
            term_stat
          );
          if (YAPP_MSG_SUCCESS != rc) { status = false; break; }
  #ifdef DEBUG_YAPP_DOMAIN_FACTORY
          std::cerr << ">>>> TERM_FLAG: " << term_flag << " TERM_STAT: " << term_stat << std::endl;
  #endif
          if (term_stat != term_flag) { status = false; break; }
        } /* for (int i = 0; i < proc_hndl_cnt; i++) */
      } /* case ((int)TASK_INPUT_TYPE::ID_RANGE): */
    } /* switch ((int)sub_proc_types[i]) */
  } /* if (true == status && 0 < proc_hndl_cnt) */
  return status;
}

bool YappDomainFactory::get_all_sub_proc_hndl(vector<string> & sub_proc_hndls,
                                              vector<string>& job_proc_hndl_arr,
                                              vector<int> & sub_proc_inpt_typ,
                                              vector<string> & task_node_arr,
                                              const string & job_hndl,
                                              ZkClusterProxy * zkc_proxy_ptr) {
  bool status = false;
  if (NULL == zkc_proxy_ptr) { return status; }
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  string job_full_path = zkc_proxy_ptr->get_job_full_path(job_hndl);

  rc = zkc_proxy_ptr->get_node_arr(job_full_path + "/task_arr", task_node_arr);

  if (YAPP_MSG_SUCCESS != rc) { return status; }
  int task_cnt = task_node_arr.size();
  int incr_cnt = 0, cur_input_type = 0;
  for (int i = 0; i < task_cnt; i++) {

    string task_full_path = job_full_path + "/task_arr/" + task_node_arr[i];
    string tmp_proc_hndl;
    string task_type;
    vector<string> proc_arr;
    bool b_fin_get_hnd = true;

    rc = zkc_proxy_ptr->get_node_data(task_full_path + "/input_type",task_type);
    if (YAPP_MSG_SUCCESS != rc) { break; }
    cur_input_type = atoi(task_type.c_str());
    rc = zkc_proxy_ptr->get_node_arr(task_full_path + "/proc_arr", proc_arr);
    if (YAPP_MSG_SUCCESS != rc) { break; }
    incr_cnt = proc_arr.size();
    for (int c = 0; c < incr_cnt; c++) {
      rc = zkc_proxy_ptr->get_node_data(
        task_full_path + "/proc_arr/" + proc_arr[c] + "/proc_hnd", tmp_proc_hndl
      );
      if (YAPP_MSG_SUCCESS != rc) { b_fin_get_hnd = false; break; }
      job_proc_hndl_arr.push_back(
        job_hndl + "_" + task_node_arr[i] + "_" + proc_arr[c]
      );
      sub_proc_inpt_typ.push_back(cur_input_type);
      sub_proc_hndls.push_back(tmp_proc_hndl);
    }
    if (true != b_fin_get_hnd) { break; }
    if (task_cnt - 1  == i) { status = true; }
  }
  return status;
}

bool YappDomainFactory::get_subtask_by_proc_hnd(Task & task_obj,
                                                const string & proc_hnd_str,
                                                ZkClusterProxy * zkc_proxy_ptr,
                                                bool b_upd_tskhnd)
{
  bool status = true;
  string proc_hnd = proc_hnd_str;
  string cur_line_idx;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  /**
   * Basically there are 2 kinds of subtask_process_handle will appear in queue,
   * - the normal one for every range job: job-00001_task-00001_proc-00001
   * - the semi-structed one for file range job:
   *     ${phnd_0}_max_111111_curhnd_${phnd_0}_curln_0_nxtln_2_proccnt_2
   */
  if (string::npos != proc_hnd_str.find(NXT_LINE_STR)) {
    string tskhd_helped, max_line_idx, nxt_line_idx, tot_proc_cnt;

    bool ret = RangeFileTaskDataParser::parse_rfile_task_data_str(
      proc_hnd_str, proc_hnd,     max_line_idx, tskhd_helped,
                    cur_line_idx, nxt_line_idx, tot_proc_cnt
    );
    if (false == ret) { proc_hnd = ""; }
  }

  if (NULL == zkc_proxy_ptr || proc_hnd.empty()) { return status; }

  string proc_path = zkc_proxy_ptr->get_proc_full_path(proc_hnd);
  string task_path = proc_path.substr(0, proc_path.rfind("/proc_arr/"));
  vector<string> columns;
  string value;
  rc = zkc_proxy_ptr->get_node_arr(task_path, columns);
  if (YAPP_MSG_SUCCESS != rc) { return status; }

  size_t pos = proc_hnd.find(TASK_HNDL_DELM);
  if (string::npos == pos) { return status; }
  string job_path = zkc_proxy_ptr->get_job_full_path(proc_hnd.substr(0, pos));

  map<string, string> task_map;
  int col_cnt = columns.size();
  for (int c = 0; c < col_cnt; c++) {
    rc = zkc_proxy_ptr->get_node_data(task_path + "/" + columns[c], value);
    if (YAPP_MSG_SUCCESS != rc) { break; }
    task_map[columns[c]] = value;
  }
  if (YAPP_MSG_SUCCESS != rc) { return status; }
  rc = zkc_proxy_ptr->get_node_data(job_path + "/owner", value);
  if (YAPP_MSG_SUCCESS != rc) { return status; }
  task_map["task_owner"] = value;

  task_obj.app_env = task_map["app_env"];
  task_obj.app_bin = task_map["app_bin"];
  task_obj.app_src = task_map["app_src"];
  task_obj.arg_str = task_map["arg_str"];
  task_obj.out_pfx = task_map["out_pfx"];
  task_obj.err_pfx = task_map["err_pfx"];
  task_obj.input_file  = task_map["input_file"];
  task_obj.range_from  = atoll(task_map["range_from"].c_str());
  task_obj.range_to    = atoll(task_map["range_to"].c_str());
  task_obj.range_step  = atoll(task_map["range_step"].c_str());
  task_obj.proc_cn     = atoi(task_map["proc_cn"].c_str());
  task_obj.input_type  = (TASK_INPUT_TYPE::type)atoi(task_map["input_type"].c_str());
  task_obj.task_att    = (TASK_ATTR_MASK::type)atoi(task_map["task_att"].c_str());
  task_obj.task_hnd    = task_map["task_hnd"];
  task_obj.working_dir = task_map["working_dir"];
  task_obj.anchor_prfx = task_map["anchor_prfx"];

  task_obj.task_owner  = task_map["task_owner"];

  task_map.clear();
  columns.clear();
  rc = zkc_proxy_ptr->get_node_arr(proc_path, columns);
  if (YAPP_MSG_SUCCESS != rc) { return status; }
  col_cnt = columns.size();
  for (int c = 0; c < col_cnt; c++) {
    rc = zkc_proxy_ptr->get_node_data(proc_path + "/" + columns[c], value);
    if (YAPP_MSG_SUCCESS != rc) { break; }
    task_map[columns[c]] = value;
  }
  if (YAPP_MSG_SUCCESS != rc) { return status; }

  ProcessControlBlock pcb;

  /** very important here as the pcb always range from 0 -> # of exec - 1 **/
  if ((int)TASK_INPUT_TYPE::DYNAMIC_RANGE == task_obj.input_type) {
    long long chunk_count =
      ((task_obj.range_to - task_obj.range_from + 1) / task_obj.range_step);
    if ((chunk_count * task_obj.range_step) <
        (task_obj.range_to - task_obj.range_from + 1)) { chunk_count += 1; }
    pcb.range_from = 0;
    pcb.range_to   = chunk_count - 1;
  } else {
    pcb.range_from = atoll(task_map["range_from"].c_str());
    pcb.range_to   = atoll(task_map["range_to"].c_str());
  }
  pcb.cur_status = (PROC_STATUS_CODE::type)atoi(task_map["cur_status"].c_str());
  pcb.return_val = atoi(task_map["return_val"].c_str());
  pcb.priority   = (PROC_PRORITY_CODE::type)atoi(task_map["priority"].c_str());
  pcb.host_pid   = atoi(task_map["host_pid"].c_str());
  pcb.std_out    = task_map["std_out"]; 
  pcb.std_err    = task_map["std_err"]; 
  pcb.host_str   = task_map["host_str"]; 
  // pcb.cur_anchor = atoll(task_map["cur_anchor"].c_str());
  pcb.cur_anchor = task_map["cur_anchor"].c_str();
  pcb.start_flag = atoll(task_map["start_flag"].c_str());

  if (true == b_upd_tskhnd) {
    pcb.proc_hnd = proc_hnd_str;
  } else {
    pcb.proc_hnd = task_map["proc_hnd"];
  }
  pcb.mem_req_in_bytes       = atoll(task_map["mem_req_in_bytes"].c_str());
  pcb.disk_req_in_bytes      = atoll(task_map["disk_req_in_bytes"].c_str());
  pcb.mem_used_in_bytes      = atoll(task_map["mem_used_in_bytes"].c_str());
  pcb.disk_used_in_bytes     = atoll(task_map["disk_used_in_bytes"].c_str());
  pcb.terminated_signal      = atoi(task_map["terminated_signal"].c_str());
  pcb.created_tmstp_sec      = atoll(task_map["created_tmstp_sec"].c_str());
  pcb.last_updated_tmstp_sec = atoll(task_map["last_updated_tmstp_sec"].c_str());

  task_obj.proc_arr.push_back(pcb);

  status = true;

  return status;
}

bool YappDomainFactory::get_subtasks_by_proc_hnds(vector<Task> & subtask_ret,
                                                  const vector<string> & proc_hnds,
                                                  ZkClusterProxy * zkc_proxy_ptr,
                                                  bool b_upd_tskhnd)
{
  bool status = true;
  int proc_cnt = proc_hnds.size();
  if (NULL == zkc_proxy_ptr || proc_hnds.size() < 1) { return status; }
  for (int i = 0; i < proc_cnt; i++) {
    Task task_obj;
    status = get_subtask_by_proc_hnd(
      task_obj, proc_hnds[i], zkc_proxy_ptr, b_upd_tskhnd
    );
    if (false == status) { break; }
    subtask_ret.push_back(task_obj);
  }
  return status;
}

Job YappDomainFactory::create_job(const string & job_owner,
                                  long long created_tmstp_sec,
                                  const vector<Task> & task_arr)
{
  Job job_obj;
  job_obj.owner             = job_owner;
  job_obj.created_tmstp_sec = created_tmstp_sec;
  job_obj.task_arr          = task_arr;
  return job_obj;
}

/**
 * struct Task {
 *   1: string          app_env = "",
 *   2: string          app_bin = "",
 *   3: string          app_src = "",
 *   4: string          arg_str = "",
 *   5: string          out_pfx = "",
 *   6: string          err_pfx = "",
 *   7: i32             proc_cn = 0,
 *   8: TASK_INPUT_TYPE input_type = TASK_DEFAULT_INPUT_TYPE,
 *   9: string          input_file = "",
 *  10: i64             range_from = TASK_DEFAULT_INPUT_RANG,
 *  11: i64             range_to = TASK_DEFAULT_INPUT_RANG,
 *  12: TASK_ATTR_MASK  task_att = TASK_DEFAULT_ATTRIBUTES,
 *  13: string          task_hnd = "",
 *  14: list<ProcessControlBlock> task_spc, 
 * }
 */
Task YappDomainFactory::create_task(
  long long range_from,   long long range_to,     int proc_num,
  const string & app_env, const string & app_bin, const string & app_src,
  const string & arg_str, const string & out_pfx, const string & err_pfx,
  const bool & b_autosp,  const bool & b_automg,  const string & working_dir,
  const string & lsn_pfx, const string& fpath, TASK_INPUT_TYPE::type input_type,
  int range_step)
{
  Task task;

  task.app_env     = app_env;
  task.app_bin     = app_bin;
  task.app_src     = app_src;
  task.arg_str     = arg_str;

  task.proc_cn     = proc_num;
  task.out_pfx     = out_pfx;
  task.err_pfx     = err_pfx;

  task.range_from  = range_from;
  task.range_to    = range_to;
  task.range_step  = range_step;
  task.input_type  = input_type;
  task.input_file  = fpath;

  task.working_dir = working_dir;
  task.anchor_prfx = lsn_pfx;

  if (true == b_autosp) {
    task.task_att = (TASK_ATTR_MASK::type)(task.task_att | TASK_ATTR_MASK::AUTO_SPLIT);
  }
  if (true == b_automg) {
    task.task_att = (TASK_ATTR_MASK::type)(task.task_att | TASK_ATTR_MASK::AUTO_MIGRATE);
  }
  return task;
}

bool YappDomainFactory::extract_subtask_from_job(
  Task & subtask, const Job & job, int task_idx, int proc_idx)
{
  bool status = false;
  if ((task_idx >= 0 && task_idx < (int)job.task_arr.size()) &&
      (proc_idx >= 0 && proc_idx < (int)job.task_arr[task_idx].proc_arr.size()))
  {
    subtask.app_env = job.task_arr[task_idx].app_env;
    subtask.app_bin = job.task_arr[task_idx].app_bin;
    subtask.app_src = job.task_arr[task_idx].app_src;
    subtask.arg_str = job.task_arr[task_idx].arg_str;
    subtask.input_type = job.task_arr[task_idx].input_type;
    subtask.input_file = job.task_arr[task_idx].input_file;
    subtask.task_att = job.task_arr[task_idx].task_att;
    subtask.task_hnd = job.task_arr[task_idx].task_hnd;

    subtask.working_dir = job.task_arr[task_idx].working_dir;
    subtask.anchor_prfx = job.task_arr[task_idx].anchor_prfx;

    subtask.range_from  = job.task_arr[task_idx].range_from;
    subtask.range_to    = job.task_arr[task_idx].range_to;
    subtask.range_step  = job.task_arr[task_idx].range_step;

    subtask.proc_arr.clear();
    subtask.proc_arr.push_back(job.task_arr[task_idx].proc_arr[proc_idx]);

    subtask.task_owner = job.owner;
    status = true;
  }
  return status;
}

ProcessControlBlock YappDomainFactory::create_pcb() {
  ProcessControlBlock pcb;
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  pcb.created_tmstp_sec = ts.tv_sec;
  return pcb;
}

/**
 * Desc:
 * - A ProcessControlBlock may resembles like:
 *   ProcessControlBlock pcb {
 *     1: range_from             : 0
 *     2: range_to               : 39
 *     3: std_out                : "${log_dir}/update_ra.stdout.job-001_task-001_proc-001"
 *     4: std_err                : "${log_dir}/update_ra.stderr.job-001_task-001_proc-001"
 *     5: created_tmstp_sec      : 2395723985u83945
 *     6: last_updated_tmstp_sec : 2395723985u83945
 *     7: cur_status             : PROC_STATUS_CODE::PROC_STATUS_NEW
 *     8: return_val             : PROC_CTRL_BLCK_DEFAULT_RETURN_VAL 
 *     9: terminated_signal      : PROC_CTRL_BLCK_DEFAULT_TERMIN_SIG 
 *    10: priority               : PROC_PRORITY_CODE::PROC_PRORITY_NORMAL
 *    11: host_str               : "la-script111"
 *    12: host_pid               : 8888888
 *    13: proc_hnd               : "job-001_task-001_proc-001"
 *    14: mem_req_in_bytes       : 43895743
 *    15: disk_req_in_bytes      : 438957434
 *    16: mem_used_in_bytes      : 4389574
 *    17: disk_used_in_bytes     : 43895743
 *    18: cur_anchor             : PROC_CTRL_BLCK_DEFAULT_ANCHOR_POS,
 *   }
 */
