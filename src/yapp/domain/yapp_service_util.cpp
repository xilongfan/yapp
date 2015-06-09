#include "yapp_service_util.h"

using namespace yapp::domain;

bool JobDataStrParser::parse_proc_host_str(
  const string & host_str, string & host_ip,
        string & host_prt, string & yapp_pid)
{
  bool status = false; 
  size_t node_end_pos = 0, host_end_pos = 0, port_end_pos = 0;

  node_end_pos = host_str.find(PROC_HOST_STR_DELM);
  if (string::npos == node_end_pos) { return status; }

  host_end_pos = host_str.find(PROC_HOST_STR_DELM, node_end_pos + 1);
  if (string::npos == host_end_pos) { return status; }
  host_ip  = host_str.substr(node_end_pos + 1, host_end_pos - node_end_pos - 1);

  port_end_pos = host_str.find(PROC_HOST_STR_DELM, host_end_pos + 1);
  if (string::npos == port_end_pos) { return status; }
  host_prt = host_str.substr(host_end_pos + 1, port_end_pos - host_end_pos - 1);

  if (host_str.size() <= port_end_pos) { return status; }
  yapp_pid = host_str.substr(port_end_pos + 1);

  if (false == host_ip.empty()  && false == host_prt.empty() &&
      false == yapp_pid.empty() && 0 != node_end_pos) {
    status = true;
  } else { status = false; }

  return status;
}

bool RangeFileTaskDataParser::get_running_proc_hnd_in_job_node(
  const string & proc_hnd_in_queue, string & proc_hnd_in_job_node)
{
  bool status = true;
  if (false == is_range_file_task_hnd(proc_hnd_in_queue)) {
    proc_hnd_in_job_node = proc_hnd_in_queue;
    return status;
  }
  string tskhd_in_job, max_line_idx, nxt_line_idx, tot_proc_cnt, cur_line_idx;

  /** ${phnd_0}_max_111111_curhnd_${phnd_0}_curln_0_nxtln_2_proccnt_2 **/
  status = RangeFileTaskDataParser::parse_rfile_task_data_str(
    proc_hnd_in_queue, tskhd_in_job, max_line_idx, proc_hnd_in_job_node,
                       cur_line_idx, nxt_line_idx, tot_proc_cnt
  );
  if (false == status) { proc_hnd_in_job_node = ""; }
  return status;
}


bool RangeFileTaskDataParser::get_helper_proc_hnd_in_job_node(
  const string & proc_hnd_in_queue, string & helper_proc_hnd)
{
  bool status = true;
  if (false == is_range_file_task_hnd(proc_hnd_in_queue)) {
    helper_proc_hnd = proc_hnd_in_queue;
    return status;
  }
  string tskhd_in_run, max_line_idx, nxt_line_idx, tot_proc_cnt, cur_line_idx;

  /** ${phnd_0}_max_111111_curhnd_${phnd_0}_curln_0_nxtln_2_proccnt_2 **/
  status = RangeFileTaskDataParser::parse_rfile_task_data_str(
    proc_hnd_in_queue, helper_proc_hnd, max_line_idx, tskhd_in_run,
                       cur_line_idx, nxt_line_idx, tot_proc_cnt
  );
  if (false == status) { helper_proc_hnd = ""; }
  return status;
}

int TaskHandleUtil::get_task_handle_type(const string & tsk_hndl_str)
{
  string hndl_str = tsk_hndl_str;
  StringUtil::trim_string(hndl_str);
  int hndl_str_type = TASK_HANDLE_TYPE_INVALID;
  size_t jobs_delm_pos = hndl_str.find(JOBS_HNDL_DELM);
  size_t task_delm_pos = hndl_str.find(TASK_HNDL_DELM);
  size_t proc_delm_pos = hndl_str.find(PROC_HNDL_DELM);
  size_t tmp_len = 0;
  bool b_jobs_hndl_int = false;
  bool b_task_hndl_int = false;
  bool b_proc_hndl_int = false;
  if (0 == jobs_delm_pos) {
    tmp_len = (string::npos == task_delm_pos) ?
              (string::npos) : (task_delm_pos - JOBS_HNDL_DELM.size());
    b_jobs_hndl_int = StringUtil::is_integer(
      hndl_str.substr(JOBS_HNDL_DELM.size(), tmp_len)
    );
#ifdef DEBUG_YAPP_SERVICE_UTIL
    std::cerr << ">> " << hndl_str.substr(JOBS_HNDL_DELM.size(), tmp_len)
              << std::endl;
#endif
    if (string::npos != task_delm_pos) {
      if (task_delm_pos > jobs_delm_pos + JOBS_HNDL_DELM.size()) {
        tmp_len = (string::npos == proc_delm_pos) ? (string::npos) :
                  (proc_delm_pos - task_delm_pos - TASK_HNDL_DELM.size());
        b_task_hndl_int = StringUtil::is_integer(
          hndl_str.substr(task_delm_pos + TASK_HNDL_DELM.size(), tmp_len)
        );
#ifdef DEBUG_YAPP_SERVICE_UTIL
        std::cerr << ">> " << hndl_str.substr(
                                task_delm_pos + TASK_HNDL_DELM.size(), tmp_len
                              )
                  << std::endl;
#endif
        if (string::npos != proc_delm_pos) {
          if (proc_delm_pos > task_delm_pos + TASK_HNDL_DELM.size()) {
            b_proc_hndl_int = StringUtil::is_integer(
              hndl_str.substr(proc_delm_pos + PROC_HNDL_DELM.size())
            );
#ifdef DEBUG_YAPP_SERVICE_UTIL
            std::cerr << ">> "
                      << hndl_str.substr(proc_delm_pos + PROC_HNDL_DELM.size())
                      << std::endl
                      << b_jobs_hndl_int << b_task_hndl_int << b_proc_hndl_int
                      << std::endl;
#endif
            if (b_jobs_hndl_int && b_task_hndl_int && b_proc_hndl_int &&
                task_delm_pos > jobs_delm_pos && proc_delm_pos > task_delm_pos){
              hndl_str_type = TASK_HANDLE_TYPE_PROC;
            }
          }
        } else { /* if (string::npos != proc_delm_pos) */
          if (b_jobs_hndl_int && b_task_hndl_int &&task_delm_pos>jobs_delm_pos){
            hndl_str_type = TASK_HANDLE_TYPE_TASK;
          }
        }
      }
    } else { /* if (string::npos != task_delm_pos) */
      if (b_jobs_hndl_int) {
        hndl_str_type = TASK_HANDLE_TYPE_JOBS;
      }
    }
  }
  return hndl_str_type;
}

bool RangeFileTaskDataParser::parse_rfile_task_data_str(
  const string & task_str, string & tskhd_in_job, string & max_line_idx,
                           string & tskhd_in_run, string & cur_line_idx,
                           string & nxt_line_idx, string & tot_proc_cnt)
{
  
  bool status = false;
  size_t max_lin_pos = task_str.find(MAX_LINE_STR);
  size_t cur_hnd_pos = task_str.find(CUR_HNDL_STR);
  size_t cur_lin_pos = task_str.find(CUR_LINE_STR);
  size_t nxt_lin_pos = task_str.find(NXT_LINE_STR);
  size_t tot_pro_pos = task_str.find(TOT_PROC_STR);

  if (string::npos == max_lin_pos || string::npos == cur_hnd_pos ||
      string::npos == cur_lin_pos || string::npos == nxt_lin_pos ||
      string::npos == tot_pro_pos ||
      !((max_lin_pos < cur_hnd_pos - 1) && (cur_hnd_pos < cur_lin_pos - 1) &&
        (cur_lin_pos < nxt_lin_pos - 1) && (nxt_lin_pos < tot_pro_pos - 1))
  ) { return status; }

  tskhd_in_job = task_str.substr(0, max_lin_pos);
  if (tskhd_in_job.empty()) { return status; }

  int start_pos = max_lin_pos + MAX_LINE_STR.size();
  int end_pos   = cur_hnd_pos - 1;
  max_line_idx  = task_str.substr(start_pos, end_pos - start_pos + 1);

  start_pos     = cur_hnd_pos + CUR_HNDL_STR.size();
  end_pos       = cur_lin_pos - 1;
  tskhd_in_run  = task_str.substr(start_pos, end_pos - start_pos + 1);

  start_pos     = cur_lin_pos + CUR_LINE_STR.size();
  end_pos       = nxt_lin_pos - 1;
  cur_line_idx  = task_str.substr(start_pos, end_pos - start_pos + 1);

  start_pos     = nxt_lin_pos + NXT_LINE_STR.size();
  end_pos       = tot_pro_pos - 1;
  nxt_line_idx  = task_str.substr(start_pos, end_pos - start_pos + 1);

  start_pos     = tot_pro_pos + TOT_PROC_STR.size();
  tot_proc_cnt  = task_str.substr(start_pos);

  status = true;
  return status;
}

void YappSubtaskQueue::set_queue_path(const string & q_path) {
  queue_path = q_path;
}

YappSubtaskQueue::YappSubtaskQueue() {
  task_queued_cnt = 0;
  pthread_mutex_init(&yapp_task_queue_mutex, NULL);
}

YappSubtaskQueue::YappSubtaskQueue(const string & qpath) {
  queue_path = qpath;
  task_queued_cnt = 0;
  pthread_mutex_init(&yapp_task_queue_mutex, NULL);
}

YappSubtaskQueue::~YappSubtaskQueue() {
  pthread_mutex_destroy(&yapp_task_queue_mutex);
}


int YappSubtaskQueue::acquire_task_queue_lock() {
  return pthread_mutex_lock(&yapp_task_queue_mutex);
}

int YappSubtaskQueue::release_task_queue_lock() {
  return pthread_mutex_unlock(&yapp_task_queue_mutex);
}

YAPP_MSG_CODE YappSubtaskQueue::get_task_queue(
  vector<string> & task_queue_ret, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }
  rc = zkc_proxy_ptr->get_node_arr(queue_path, task_queue_ret);
  if (YAPP_MSG_SUCCESS == rc) {
    acquire_task_queue_lock();
    task_queued_cnt = task_queue_ret.size();
    release_task_queue_lock();
  }
  return rc;
}

YAPP_MSG_CODE YappSubtaskQueue::push_back_task_queue(
  const vector<string> & subtask_hnd_arr, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }

  vector<string> node_arr;
  vector<string> data_arr;
  for (size_t i = 0; i < subtask_hnd_arr.size(); i++) {
    node_arr.push_back(queue_path + "/" + subtask_hnd_arr[i]);
    data_arr.push_back("");
  }

  rc = zkc_proxy_ptr->batch_create(node_arr, data_arr);

  if (YAPP_MSG_SUCCESS == rc) {
    increment_queue_cnt(node_arr.size());
  }

  return rc;
}

YAPP_MSG_CODE YappSubtaskQueue::push_back_task_queue(
  const string & subtask_hnd, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }

  string path_str = queue_path + "/" + subtask_hnd;
  string path_ret;

  rc = zkc_proxy_ptr->create_node(path_str, "", path_ret);

  if (YAPP_MSG_SUCCESS != rc) {
    zkc_proxy_ptr->del_node(path_str);
  } else {
    increment_queue_cnt();
  }

  return rc;
}

YAPP_MSG_CODE YappSubtaskQueue::push_back_and_lock_task(
  const string & subtask_hnd, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }
  rc = zkc_proxy_ptr->create_and_acquire_ex_lock(
    queue_path + "/" + subtask_hnd, ""
  );
  return rc;
}

YAPP_MSG_CODE YappSubtaskQueue::push_back_and_lock_task(
  const vector<string> & subtask_hnd_arr, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }
  vector<string> path_arr;
  vector<string> data_arr;
  int size = subtask_hnd_arr.size();
  for (int i = 0; i < size; i++) {
    path_arr.push_back(queue_path + "/" + subtask_hnd_arr[i]);
    data_arr.push_back("");
  }
  rc = zkc_proxy_ptr->create_and_acquire_ex_lock(path_arr, data_arr);
  return rc;
}

YAPP_MSG_CODE YappSubtaskQueue::unlock_and_remv_from_task_queue(
  const vector<string> & subtask_hnd_del, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }
  vector<string> node_arr;
  for (size_t i = 0; i < subtask_hnd_del.size(); i++) {
    node_arr.push_back(queue_path + "/" + subtask_hnd_del[i]);
  }
#ifdef DEBUG_YAPP_SERVICE_UTIL
  std::cerr << ">>>> going to unlock and delete nodes: ";
  for (size_t i = 0; i < node_arr.size(); i++) {
    std::cerr << node_arr[i] << " ";
  }
  std::cerr << std::endl;
#endif
  rc = zkc_proxy_ptr->release_lock_and_delete(node_arr);
  if (YAPP_MSG_SUCCESS == rc) {
    decrement_queue_cnt(node_arr.size());
  }
  return rc;
}

YAPP_MSG_CODE YappSubtaskQueue::unlock_and_remv_from_task_queue(
  const string & task_hnd_del, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }
#ifdef DEBUG_YAPP_SERVICE_UTIL
  string node_tr;
  std::cerr << ">>>> going to unlock and delete nodes: "
            << queue_path + "/" + task_hnd_del << std::endl; 
  zkc_proxy_ptr->print_node_recur(node_tr, queue_path + "/" + task_hnd_del);
  std::cerr << node_tr << std::endl;
#endif
  rc = zkc_proxy_ptr->release_lock_and_delete(queue_path + "/" + task_hnd_del);
  if (YAPP_MSG_SUCCESS == rc) {
    decrement_queue_cnt();
  }
  return rc;
}


YAPP_MSG_CODE YappSubtaskQueue::try_acquire_ex_lock(
  string & lock_hnd_ret, const string task_hnd, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }
  return zkc_proxy_ptr->try_acquire_write_lock(queue_path + "/" + task_hnd,
                                               lock_hnd_ret);
}

YAPP_MSG_CODE YappSubtaskQueue::release_ex_lock(const string & lock_hnd, 
                                                ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }
  return zkc_proxy_ptr->release_lock(lock_hnd);
}

void YappSubtaskQueue::decrement_queue_cnt(int dec) {
  acquire_task_queue_lock();
  task_queued_cnt += dec;
  release_task_queue_lock();
}

void YappSubtaskQueue::increment_queue_cnt(int inc) {
  acquire_task_queue_lock();
  task_queued_cnt += inc;
  release_task_queue_lock();
}

YAPP_MSG_CODE YappSubtaskQueue::sync_and_get_queue(
  vector<string> & node_arr, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }
  rc = zkc_proxy_ptr->sync(queue_path);
  if (YAPP_MSG_SUCCESS == rc) {
    rc = get_task_queue(node_arr, zkc_proxy_ptr);
  }
  return rc;
}

int YappSubtaskQueue::size() {
  return task_queued_cnt;
}

string YappSubtaskQueue::get_queue_path() {
  return queue_path;
}

bool YappSubtaskQueue::is_task_existed(
  const string & task_hnd, ZkClusterProxy * zkc_proxy_ptr)
{
  bool status = false;
  if (NULL == zkc_proxy_ptr) { return status; }
  string node_path = queue_path + "/" + task_hnd;
  if (YAPP_MSG_SUCCESS == zkc_proxy_ptr->node_exists(node_path)) {
    status = true;
  }
  return status;
}

const static string YAPP_TASK_QUEUE_ITEM_FLAG = "P";

YAPP_MSG_CODE YappSubtaskQueue::mark_task_in_queue(
  const string & task_hnd, ZkClusterProxy * zkc_proxy_ptr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return rc; }
  rc = zkc_proxy_ptr->set_node_data(
    queue_path + "/" + task_hnd, YAPP_TASK_QUEUE_ITEM_FLAG
  );
  return rc;
}

bool YappSubtaskQueue::is_task_marked(
  const string & task_hnd, ZkClusterProxy * zkc_proxy_ptr)
{
  bool status = false;
  if (NULL == zkc_proxy_ptr) { return status; }
  string data;
  if (YAPP_MSG_SUCCESS == zkc_proxy_ptr->get_node_data(
        queue_path + "/" + task_hnd, data))
  {
    if (data == YAPP_TASK_QUEUE_ITEM_FLAG) { status = true; }
  }
  return status;
}

YappWorkerList::YappWorkerList(
  const vector<YappServerInfo> & worker_arr, int flag) {
  reset_cur_yapp_worker(worker_arr, flag);
}

bool YappWorkerList::reset_cur_yapp_worker(
  const vector<YappServerInfo> & worker_arr, int flag) {
  yw_list.clear();
  for (size_t i = 0; i < worker_arr.size(); i++) {
    yw_list.push_back(worker_arr[i]);
  }
  cur_itr = yw_list.begin();
  fetch_policy = flag;
  return true;
}

YappWorkerList::~YappWorkerList() {}

int YappWorkerList::size() {
  return yw_list.size();
}

bool YappWorkerList::fetch_cur_yapp_worker(YappServerInfo & yw_srv_ret)
{
  bool status = false;
  if (yw_list.end() != cur_itr) {
    yw_srv_ret = * cur_itr;
    status = true;
  }
  return status;
}

bool YappWorkerList::shift_cur_yapp_worker() {
  bool status = false;
  if (cur_itr != yw_list.end()) {
    cur_itr++;
    if (yw_list.end() == cur_itr) { cur_itr++; }
    status = true;
  }
  return status;
}

int YappWorkerList::remove_and_shift_cur_yapp_worker() {
  if (yw_list.end() != cur_itr) {
    cur_itr = yw_list.erase(cur_itr);
    if (yw_list.end() == cur_itr) { cur_itr++; }
  }
  return yw_list.size();
}
