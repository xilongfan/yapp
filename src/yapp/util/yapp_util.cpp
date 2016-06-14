#include <fstream>
#include <cassert>
#include <sstream>
#include <cstring>
#include <cerrno>
#include <ctime>

#include <pthread.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <linux/netdevice.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "./yapp_util.h"

using namespace yapp::util;

using std::ifstream;
using std::ios;
using std::ostringstream;

YAPP_MSG_CODE ZkClusterProxy::get_yapp_master_addr(string & ret_host,
                                                   string & ret_port,
                                                   string & ret_pcid)
{
  YAPP_MSG_CODE ret_val = YAPP_MSG_INVALID_ZKCONN;
  vector<string> node_arr;
  string election_folder = get_election_path();
  ret_val = get_node_arr(election_folder, node_arr);
  if (node_arr.size() > 0) {
    string master_node_path = election_folder + "/" + node_arr[0];
    string master_data_str;
    ret_val = get_node_data(master_node_path, master_data_str);
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "##########################################################\n";
    std::cerr << master_node_path << ":" << master_data_str << std::endl;
    std::cerr << "##########################################################\n";
#endif
    ret_val = parse_election_node_data_str(master_data_str, ret_host,
                                                            ret_port,
                                                            ret_pcid);
    master_node_path = master_node_path;
    master_host_str = ret_host;
    master_port_str = ret_port;
    master_proc_pid = ret_pcid;
  } else {
    ret_val = YAPP_MSG_INVALID_MASTER_STATUS;
  }
  return ret_val;
}

void ZkClusterProxy::zk_init_callback(zhandle_t *, int type, int state,
                                      const char * path, void * zk_proxy_ptr) {
  ZkClusterProxy * zkc_ptr = ((ZkClusterProxy *)zk_proxy_ptr);
#ifdef DEBUG_YAPP_UTIL
  std::cerr << "-- ZkClusterProxy::zk_init_callback" << std::endl;
#endif
  /**
   * Only start running the master election when the session was fully created!
   */
  if (ZOO_CONNECTED_STATE == state) {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "Session Successfully Created! Thread: " << pthread_self()
              << " now can run election." << std::endl;
#endif
  } else if (ZOO_EXPIRED_SESSION_STATE == state) {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "Session Expired! Thread: " << pthread_self() << std::endl;
#endif
  } else {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "State Event Received! State:" << state
              << ", Thread: " << pthread_self() << std::endl;
    std::cerr << "Type Event Received! Type:" << type
              << ", Thread: " << pthread_self() << std::endl;
#endif
  }
  /**
   * here we use mutex to make sure the main thread calling init_zk_conn is
   * waiting for the cond_var before we sending the signal, since we
   * should never depend on sleep to serialize threads.
   */
  pthread_mutex_lock(&(zkc_ptr->zk_mutex));
  zkc_ptr->callback_ret_val = state;
  pthread_cond_signal(&(zkc_ptr->cond_var));
  pthread_mutex_unlock(&(zkc_ptr->zk_mutex));
}

ZkClusterProxy::ZkClusterProxy(
  bool b_test, int port, int max_q, int max_t, const string & root_pt)
{
  cur_zk_ptr = NULL;
  port_num = port;
  max_queued_task = max_q;
  max_zkc_timeout = max_t;
  b_testing = b_test;
  yapp_mode_code = YAPP_MODE_INVALID;
  callback_ret_val = CALLBACK_RET_INVALID_VAL;
  root_path = root_pt;
  pthread_mutex_init(&zk_mutex, NULL);
  pthread_cond_init(&cond_var, NULL);
  pthread_rwlock_init(&zk_ptr_latch, NULL);

#ifdef DEBUG_YAPP_UTIL
  std::cerr << "finish init ZkClusterProxy" << std::endl;
#endif
}

void ZkClusterProxy::zk_sync_callback(int ret_val, const char * ret_str,
                                                   const void * zctl_ptr) {
#ifdef DEBUG_YAPP_JOB_SUBMISSION
  std::cerr << "==>> ZkClusterProxy::zk_sync_callback" << std::endl;
#endif
  if (ZOK == ret_val) {
#ifdef DEBUG_YAPP_JOB_SUBMISSION
    std::cerr << "Sync Successuflly Finished! Val: " << ret_str
              << " Thread: " << pthread_self()
              << " now can proceed." << std::endl;
#endif
  } else {
#ifdef DEBUG_YAPP_JOB_SUBMISSION
    std::cerr << "Error Found in Sync! State:" << ret_val 
              << ", Val: " << ret_str
              << ", Thread: " << pthread_self() << std::endl;
#endif
  }
}

YAPP_MSG_CODE ZkClusterProxy::sync(const string & path_to_sync) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_SUCCESS;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  int rc = zoo_async(zk_ptr, path_to_sync.c_str(), zk_sync_callback, NULL);
  if (ZOK != rc) { ret_code = YAPP_MSG_INVALID_SYNC; }
#ifdef DEBUG_YAPP_JOB_SUBMISSION
  if (YAPP_MSG_SUCCESS != ret_code) {
    std::cerr << "Error Happened In Sync, RC: " << rc << ", Thread: "
              << pthread_self() << std::endl
              << YAPP_MSG_ENTRY[0 - (int)ret_code] << std::endl;
  } else {
    std::cerr << "Sync Successfully Finished for node: "
              << path_to_sync << ", Thread: " << pthread_self() << std::endl;
  }
#endif
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::init_zk_conn(const string & conn_str) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_SUCCESS;
  assert(false == conn_str.empty());
  /** automatically blocks until a session has been set up successfully. */
  pthread_mutex_lock(&zk_mutex);
  if (NULL != cur_zk_ptr) { closed_zk_ptr_arr.push_back(cur_zk_ptr); }
  zk_conn_str = conn_str;
  
  zhandle_t * zk_ptr = zookeeper_init (
    zk_conn_str.c_str(), zk_init_callback, max_zkc_timeout, 0, this, 0
  );

  if (true != set_cur_zkptr_atomic(zk_ptr)) { return ret_code; }


#ifdef DEBUG_YAPP_UTIL
  std::cerr << "Wait for Session to be Created. Thread: "
            << pthread_self() << std::endl;
#endif
  /**
   * - The conditional wait here is only supposed to return either ETIMEDOUT
   *   or the certain condition satisfied to avoid spurious wakeup or blocking
   */
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += MAX_COND_WAIT_TIME_OUT;
  int rc = 0;
  while (CALLBACK_RET_INVALID_VAL == callback_ret_val && rc == 0) {
    rc = pthread_cond_timedwait(&cond_var, &zk_mutex, &ts);
  }
  if (ZOO_CONNECTED_STATE != callback_ret_val) {
    ret_code = (ETIMEDOUT == rc) ? YAPP_MSG_INVALID_EXCEED_MAX_TIMEOUT :
                                   YAPP_MSG_INVALID_ZKCONN;
  }
  callback_ret_val = CALLBACK_RET_INVALID_VAL;
  pthread_mutex_unlock(&zk_mutex);
#ifdef DEBUG_YAPP_UTIL
  if (YAPP_MSG_SUCCESS == ret_code) {
    std::cerr << "Session Successfully Created! Thread: "
              << pthread_self() << std::endl;
  } else {
    std::cerr << "Error Happened In Setting Up Conn.: " << rc << ", Thread: "
              << pthread_self() << std::endl
              << YAPP_MSG_ENTRY[0 - (int)ret_code] << std::endl;
  }
#endif

  return ret_code;
}

ZkClusterProxy::~ZkClusterProxy() {
  if (NULL != cur_zk_ptr) {
    zookeeper_close(cur_zk_ptr);
  }
  for (unsigned int i = 0; i < closed_zk_ptr_arr.size(); i++) {
    zookeeper_close(closed_zk_ptr_arr[i]);
  }
  pthread_mutex_destroy(&zk_mutex);
  pthread_cond_destroy(&cond_var);
  pthread_rwlock_destroy(&zk_ptr_latch);
}

YAPP_MSG_CODE ZkClusterProxy::node_exists(const string & node_path) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  int ret = zoo_exists(zk_ptr, node_path.c_str(), 0, NULL);
  if (ZOK == ret) { ret_code = YAPP_MSG_SUCCESS; }
  else if (ZNONODE == ret) { ret_code = YAPP_MSG_INVALID_NONODE; }
  else if (ZINVALIDSTATE == ret) { ret_code = YAPP_MSG_INVALID_ZKC_HNDLE_STATE;}
#ifdef DEBUG_YAPP_UTIL
  std::cerr << "RET: " << ret << std::endl;
#endif
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::del_node(const string & node_path) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  if (ZOK == zoo_delete(zk_ptr, node_path.c_str(), -1)) {
    ret_code = YAPP_MSG_SUCCESS;
  }
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::create_node(const string & path_str,
                                          const string & data_str,
                                          string & path_created,
                                          int node_flag) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  char * path_created_buf = new char[MAX_PATH_LEN];
  memset(path_created_buf, 0, MAX_PATH_LEN);

  int ret_val = zoo_create(
    zk_ptr, path_str.c_str(), data_str.c_str(), data_str.size(),
    &ZOO_OPEN_ACL_UNSAFE, node_flag, path_created_buf, MAX_PATH_LEN
  ); 

  if (ZOK == ret_val) {
    path_created = path_created_buf;
    ret_code = YAPP_MSG_SUCCESS;
  }

#ifdef DEBUG_YAPP_UTIL
  std::cerr << ">>>> Finish creating election node: " << path_created
            << " with Value: " << data_str << " RET: " << ret_val << std::endl;
#endif
  delete[] path_created_buf;
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::batch_create(
  const vector<string> & path_arr, const vector<string> & data_arr)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;
  if (path_arr.size() != data_arr.size()) { return ret_code; }

  int tot_size = path_arr.size();
  int itr_cnts = tot_size / MAX_BATCH_CREATE_CHUNK;
  int tsk_remn = tot_size - itr_cnts * MAX_BATCH_CREATE_CHUNK;
  if (tsk_remn > 0) { itr_cnts++; }
 
  int prev = 0;
  int next = 0;
  vector<string>::const_iterator path_prev_itr = path_arr.begin();
  vector<string>::const_iterator path_next_itr = path_arr.begin();
  vector<string>::const_iterator data_prev_itr = data_arr.begin();
  vector<string>::const_iterator data_next_itr = data_arr.begin();

  for (int c = 0; c < itr_cnts; c++)
  {
    next += MAX_BATCH_CREATE_CHUNK;
    next = (next > tot_size) ? tot_size : next;

    path_prev_itr = path_arr.begin() + prev;
    path_next_itr = path_arr.begin() + next;
    data_prev_itr = data_arr.begin() + prev;
    data_next_itr = data_arr.begin() + next;

    vector<string> sub_path_arr(path_prev_itr, path_next_itr);
    vector<string> sub_data_arr(data_prev_itr, data_next_itr);

    ret_code = batch_create_atomic(sub_path_arr, sub_data_arr);

    usleep(BATCH_OP_NICE_TIME_IN_MICRO_SEC);
    if (YAPP_MSG_SUCCESS != ret_code) { break; }
    prev = next;
  } /** end of for **/
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::batch_create_atomic(
  const vector<string> & path_arr, const vector<string> & data_arr)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  if (path_arr.size() != data_arr.size()) { return ret_code; }

  /** initiate & zero out the necessary mem. **/
  int total_node_cnt = path_arr.size();
  zoo_op_t * ops_set = new zoo_op_t[total_node_cnt];
  zoo_op_result_t * ret_set = new zoo_op_result_t[total_node_cnt];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ops_set, 0, sizeof(zoo_op_t) * total_node_cnt);
  memset(ret_set, 0, sizeof(zoo_op_result_t) * total_node_cnt);

  /** generate all set of full path, add them to the operation set  **/
  char ret_path[MAX_PATH_LEN];
  int  ret_val = -1;

  for (int i = 0; i < total_node_cnt; i++) {
    zoo_create_op_init(
      &ops_set[i], path_arr[i].c_str(), data_arr[i].c_str(), data_arr[i].size(),
      &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
    );
  }

  /** only execute create when all operations are added correctly. **/
  if (total_node_cnt > 0) {
    /** execute in batch, and we only care about if last node got created **/
    ret_val = zoo_multi(zk_ptr, total_node_cnt, &ops_set[0], &ret_set[0]);
#ifdef DEBUG_YAPP_JOB_SUBMISSION
    std::cerr << ">>>> batch_create_atomic: " << path_arr.size() << " nodes "
              << " with ret of " << ret_val << std::endl;
#endif
    if ((int)ZOK == ret_val) { ret_code = sync(path_arr.back()); }
  }

  delete[] ops_set;
  delete[] ret_set;

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::create_node_recur(const string & path_str,
                                                const string & data_str,
                                                string & path_created,
                                                int node_flag) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  vector<string> ret_dir_arr;
  string ret_node_name;
  bool b_path_valid = StringUtil::parse_full_zk_node_path(
    path_str, ret_dir_arr, ret_node_name
  );
  if (false == b_path_valid || ret_node_name.empty()) { return ret_code; }

  /** initiate & zero out the necessary mem. **/
  int total_node_cnt = ret_dir_arr.size() + 1;
  string * path_arr = new string[total_node_cnt];
  zoo_op_t * ops_set = new zoo_op_t[total_node_cnt];
  zoo_op_result_t * ret_set = new zoo_op_result_t[total_node_cnt];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ops_set, 0, sizeof(zoo_op_t) * total_node_cnt);
  memset(ret_set, 0, sizeof(zoo_op_result_t) * total_node_cnt);

  /** generate all set of full path, add them to the operation set  **/
  char ret_path[MAX_PATH_LEN];
  bool b_valid = false;
  int  ret_val = -1;
  int  idx_to_start_create = total_node_cnt;

  for (int i = 0; i < total_node_cnt - 1; i++) {
    path_arr[i] = "/" + ret_dir_arr[i];
    if (i > 0) {
      path_arr[i] = path_arr[i - 1] + path_arr[i];
    }
  }
  path_arr[total_node_cnt - 1] = path_str;

  for (int i = total_node_cnt - 1; i >= 0; i--) {
    b_valid = false;
    ret_val = zoo_exists(zk_ptr, path_arr[i].c_str(), 0, NULL);
    if (ZNONODE == ret_val || ZOK == ret_val) { b_valid = true; }
    if (ZNONODE == ret_val) {
#ifdef DEBUG_YAPP_UTIL
      if (i == (total_node_cnt - 1)) {
        std::cerr << "==>> adding " << path_arr[i] << " : " << data_str << std::endl;
      } else {
        std::cerr << "==>> adding " << path_arr[i] << std::endl;
      }
#endif 
      if (i == (total_node_cnt - 1)) {
        zoo_create_op_init(
          &ops_set[i], path_arr[i].c_str(), data_str.c_str(), data_str.size(), &ZOO_OPEN_ACL_UNSAFE,
          (i == (total_node_cnt - 1)) ? node_flag : 0, ret_path, sizeof(ret_path)
        );
      } else {
        zoo_create_op_init(
          &ops_set[i], path_arr[i].c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE,
          (i == (total_node_cnt - 1)) ? node_flag : 0, ret_path, sizeof(ret_path)
        );
      }
      idx_to_start_create = idx_to_start_create - 1;
    }
#ifdef DEBUG_YAPP_UTIL
    if (ZOK == ret_val) {
      std::cerr << "==>> existed " << path_arr[i] << std::endl;
    }
#endif 
    if (ZOK == ret_val || false == b_valid) { break; }
  }

  /** only execute create when all operations are added correctly. **/
  if (true == b_valid && idx_to_start_create < total_node_cnt) {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "==>> start: " << idx_to_start_create
              << " total: " << total_node_cnt
              << " create: " << total_node_cnt - idx_to_start_create
              << std::endl;
    for (int i = idx_to_start_create; i < total_node_cnt; i++) {
      std::cerr << "==>> going to add: " << ops_set[i].create_op.path
                << std::endl;
    }
#endif
    /** execute in batch, and we only care about if last node got created **/
    ret_val = zoo_multi(
      zk_ptr, total_node_cnt - idx_to_start_create,
      &ops_set[idx_to_start_create], &ret_set[idx_to_start_create]
    );
    if ((int)ZOK == ret_val) {
      path_created = ret_path;
      ret_code = YAPP_MSG_SUCCESS;
    }
  }
  delete[] ops_set;
  delete[] ret_set;
  delete[] path_arr;

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::get_node_arr_recur_pre_desc(
  const string & root_path, vector<string> & revr_nodes_array)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  /**
   * revert the usage of stack will actually gave us a post order treversal with
   * reverse sorted order without using recursion, would be helpful for dealing
   * with hierarchical data.
   */
  vector<string> temp_nodes_stack;
  vector<string> temp_child_arr;
  string cur_node = root_path;

  temp_nodes_stack.push_back(root_path);
  while (false == temp_nodes_stack.empty()) {
    cur_node = temp_nodes_stack.back();
    ret_code = get_node_arr(cur_node, temp_child_arr);
    if (YAPP_MSG_SUCCESS != ret_code) { break; }
    revr_nodes_array.push_back(cur_node);
    temp_nodes_stack.pop_back();
    for (size_t i = 0; i < temp_child_arr.size(); i++) {
      temp_nodes_stack.push_back(cur_node + "/" + temp_child_arr[i]);
    }
    temp_child_arr.clear();
  }
  if (YAPP_MSG_SUCCESS != ret_code) { return ret_code; }

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::get_node_arr_recur_post_asc(
  const string & root_path, vector<string> & nodes_array)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;
  vector<string> revr_nodes_array;
  ret_code = get_node_arr_recur_pre_desc(root_path, revr_nodes_array);
  if (YAPP_MSG_SUCCESS == ret_code) {
    int node_cnt = revr_nodes_array.size();
    for (int i = 0; i < node_cnt; i++) {
      nodes_array.push_back(revr_nodes_array[node_cnt - i - 1]);
    }
  }
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::print_node_recur(string & node_tree_ret,
                                               const string & root_path,
                                               bool b_show_node_value,
                                               bool b_show_full_path) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  /**
   * revert the usage of stack will actually gave us a post order treversal with
   * reverse sorted order without using recursion, would be helpful for dealing
   * with hierarchical data.
   */
  vector<string> revr_ndata_array;
  vector<string> revr_nodes_array;
  vector<string> temp_nodes_stack;
  vector<string> temp_child_array;
  vector<int>    level_cnts_array;
  vector<int>    temp_level_stack;

  string cur_node = root_path;
  int cur_level = 0;

  temp_level_stack.push_back(0);
  temp_nodes_stack.push_back(root_path);
  while (false == temp_nodes_stack.empty()) {
    cur_level = temp_level_stack.back();
    cur_node = temp_nodes_stack.back();
    ret_code = get_node_arr(cur_node, temp_child_array);
    if (YAPP_MSG_SUCCESS != ret_code) { break; }
    level_cnts_array.push_back(cur_level);
    revr_nodes_array.push_back(cur_node);
    temp_nodes_stack.pop_back();
    temp_level_stack.pop_back();
    for (int i = int(temp_child_array.size()) - 1; i >= 0; i--) {
      temp_nodes_stack.push_back(cur_node + "/" + temp_child_array[i]);
      temp_level_stack.push_back(cur_level + 1);
    }
    temp_child_array.clear();
  }

  int revr_nodes_arr_size = revr_nodes_array.size();

  if (true == b_show_node_value) {
    string base_str;
    for (int i = 0; i < revr_nodes_arr_size; i++) {
      string data_str;
      get_node_data(revr_nodes_array[i], data_str);

      /** convert the number to more informative string constants. */
      base_str = StringUtil::get_path_basename(revr_nodes_array[i]);
      if (string("cur_status") == base_str) {
        data_str = string(PROC_STATUS_CODE_ENTRY[atoi(data_str.c_str())]);
      }
      if (string("priority") == base_str) {
        data_str = string(PROC_PRORITY_CODE_ENTRY[atoi(data_str.c_str())]);
      }
      if (string("input_type") == base_str) {
#ifdef DEBUG_YAPP_UTIL
        std::cerr << TASK_INPUT_TYPE_ENTRY[5] << std::endl << data_str << std::endl
                  << atoi(data_str.c_str()) << std::endl;
#endif
        data_str = string(TASK_INPUT_TYPE_ENTRY[atoi(data_str.c_str())]);
      }

      revr_ndata_array.push_back(data_str);
    }
  }

  if (false == b_show_full_path) {
    for (int i = 1; i < revr_nodes_arr_size; i++) {
      revr_nodes_array[i] = revr_nodes_array[i].substr(
        revr_nodes_array[i].find_last_of('/') + 1
      );
    }
  }

  int total_nodes = level_cnts_array.size();
  int * parent_id_arr = new int[total_nodes];
  int cur_parent_id = 0;
  int level_diff = 0;
  parent_id_arr[0] = 0;
  temp_level_stack.push_back(0);
  for (int i = 1; i < total_nodes; i++) {
    cur_parent_id = temp_level_stack.back();
    level_diff = level_cnts_array[i] - level_cnts_array[cur_parent_id];
    if (1 == level_diff) {
      parent_id_arr[i] = cur_parent_id;
    } else if (2 == level_diff) {
      temp_level_stack.push_back(i - 1);
      parent_id_arr[i] = i - 1;
    } else {
      while (1 > level_diff) {
        temp_level_stack.pop_back();
        cur_parent_id = temp_level_stack.back();
        level_diff = level_cnts_array[i] - level_cnts_array[cur_parent_id];
      }
      parent_id_arr[i] = cur_parent_id;
    }
  }

  int * child_cnt_arr = new int[total_nodes];
  int * last_chid_arr = new int[total_nodes];
  int * firt_chid_arr = new int[total_nodes];
  for (int i = 0; i < total_nodes; i++) {
    child_cnt_arr[i] = -1;
    last_chid_arr[i] = -1;
    firt_chid_arr[i] = -1;
  }
  child_cnt_arr[0] = 0;
  for (int i = 1; i < total_nodes; i++) {
    if (-1 == child_cnt_arr[i]) {
      child_cnt_arr[i] = 0;
    }
    if (-1 == firt_chid_arr[parent_id_arr[i]]) {
      firt_chid_arr[parent_id_arr[i]] = i;
    }
    child_cnt_arr[parent_id_arr[i]]++;
    last_chid_arr[parent_id_arr[i]] = i;
  }
#ifdef DEBUG_YAPP_UTIL
  for (int i = 0; i < revr_nodes_arr_size; i++) {
    std::cerr << "==>> id: " << i
              << " level: " << level_cnts_array[i]
              << " parent: " << parent_id_arr[i]
              << " child_cnt: " << child_cnt_arr[i]
              << " last_chid: " << last_chid_arr[i]
              << " first_chid: " << firt_chid_arr[i]
              << " path: " << revr_nodes_array[i] << std::endl;
  }
#endif

  vector<int> parent_id_stack;
  int parent_id_stack_size = 0;

  ostringstream tree_stream;
  if (revr_nodes_arr_size > 0) {
    if (true == b_show_node_value) {
      tree_stream << revr_nodes_array[0] << ": "
                  << revr_ndata_array[0] << std::endl;
    } else {
      tree_stream << revr_nodes_array[0] << std::endl;
    }
    parent_id_stack.push_back(0);
  }
  
  for (int i = 1; i < revr_nodes_arr_size; i++) {
    child_cnt_arr[parent_id_arr[i]]--;
    if (parent_id_arr[i] > parent_id_stack.back()) {
      parent_id_stack.push_back(parent_id_arr[i]);
    }
    while (parent_id_stack.back() > parent_id_arr[i]) {
#ifdef DEBUG_YAPP_UTIL
      std::cerr << "==>> cur_parent: " << parent_id_arr[i] << " : "
                << parent_id_stack.back() << "poped." << std::endl;
#endif
      parent_id_stack.pop_back();
    }
#ifdef DEBUG_YAPP_UTIL
    for (size_t c = 0; c < parent_id_stack.size(); c++) {
      std::cerr << parent_id_stack[c] << " : ";
    }
    std::cerr << std::endl;
#endif 
    parent_id_stack_size = parent_id_stack.size();
    if (i == firt_chid_arr[parent_id_arr[i]]) {
      for (int c = 0; c < parent_id_stack_size - 1; c++) {
        if (0 < child_cnt_arr[parent_id_stack[c]]) {
          tree_stream << " |";
        } else {
          tree_stream << "  ";
        }
        tree_stream << "  ";
      }
      tree_stream << " +" << std::endl;
    }
    for (int c = 0; c < parent_id_stack_size; c++) {
      if (0 < child_cnt_arr[parent_id_stack[c]]) {
        tree_stream << " |";
      } else if (0 == child_cnt_arr[parent_id_stack[c]]) {
        if (c == parent_id_stack_size - 1) {
          tree_stream << " `";
        } else {
          tree_stream << "  ";
        }
      } else {
        tree_stream << "  ";
      }
      if (c != parent_id_stack_size - 1) { tree_stream << "  "; }
    }
    if (true == b_show_node_value) {
      tree_stream << "- " << revr_nodes_array[i] << ": "
                  << revr_ndata_array[i] << std::endl;
    } else {
      tree_stream << "- " << revr_nodes_array[i] << std::endl;
    }
  }

  if (YAPP_MSG_SUCCESS == ret_code) {
    node_tree_ret = tree_stream.str();
  }
  delete[] child_cnt_arr;
  delete[] last_chid_arr;
  delete[] firt_chid_arr;
  delete[] parent_id_arr;

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::batch_delete(const vector<string> & nodes_todel) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  int tot_size = nodes_todel.size();
  int itr_cnts = tot_size / MAX_BATCH_CHUNK;
  int tsk_remn = tot_size - itr_cnts * MAX_BATCH_CHUNK;
  if (tsk_remn > 0) { itr_cnts++; }
 
  int prev = 0;
  int next = 0;
  vector<string>::const_iterator prev_itr = nodes_todel.begin();
  vector<string>::const_iterator next_itr = nodes_todel.begin();

  for (int c = 0; c < itr_cnts; c++)
  {
    next += MAX_BATCH_CHUNK;
    next = (next > tot_size) ? tot_size : next;

    prev_itr = nodes_todel.begin() + prev;
    next_itr = nodes_todel.begin() + next;

    vector<string> revr_nodes_todel(prev_itr, next_itr);

    int node_cnt = revr_nodes_todel.size();
  
    zoo_op_result_t * ret_set = new zoo_op_result_t[node_cnt];
    zoo_op_t *        ops_set = new zoo_op_t[node_cnt];
    assert(NULL != ops_set);
    assert(NULL != ret_set);
    memset(ret_set, 0, sizeof(zoo_op_result_t) * node_cnt);
    memset(ops_set, 0, sizeof(zoo_op_t) * node_cnt);
  
    for (int i = 0; i < node_cnt; i++) {
      zoo_delete_op_init(&ops_set[i], revr_nodes_todel[i].c_str(), -1);
#ifdef DEBUG_YAPP_UTIL
      // std::cerr << "==>> " << revr_nodes_todel[node_cnt - i - 1] << std::endl;
      std::cerr << "==>> " << revr_nodes_todel[i] << std::endl;
#endif
    } /** end of for **/
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "==>> going to delete " << node_cnt << " nodes!" << std::endl;
    std::cerr.flush();
#endif
    int rval = zoo_multi(zk_ptr, node_cnt, ops_set, ret_set);
    if ((int)ZOK == rval) {
      ret_code = YAPP_MSG_SUCCESS;
#ifdef DEBUG_YAPP_UTIL
      std::cerr << "==>> succeeded in deleting " << node_cnt << " nodes!" << std::endl;
#endif
    } else {
#ifdef DEBUG_YAPP_UTIL
      std::cerr << "==>> Trying to Pre-Set Some Value to Work Around the ZK Bugs." << std::endl;
#endif
      vector<string> data_arr(node_cnt, "");
      if (YAPP_MSG_SUCCESS == batch_set(revr_nodes_todel, data_arr)) {
        rval = zoo_multi(zk_ptr, node_cnt, ops_set, ret_set);
      }
      if ((int)ZOK == rval) {
        ret_code = YAPP_MSG_SUCCESS;
#ifdef DEBUG_YAPP_UTIL
        std::cerr << "==>> succeeded in deleting " << node_cnt << " nodes!" << std::endl;
#endif
      } else {
#ifdef DEBUG_YAPP_UTIL
        std::cerr << "==>> errno: " << rval << " fail to delete "
                  << node_cnt << " nodes!" << std::endl;
        std::cerr.flush();
#endif
      }
    } /** end of if **/
    delete[] ret_set;
    delete[] ops_set;

    prev = next;
  } /** end of for **/
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::print_queue_stat(string & node_tree_ret,
                                         const string & leaf_filter,
                                         bool is_display_failed_only) {
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  string range_file_tsk_queue_path = get_rfile_task_queue_path_prefix();
  string range_tsk_queue_path      = get_queue_path_prefix();
  string leaf_filter_to_disp       = leaf_filter;
  StringUtil::trim_string(leaf_filter_to_disp);

  if (leaf_filter_to_disp.empty()) { leaf_filter_to_disp = "job-"; }
  string failed_tsk_queue_path     = get_failed_task_queue_path_prefix();
  string new_tsk_queue_path        = get_newtsk_queue_path_prefix();
  string ready_tsk_queue_path      = get_ready_queue_path_prefix();
  string runn_tsk_queue_path       = get_running_queue_path_prefix();
  string paused_queue_path         = get_paused_queue_path_prefix();
  string term_queue_path           = get_terminated_queue_path_prefix();

  vector<string> child_nodes_arr;
  vector<string> hndle_nodes_arr;
  vector<string> tsk_hndl_arr;

  rc = get_node_arr(range_file_tsk_queue_path, child_nodes_arr);
  bool b_hndl_type_is_proc = false;

  /** 1. print necessary info. if any matches found in range file tasks. */
  if (child_nodes_arr.size() > 0 && YAPP_MSG_SUCCESS == rc) {
    /** 1-1. find all possible result set to show. */
    int rf_tsk_size = child_nodes_arr.size();
    if (leaf_filter_to_disp.size() > child_nodes_arr.front().size()) {
      for (int i = 0; i < rf_tsk_size; i++) {
        if (0 == leaf_filter_to_disp.find(child_nodes_arr[i])) {
          tsk_hndl_arr.push_back(child_nodes_arr[i]);
        }
      }
      b_hndl_type_is_proc = true;
    } else {
      for (int i = 0; i < rf_tsk_size; i++) {
        if (0 == child_nodes_arr[i].find(leaf_filter_to_disp)) {
          tsk_hndl_arr.push_back(child_nodes_arr[i]);
        }
      }
    }

    /** 1-2. print all possible result found. */
    int sel_cnt = tsk_hndl_arr.size();
    for (int i = 0; i < sel_cnt; i++) {
      child_nodes_arr.clear();
      if (0 == i) {
        node_tree_ret.append(range_file_tsk_queue_path);
        node_tree_ret.append("\n");
        node_tree_ret.append(" +\n");
      }
      if (sel_cnt - 1 == i) {
        node_tree_ret.append(" `- ");
      } else {
        node_tree_ret.append(" |- ");
      }
      node_tree_ret.append(tsk_hndl_arr[i]);
      node_tree_ret.append("\n");

      string tmp_path = range_file_tsk_queue_path + "/" +  tsk_hndl_arr[i];
      rc = get_node_arr(tmp_path, child_nodes_arr);
      int chld_cnt = child_nodes_arr.size();
      for (int c = 0; c < chld_cnt; c++) {
        if (0 == c) {
          if (sel_cnt - 1 == i)  { node_tree_ret.append("     +\n"); }
          else { node_tree_ret.append(" |   +\n"); }
        }
        if (chld_cnt - 1 == c) {
          if (sel_cnt - 1 == i)  { node_tree_ret.append("     `- "); }
          else { node_tree_ret.append(" |   `- "); }
        } else {
          if (sel_cnt - 1 == i)  { node_tree_ret.append("     |- "); }
          else { node_tree_ret.append(" |   |- "); }
        }
        node_tree_ret.append(child_nodes_arr[c]);
        node_tree_ret.append("\n");
        string hndl_path = tmp_path + "/" + child_nodes_arr[c];
        hndle_nodes_arr.clear();
        rc = get_node_arr(hndl_path, hndle_nodes_arr);
#ifdef DEBUG_YAPP_UTIL
        std::cerr << ">>>> Printing Nodes Under: " << hndl_path << std::endl;
        for (size_t x = 0; x < hndle_nodes_arr.size(); x++) {
          std::cerr << hndle_nodes_arr[x] << " ";
        }
        std::cerr << std::endl;
#endif
        if (YAPP_MSG_SUCCESS != rc) { break; }
        vector<string> proc_hndl_arr;
        int hndl_cnt = hndle_nodes_arr.size();
        if (true == b_hndl_type_is_proc) {
          for (int s = 0; s < hndl_cnt; s++) {
            if (0 == hndle_nodes_arr[s].find(leaf_filter_to_disp)) {
              proc_hndl_arr.push_back(hndle_nodes_arr[s]);
            }
          }
          hndl_cnt = proc_hndl_arr.size();
        } else {
          proc_hndl_arr = hndle_nodes_arr;
        }
        for (int s = 0; s < hndl_cnt; s++) {
          if (0 == s) {
            if (chld_cnt - 1 == c) {
              if (sel_cnt - 1 == i)  { node_tree_ret.append("         +\n"); }
              else { node_tree_ret.append(" |       +\n"); }
            } else {
              if (sel_cnt - 1 == i)  { node_tree_ret.append("     |   +\n"); }
              else { node_tree_ret.append(" |   |   +\n"); }
            }
          } /* if (0 ==s) */
          if (hndl_cnt - 1 == s) {
            if (chld_cnt - 1 == c) {
              if (sel_cnt - 1 == i)  { node_tree_ret.append("         `- "); }
              else { node_tree_ret.append(" |       `- "); }
            } else {
              if (sel_cnt - 1 == i)  { node_tree_ret.append("     |   `- "); }
              else { node_tree_ret.append(" |   |   `- "); }
            } /* if (chld_cnt - 1 == c) */
          } else {
            if (chld_cnt - 1 == c) {
              if (sel_cnt - 1 == i)  { node_tree_ret.append("         |- "); }
              else { node_tree_ret.append(" |       |- "); }
            } else {
              if (sel_cnt - 1 == i)  { node_tree_ret.append("     |   |- "); }
              else { node_tree_ret.append(" |   |   |- "); }
            } /* if (chld_cnt - 1 == c) */
          } /* if (hndl_ctn - 1 = s) */
          node_tree_ret.append(proc_hndl_arr[s]);
          node_tree_ret.append("\n");
        } /* for (int s = 0; s < hndl_cnt; s++) */
        if (YAPP_MSG_SUCCESS != rc) { break; }
      } /* for (int c = 0; c < chld_cnt; c++) */
      if (YAPP_MSG_SUCCESS != rc) { break; }
    } /* for (int i = 0; i < sel_cnt; i++) */
  } /* if (child_nodes_arr.size() > 0 && YAPP_MSG_SUCCESS == rc) */

  /** 2. print necessary info. if any matches found in cur. running queue. */
  vector<string> cur_queue_arr;
  vector<string> hnd_selec_arr;
  cur_queue_arr.push_back(failed_tsk_queue_path);
  if (!is_display_failed_only) {
    cur_queue_arr.push_back(new_tsk_queue_path);
    cur_queue_arr.push_back(ready_tsk_queue_path);
    cur_queue_arr.push_back(runn_tsk_queue_path);
    cur_queue_arr.push_back(paused_queue_path);
    cur_queue_arr.push_back(term_queue_path);
  }

  int tot_tcnt_arr[] = { 0, 0, 0, 0, 0, 0, };
  int tcnt_arr[]     = { 0, 0, 0, 0, 0, 0, };
  const int failed_idx = 0, newtsk_idx = 1, readyt_idx = 2,
            runtsk_idx = 3, paused_idx = 4, termts_idx = 5;
  int tot_tcnt_tsk = 0;
  int tcnt_tsk     = 0;

  node_tree_ret.append(range_tsk_queue_path);
  node_tree_ret.append("\n");
  int queue_cnt = cur_queue_arr.size();
  for (int i = 0; i < queue_cnt; i++) {
    if (0 == i) {
      node_tree_ret.append(" +\n");
    }
    if (queue_cnt - 1 == i) { node_tree_ret.append(" `- "); }
    else { node_tree_ret.append(" |- "); }
    node_tree_ret.append(StringUtil::get_path_basename(cur_queue_arr[i]));
    node_tree_ret.append("\n");
    hndle_nodes_arr.clear();
    hnd_selec_arr.clear();
    rc = get_node_arr(cur_queue_arr[i], hndle_nodes_arr);
    if (YAPP_MSG_SUCCESS != rc) { break; }
#ifdef DEBUG_YAPP_UTIL
    std::cerr << ">>>> Printing Nodes Under: " << cur_queue_arr[i] << std::endl;
    for (size_t x = 0; x < hndle_nodes_arr.size(); x++) {
      std::cerr << hndle_nodes_arr[x] << " ";
    }
    std::cerr << std::endl;
#endif

    int hndl_cnt = hndle_nodes_arr.size();
    tot_tcnt_arr[i] = hndl_cnt;
    tot_tcnt_tsk   += hndl_cnt;
    for (int c = 0; c < hndl_cnt; c++) {
      if (0 == hndle_nodes_arr[c].find(leaf_filter_to_disp)) {
        hnd_selec_arr.push_back(hndle_nodes_arr[c]);
      }
    }
    hndl_cnt = hnd_selec_arr.size();
    for (int c = 0; c < hndl_cnt; c++) {
      if (0 == c) {
        if (queue_cnt - 1 == i)  { node_tree_ret.append("     +\n"); }
        else { node_tree_ret.append(" |   +\n"); }
      }
      if (hndl_cnt - 1 == c) {
        if (queue_cnt - 1 == i)  { node_tree_ret.append("     `- "); }
        else { node_tree_ret.append(" |   `- "); }
      } else {
        if (queue_cnt - 1 == i)  { node_tree_ret.append("     |- "); }
        else { node_tree_ret.append(" |   |- "); }
      }
      node_tree_ret.append(hnd_selec_arr[c]);
      node_tree_ret.append("\n");
      tcnt_arr[i]++;
      tcnt_tsk++;
    } /* for (int c = 0; c < hndl_cnt; c++) */
  } /* for (int i = 0; i < queue_cnt; i++) */

  if (false == leaf_filter.empty()) {
    node_tree_ret.append("Query Result Summary:\n");
    node_tree_ret.append("    Task To Query in Queue: ");
    node_tree_ret.append(leaf_filter_to_disp);
    node_tree_ret.append("\n");
    node_tree_ret.append("    Subtask Found: ");
    node_tree_ret.append(StringUtil::convert_int_to_str(tcnt_tsk));
    node_tree_ret.append("\n");
    node_tree_ret.append("    Newly Created: ");
    node_tree_ret.append(StringUtil::convert_int_to_str(tcnt_arr[newtsk_idx]));
    node_tree_ret.append("\n");
    node_tree_ret.append("    Ready to Fire: ");
    node_tree_ret.append(StringUtil::convert_int_to_str(tcnt_arr[readyt_idx]));
    node_tree_ret.append("\n");
    node_tree_ret.append("    Curr. Running: ");
    node_tree_ret.append(StringUtil::convert_int_to_str(tcnt_arr[runtsk_idx]));
    node_tree_ret.append("\n");
    node_tree_ret.append("    Succ. Finished: ");
    node_tree_ret.append(StringUtil::convert_int_to_str(tcnt_arr[termts_idx]));
    node_tree_ret.append("\n");
    node_tree_ret.append("    Being Paused..: ");
    node_tree_ret.append(StringUtil::convert_int_to_str(tcnt_arr[paused_idx]));
    node_tree_ret.append("\n");
    node_tree_ret.append("    Already Failed: ");
    node_tree_ret.append(StringUtil::convert_int_to_str(tcnt_arr[failed_idx]));
    node_tree_ret.append("\n");
  }

  node_tree_ret.append("Overall Summary for the Entire Queue:\n");
  node_tree_ret.append("    Total Subtask: ");
  node_tree_ret.append(StringUtil::convert_int_to_str(tot_tcnt_tsk));
  node_tree_ret.append("\n");
  node_tree_ret.append("    Newly Created: ");
  node_tree_ret.append(StringUtil::convert_int_to_str(tot_tcnt_arr[newtsk_idx]));
  node_tree_ret.append("\n");
  node_tree_ret.append("    Ready to Fire: ");
  node_tree_ret.append(StringUtil::convert_int_to_str(tot_tcnt_arr[readyt_idx]));
  node_tree_ret.append("\n");
  node_tree_ret.append("    Curr. Running: ");
  node_tree_ret.append(StringUtil::convert_int_to_str(tot_tcnt_arr[runtsk_idx]));
  node_tree_ret.append("\n");
  node_tree_ret.append("    Succ. Finished: ");
  node_tree_ret.append(StringUtil::convert_int_to_str(tot_tcnt_arr[termts_idx]));
  node_tree_ret.append("\n");
  node_tree_ret.append("    Being Paused..: ");
  node_tree_ret.append(StringUtil::convert_int_to_str(tot_tcnt_arr[paused_idx]));
  node_tree_ret.append("\n");
  node_tree_ret.append("    Already Failed: ");
  node_tree_ret.append(StringUtil::convert_int_to_str(tot_tcnt_arr[failed_idx]));
  node_tree_ret.append("\n");
  node_tree_ret.append("NOTE:\n");
  node_tree_ret.append("\
    If the size of the newly created queue is close to the limit of ");
  node_tree_ret.append(StringUtil::convert_int_to_str(max_queued_task));
  node_tree_ret.append(",\n\
then it is highly likely for Yapp to reject the next job submission.\n");

  return rc;
}


YAPP_MSG_CODE ZkClusterProxy::delete_node_recur(const string & path_str) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

#ifdef DEBUG_YAPP_UTIL
  std::cerr << "==>> GOING TO RM -RF " << path_str << std::endl;
#endif

  vector<string> nodes_todel;
  
  ret_code = get_node_arr_recur_post_asc(path_str, nodes_todel);
  if (YAPP_MSG_SUCCESS != ret_code) { return ret_code; }

  return batch_delete(nodes_todel);
}

YAPP_MSG_CODE ZkClusterProxy::purge_job_info(const string & job_hndl) {
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  vector<string> nodes_arr;
  string job_node_path = get_job_full_path(job_hndl);

  if (YAPP_MSG_INVALID_NONODE == node_exists(job_node_path)) {
    rc = YAPP_MSG_SUCCESS;
  } else {
    rc = get_node_arr_recur_post_asc(job_node_path, nodes_arr);
  }

  if (YAPP_MSG_SUCCESS != rc) { return rc; }

  string queue_path =  get_rfile_task_queue_path_prefix();
  vector<string> task_hndl_arr;
  rc = get_node_arr(queue_path, task_hndl_arr);
  if (YAPP_MSG_SUCCESS != rc) { return rc; }
  int task_cnt = task_hndl_arr.size();

  if (task_cnt > 0) {
    /** make sure the job_hndl can won't be mapped to multiple ones. **/
    if (task_hndl_arr[0].find(TASK_HNDL_DELM) != job_hndl.size()) {
      rc = YAPP_MSG_INVALID;
    }
  }
#ifdef DEBUG_YAPP_UTIL
  std::cerr << ">>>> TASK_HNDL: " << task_hndl_arr[0] << std::endl;
  std::cerr << ">>>> JOBS_HNDL: " << job_hndl << std::endl;
#endif

  if (YAPP_MSG_SUCCESS != rc) { return rc; }

  for (int i = 0; i < task_cnt; i++) {
    if (0 == task_hndl_arr[i].find(job_hndl)) {
      rc = get_node_arr_recur_post_asc(
        queue_path + "/" + task_hndl_arr[i], nodes_arr
      );
      if (YAPP_MSG_SUCCESS != rc) { break; }
    }
  }
  if (YAPP_MSG_SUCCESS != rc) { return rc; }

  vector<string> term_jobhndl_arr;
  int term_proc_cnt = 0;
  rc = get_node_arr(get_failed_task_queue_path_prefix(), term_jobhndl_arr);
  if (YAPP_MSG_SUCCESS != rc) { return rc; }
  term_proc_cnt = term_jobhndl_arr.size();
  for (int i = 0; i < term_proc_cnt; i++) {
    if (0 == term_jobhndl_arr[i].find(job_hndl)) {
      nodes_arr.push_back(
        get_failed_task_queue_path_prefix() + "/" + term_jobhndl_arr[i]
      );
    }
  }

  term_jobhndl_arr.clear();
  rc = get_node_arr(get_terminated_queue_path_prefix(), term_jobhndl_arr);
  if (YAPP_MSG_SUCCESS != rc) { return rc; }
  term_proc_cnt = term_jobhndl_arr.size();
  for (int i = 0; i < term_proc_cnt; i++) {
    if (0 == term_jobhndl_arr[i].find(job_hndl)) {
      nodes_arr.push_back(
        get_terminated_queue_path_prefix() + "/" + term_jobhndl_arr[i]
      );
    }
  }

#ifdef DEBUG_YAPP_UTIL
  std::cerr << ">>>> Going to Delete Nodes:" << std::endl;
  for (size_t i = 0; i < nodes_arr.size(); i++) {
    std::cerr << "++++ " << nodes_arr[i] << std::endl;
  }
#endif

  if (nodes_arr.empty()) { return YAPP_MSG_INVALID; }

  return batch_delete(nodes_arr);
}

YAPP_MSG_CODE ZkClusterProxy::create_election_node(string & path_created,
                                                   string & data_str,
                                                   bool b_master_pref){
  string node_path = get_election_path() + string("/");
  if (true == b_master_pref) {
    node_path += NODE_PREFX_MASTER_PREFERRED;
  } else {
    node_path += NODE_PREFX_NORMAL_PRIORITY;
  }

  data_str = ZkClusterProxy::get_ip_addr_v4_lan();
  data_str += ELECTION_NODE_DATA_DELIM;
  ostringstream num_to_str_in;
  num_to_str_in << port_num;
  data_str += num_to_str_in.str();
  num_to_str_in.str("");
  num_to_str_in.clear();
  num_to_str_in << (b_testing ? getpid() : getpid());
  data_str += ELECTION_NODE_DATA_DELIM;
  data_str += num_to_str_in.str();

#ifdef DEBUG_YAPP_UTIL
  std::cerr << ">>>> Going to create election node: " << node_path
            << " with Value: " << data_str << std::endl;
#endif
  return create_node(
    node_path, data_str, path_created, ZOO_EPHEMERAL | ZOO_SEQUENCE
  );
}

YAPP_MSG_CODE ZkClusterProxy::get_node_arr(const string & dir_path,
                                           vector<string> & node_arr) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  if (dir_path.empty()) { return ret_code; }
  /** grab all nodes under the current folder holding the election & sort asc. */
  struct String_vector child_nodes_arr;
  child_nodes_arr.data = NULL;
  child_nodes_arr.count = 0;
  int ret_val = zoo_get_children(zk_ptr, dir_path.c_str(), 0, &child_nodes_arr);
#ifdef DEBUG_YAPP_UTIL
  std::cerr << "dir_path => " << dir_path << " : "
            << "# of nodes => " << child_nodes_arr.count << " : "
            << "ret_val => " << ret_val << std::endl;
#endif
  if (ZOK == ret_val) {
    sort_child_nodes_arr(&child_nodes_arr);
    for (int i = 0; i < child_nodes_arr.count; i++) {
      node_arr.push_back(child_nodes_arr.data[i]);
#ifdef DEBUG_YAPP_UTIL
      std::cerr << "Child Node: " << node_arr[i] << std::endl;
#endif
    }
    ret_code = YAPP_MSG_SUCCESS;
  }
  deallocate_String_vector(&child_nodes_arr);
  return ret_code;
}


YAPP_MSG_CODE ZkClusterProxy::set_node_data(const string & node_path,
                                            const string & node_data,
                                            int version) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  int ret_val = zoo_set(
    zk_ptr, node_path.c_str(), node_data.c_str(), node_data.size(), version
  );
  if (ZOK == ret_val) { ret_code = YAPP_MSG_SUCCESS; }
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::batch_set(const vector<string> & path_arr,
                                        const vector<string> & data_arr) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  if (path_arr.size() != data_arr.size()) { return ret_code; }

  /** initiate & zero out the necessary mem. **/
  int total_node_cnt = path_arr.size();
  zoo_op_t * ops_set = new zoo_op_t[total_node_cnt];
  zoo_op_result_t * ret_set = new zoo_op_result_t[total_node_cnt];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ops_set, 0, sizeof(zoo_op_t) * total_node_cnt);
  memset(ret_set, 0, sizeof(zoo_op_result_t) * total_node_cnt);

  /** generate all set of full path, add them to the operation set  **/
  int  ret_val = -1;

  for (int i = 0; i < total_node_cnt; i++) {
    zoo_set_op_init(&ops_set[i], path_arr[i].c_str(), data_arr[i].c_str(),
                    data_arr[i].size(), -1, NULL);
  }

  /** only execute set when all operations are added correctly. **/
  if (total_node_cnt > 0) {
    /** execute in batch, and we only care about if last node got created **/
    ret_val = zoo_multi(zk_ptr, total_node_cnt, &ops_set[0], &ret_set[0]);
    if ((int)ZOK == ret_val) { ret_code = YAPP_MSG_SUCCESS; }
  }

  delete[] ops_set;
  delete[] ret_set;

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::get_node_data(const string & node_path,
                                            string & data,
                                            struct Stat * stat_ptr) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  char data_buf[MAX_ELECTION_NODE_DATA_LEN] = { 0 };
  int buf_len = sizeof(data_buf);
  int ret_val = zoo_get(zk_ptr, node_path.c_str(), 0,
                        data_buf, &buf_len, stat_ptr);
#ifdef DEBUG_YAPP_UTIL
  std::cerr << "data_buff => " << data_buf << " : "
            << "node_path => " << node_path << " : "
            << "ret_val => " << ret_val << std::endl;
#endif
  if (ZOK == ret_val) {
    ret_code = YAPP_MSG_SUCCESS;
    data = data_buf;
  }
  return ret_code;
}

string ZkClusterProxy::get_election_path() {
  string election_path = root_path + (b_testing ? YAPP_ROOT_PATH_FOR_TEST : YAPP_ROOT_PATH);
  return (election_path + ELECTION_PATH);
}

zhandle_t * ZkClusterProxy::get_cur_zkptr_atomic() {
  zhandle_t * cur_ptr = NULL;
  if (0 == pthread_rwlock_rdlock(&zk_ptr_latch)) {
    cur_ptr = cur_zk_ptr;
    pthread_rwlock_unlock(&zk_ptr_latch);
  }
  return cur_ptr;
}

bool ZkClusterProxy::set_cur_zkptr_atomic(zhandle_t * new_zk_ptr) {
  bool status = false;
  if (NULL != new_zk_ptr && 0 == pthread_rwlock_wrlock(&zk_ptr_latch)) {
    cur_zk_ptr = new_zk_ptr;
    if (0 == pthread_rwlock_unlock(&zk_ptr_latch)) { status = true; }
  }
  return status;
}


void ZkClusterProxy::node_change_watcher(zhandle_t * zh, int type, int state,
                                         const char * path, void * zkcptr) {
/*
  ZkClusterProxy * zkc_proxy_ptr = ((ZkClusterProxy *)zkcptr);
  zhandle_t * zk_ptr = zkc_proxy_ptr->get_cur_zkptr_atomic();
  if(NULL == zkc_ptr || NULL == ctx) { return; }
  WatcherAction * action=(WatcherAction*)ctx;
    
    if(type==ZOO_SESSION_EVENT){
        if(state==ZOO_EXPIRED_SESSION_STATE)
            action->onSessionExpired(zh);
        else if(state==ZOO_CONNECTING_STATE)
            action->onConnectionLost(zh);
        else if(state==ZOO_CONNECTED_STATE)
            action->onConnectionEstablished(zh);
    }else if(type==ZOO_CHANGED_EVENT)
        action->onNodeValueChanged(zh,path);
    else if(type==ZOO_DELETED_EVENT)
        action->onNodeDeleted(zh,path);
    else if(type==ZOO_CHILD_EVENT)
        action->onChildChanged(zh,path);
    // TODO: implement for the rest of the event types
    // ...
    action->setWatcherTriggered();
*/
}



YAPP_MSG_CODE ZkClusterProxy::run_master_election(bool b_master_pref)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  /**
   * every running stack(per invoke) of this method will create a ephemeral
   * node with a global auto-incr sequence on zk cluster for leader election.
   * it also log down some necessary info. includes:
   * - host name, port number(reserved for communication), pid(tid when test)
   * - { host_name:port:pid(or tid) }
   */
  ret_code = create_election_node(self_node_path, self_node_valu,b_master_pref);
  if (YAPP_MSG_SUCCESS == ret_code) {
    ret_code = config_node_info();
#ifdef DEBUG_YAPP_UTIL
    if (YAPP_MSG_SUCCESS == ret_code) {
      std::cerr << ">>>> Succeeded in configuring election." << std::endl;
    }
#endif
  } else {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << ">>>> Failed in creating election node: " << self_node_path
              << " with Value: " << self_node_valu << std::endl;
#endif
  }
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::config_node_info() {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  vector<string> child_nodes_arr;
  ret_code = get_node_arr(get_election_path(), child_nodes_arr);
  if (0 >= child_nodes_arr.size()) { ret_code = YAPP_MSG_INVALID; }

  if (YAPP_MSG_SUCCESS != ret_code) { return ret_code; }

  /** for sorted node paths, pick the 1st(with min seq #) as master */
  master_node_path = get_election_path() + "/" + child_nodes_arr[0];
  string node_data_buf;
  ret_code = get_node_data(master_node_path, node_data_buf);
  if (YAPP_MSG_SUCCESS != ret_code) { return ret_code; }

  ret_code = parse_election_node_data_str(
    node_data_buf, master_host_str, master_port_str, master_proc_pid
  );
  if (YAPP_MSG_SUCCESS != ret_code) { return ret_code; }
  /**
   * if the current node is the master, then it would set a watch to current
   * folder performing the election, in case of any worker node quit or join.
   * this is acutally done by the class YappMaster.
   *
   * each worker node will listen to its predecessor, and there will a a node
   * at the beginning(a worker node with 2nd min seq id) listen to the master
   * in case of master failure.
   *
   * for dev. purpose, setting up callbacks & watches will be in diff. place.
   */
  if (self_node_path == master_node_path) {
    yapp_mode_code = YAPP_MODE_MASTER;
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "Thread: " << pthread_self()
              << " Finished Running Election as LEADER. "
              << self_node_path << std::endl;
    std::cerr.flush();
#endif
  } else {
    yapp_mode_code = YAPP_MODE_WORKER;
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "Thread: " << pthread_self()
              << " Finished Running Election as WORKER. "
              << self_node_path << std::endl;
    std::cerr.flush();
#endif
  }
  ret_code = YAPP_MSG_SUCCESS;
  return ret_code;
}

/**
 * 192.168.1.1:9527:8888 -> { "192.168.1.1", "9527", "8888" }
 */
YAPP_MSG_CODE ZkClusterProxy::parse_election_node_data_str(
    const string & data_str, string & out_host,
          string & out_port, string & out_pidn)
{
  assert(data_str.size() < (size_t)MAX_ELECTION_NODE_DATA_LEN);
  size_t port_pos = data_str.find(ELECTION_NODE_DATA_DELIM, 0) + 1;
  size_t pidn_pos = data_str.find(ELECTION_NODE_DATA_DELIM,port_pos)+1;
  assert(string::npos != port_pos &&
         string::npos != pidn_pos &&
         pidn_pos > port_pos
  );
  out_host = data_str.substr(0, port_pos - 1);
  out_port = data_str.substr(port_pos, pidn_pos - port_pos - 1);
  out_pidn = data_str.substr(pidn_pos, data_str.size() - pidn_pos);
  return YAPP_MSG_SUCCESS;
}

int ZkClusterProxy::vstrcmp(const void* str1, const void* str2) {
    const char **a = (const char**) str1;
    const char **b = (const char**) str2;
    // return strcmp(strrchr(* a, '-') + 1, strrchr(* b, '-') + 1); 
    return strcmp(* a, * b); 
} 

void ZkClusterProxy::sort_child_nodes_arr(
    struct String_vector * child_nodes_arr)
{
    qsort(
      child_nodes_arr->data, child_nodes_arr->count, sizeof(char*), &vstrcmp
    );
}

string ZkClusterProxy::get_ip_addr_v4_lan() {
  string local_ip;
  struct ifconf ifconf;
  struct ifreq ifr[MAX_ELECTION_NODE_INTERFACES_CNT];
  int fd_sock = -1;
  int if_cnt = 0;
  int ret_val = -1;

  fd_sock = socket(AF_INET, SOCK_STREAM, 0);

  assert(0 < fd_sock);
  ifconf.ifc_buf = (char *) ifr;
  ifconf.ifc_len = sizeof(ifr);
  ret_val = ioctl(fd_sock, SIOCGIFCONF, &ifconf);

  close(fd_sock);

  assert (0 == ret_val);
  if_cnt = ifconf.ifc_len / sizeof(ifr[0]);
  for (int i = 0; i < if_cnt; i++) {
    char ip[INET_ADDRSTRLEN + 1] = { 0 };
    struct sockaddr_in * s_in = (struct sockaddr_in *) &ifr[i].ifr_addr;
    assert(NULL != inet_ntop(AF_INET, &s_in->sin_addr, ip, sizeof(ip)));
    if (0 == strcmp(ELECTION_NODE_INV_IP_V4, ip)) {
      continue;
    } else {
      local_ip = ip;
      break;
    }
  }

  return local_ip;
}

YAPP_MSG_CODE ZkClusterProxy::try_acquire_read_lock(const string & fpath,
                                                          string & lock_hnd) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID_FAILED_GRAB_RLOCK;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  string lock_prfx = fpath + "/" + READ_LOCK_PREFIX_STR;
  ret_code = create_node(
    lock_prfx, "", lock_hnd, ZOO_EPHEMERAL | ZOO_SEQUENCE
  );
#ifdef DEBUG_YAPP_UTIL
  std::cerr << "==>> lock: " << lock_hnd << std::endl;
#endif
  if (YAPP_MSG_SUCCESS == ret_code) {
    vector<string> lock_arr;
    ret_code = get_node_arr(fpath, lock_arr);
    string sh_lock_key = StringUtil::get_path_basename(lock_hnd);
    string sh_lock_seq = sh_lock_key.substr(READ_LOCK_PREFIX_STR.size());
    size_t start_pos = string::npos;

    if (YAPP_MSG_SUCCESS == ret_code) {
      ret_code = YAPP_MSG_SUCCESS;
      for (size_t i = 0; i < lock_arr.size(); i++) {
        start_pos = lock_arr[i].find(WRITE_LOCK_PREFIX_STR);
#ifdef DEBUG_YAPP_UTIL
        std::cerr << "==>> child: " << lock_arr[i] << std::endl;
#endif
        if (0 == start_pos) {
          string ex_lock_seq = lock_arr[i].substr(WRITE_LOCK_PREFIX_STR.size());
          if (sh_lock_seq > ex_lock_seq) {
            ret_code = YAPP_MSG_INVALID_FAILED_GRAB_RLOCK;
            break;
          }
        }
      }
    }
  }
  if (YAPP_MSG_SUCCESS != ret_code) { release_lock(lock_hnd); }
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::try_acquire_write_lock(const string & fpath,
                                                           string & lock_hnd) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID_FAILED_GRAB_RLOCK;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  string lock_prfx = fpath + "/" + WRITE_LOCK_PREFIX_STR;
  ret_code = create_node(
    lock_prfx, "", lock_hnd, ZOO_EPHEMERAL// | ZOO_SEQUENCE
  );
  if (YAPP_MSG_SUCCESS != ret_code) {
    ret_code = YAPP_MSG_INVALID_FAILED_GRAB_WLOCK;
  }
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::acquire_read_lock(const string & fpath,
                                                      string & lock_hnd) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_SUCCESS;
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::acquire_write_lock(const string & fpath,
                                                       string & lock_hnd) {
  YAPP_MSG_CODE ret_code = YAPP_MSG_SUCCESS;
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::release_lock(const string & lock_hnd) {
  return del_node(lock_hnd);
}

YAPP_MSG_CODE ZkClusterProxy::release_lock_and_delete(const string & node_path)
{
  return delete_node_recur(node_path);
}
 
YAPP_MSG_CODE ZkClusterProxy::release_lock_and_delete(
  const vector<string> & node_path_arr)
{
  vector<string> node_to_del_arr;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  for (size_t i = 0; i < node_path_arr.size(); i++) {
    rc = get_node_arr_recur_post_asc(node_path_arr[i], node_to_del_arr);
    if (YAPP_MSG_SUCCESS != rc) { break; }
  }
  if (YAPP_MSG_SUCCESS == rc) {
    if ((int)node_to_del_arr.size() <= MAX_BATCH_CHUNK) {
      rc = batch_delete(node_to_del_arr);
    } else{
      rc = YAPP_MSG_INVALID_EXCEED_MAX_BATCH_SIZE;
    }
  }
  return rc;
}

YAPP_MSG_CODE ZkClusterProxy::create_and_acquire_ex_lock(
  const string & node_path, const string & data_str)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  /** initiate & zero out the necessary mem. **/
  zoo_op_t ops_set[2];
  zoo_op_result_t ret_set[2];
  memset(ops_set, 0, sizeof(ops_set));
  memset(ret_set, 0, sizeof(ret_set));

  /** generate all set of full path, add them to the operation set  **/
  char ret_path[MAX_PATH_LEN];
  int  ret_val = -1;

  string lock_prfx = node_path + "/" + WRITE_LOCK_PREFIX_STR;
  zoo_create_op_init(
    &ops_set[0], node_path.c_str(), data_str.c_str(), data_str.size(),
    &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
  );
  zoo_create_op_init(
    &ops_set[1], lock_prfx.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE,
    ZOO_EPHEMERAL, ret_path, sizeof(ret_path) // | ZOO_SEQUENCE
  );

  /** only execute create when all operations are added correctly. **/
  /** execute in batch, and we only care about if last node got created **/
  ret_val = zoo_multi(zk_ptr, 2, &ops_set[0], &ret_set[0]);
  if ((int)ZOK == ret_val) { ret_code = YAPP_MSG_SUCCESS; }

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::create_seq_node_and_acquire_ex_lock(
  string & path_ret, string & lock_hnd, const string & node_path,
                                        const string & data_str)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  /** initiate & zero out the necessary mem. **/
  zoo_op_t ops_set[2];
  zoo_op_result_t ret_set[2];
  memset(ops_set, 0, sizeof(ops_set));
  memset(ret_set, 0, sizeof(ret_set));

  /** generate all set of full path, add them to the operation set  **/
  char ret_path[2][MAX_PATH_LEN];
  int  ret_val = -1;
  string lock_prfx = node_path + "/" + WRITE_LOCK_PREFIX_STR;

  zoo_create_op_init(
    &ops_set[0], node_path.c_str(), data_str.c_str(), data_str.size(),
    &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE, ret_path[0], sizeof(ret_path)
  );
  zoo_create_op_init(
    &ops_set[1], lock_prfx.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE,
    ZOO_EPHEMERAL, ret_path[1], sizeof(ret_path) // | ZOO_SEQUENCE
  );

  /** only execute create when all operations are added correctly. **/
  /** execute in batch, and we only care about if last node got created **/
  ret_val = zoo_multi(zk_ptr, 2, &ops_set[0], &ret_set[0]);
  if ((int)ZOK == ret_val) {
    ret_code = YAPP_MSG_SUCCESS;
    path_ret = ret_path[0];
    lock_hnd = ret_path[1];
  }

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::create_and_acquire_ex_lock(
  const vector<string> & path_arr, const vector<string> & data_arr)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  if (path_arr.size() != data_arr.size()) { return ret_code; }

  /** initiate & zero out the necessary mem. **/
  int total_node_cnt = path_arr.size() * 2;
  zoo_op_t * ops_set = new zoo_op_t[total_node_cnt];
  zoo_op_result_t * ret_set = new zoo_op_result_t[total_node_cnt];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ops_set, 0, sizeof(zoo_op_t) * total_node_cnt);
  memset(ret_set, 0, sizeof(zoo_op_result_t) * total_node_cnt);

  /** generate all set of full path, add them to the operation set  **/
  char ret_path[MAX_PATH_LEN];
  int  ret_val = -1;

  string lock_prfx = "";
  for (int i = 0; i < total_node_cnt; i += 2) {
    zoo_create_op_init(
      &ops_set[i], path_arr[i].c_str(), data_arr[i].c_str(), data_arr[i].size(),
      &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
    );
    lock_prfx = path_arr[i] + "/" + WRITE_LOCK_PREFIX_STR;
    zoo_create_op_init(
      &ops_set[i + 1], lock_prfx.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE,
      ZOO_EPHEMERAL, ret_path, sizeof(ret_path) // | ZOO_SEQUENCE
    );
  }

  /** only execute create when all operations are added correctly. **/
  if (total_node_cnt > 0) {
    /** execute in batch, and we only care about if last node got created **/
    ret_val = zoo_multi(zk_ptr, total_node_cnt, &ops_set[0], &ret_set[0]);
    if ((int)ZOK == ret_val) { ret_code = YAPP_MSG_SUCCESS; }
  }

  delete[] ops_set;
  delete[] ret_set;

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::move_and_ex_lock_node_with_no_children(
  const string & path_from, const string & path_to,
  const string & node_data,       string & lock_hnd)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  vector<string> node_arr;
  if (YAPP_MSG_SUCCESS != get_node_arr(path_from, node_arr)) { return ret_code;}
  if (node_arr.size() > 1) { return ret_code; }
  string lock_to_del;
  if (1 == node_arr.size()) {
    if (0 != node_arr.front().find(WRITE_LOCK_PREFIX_STR)) { return ret_code; }
    lock_to_del = path_from + "/" + node_arr.front();
  }

  int op_cnt = 3;
  if (false == lock_to_del.empty()) { op_cnt++; }

  zoo_op_result_t ret_set[4];
  zoo_op_t        ops_set[4];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ret_set, 0, sizeof(ret_set));
  memset(ops_set, 0, sizeof(ops_set));

  char ret_path[MAX_PATH_LEN];
  char lock_buf[MAX_PATH_LEN];
  memset(lock_buf, 0, MAX_PATH_LEN);

  string lock_prfx = path_to + "/" + WRITE_LOCK_PREFIX_STR;

  zoo_create_op_init(
    &ops_set[0], path_to.c_str(), node_data.c_str(), node_data.size(),
    &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
  );
  zoo_create_op_init(
    &ops_set[1], lock_prfx.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE,
    ZOO_EPHEMERAL, lock_buf, sizeof(lock_buf) // | ZOO_SEQUENCE
  );
  if (4 == op_cnt) {
    zoo_delete_op_init(&ops_set[2], lock_to_del.c_str(), -1);
    zoo_delete_op_init(&ops_set[3], path_from.c_str(), -1);
  } else {
    zoo_delete_op_init(&ops_set[2], path_from.c_str(), -1);
  }
  if ((int)ZOK == zoo_multi(zk_ptr, op_cnt, &ops_set[0], &ret_set[0])) {
    lock_hnd = lock_buf;
    ret_code = YAPP_MSG_SUCCESS;
  }
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::move_node_with_no_children(
  const string & path_from, const string & path_to, const string & node_data)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  vector<string> node_arr;
  if (YAPP_MSG_SUCCESS != get_node_arr(path_from, node_arr)) { return ret_code;}
  if (node_arr.size() > 1) { return ret_code; }
  string lock_to_del;
  if (1 == node_arr.size()) {
    if (0 != node_arr.front().find(WRITE_LOCK_PREFIX_STR)) { return ret_code; }
    lock_to_del = path_from + "/" + node_arr.front();
  }

  int op_cnt = 2;
  if (false == lock_to_del.empty()) { op_cnt++; }

  zoo_op_result_t ret_set[3];
  zoo_op_t        ops_set[3];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ret_set, 0, sizeof(ret_set));
  memset(ops_set, 0, sizeof(ops_set));

  char ret_path[MAX_PATH_LEN];

  zoo_create_op_init(
    &ops_set[0], path_to.c_str(), node_data.c_str(), node_data.size(),
    &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
  );

  if (3 == op_cnt) {
    zoo_delete_op_init(&ops_set[1], lock_to_del.c_str(), -1);
    zoo_delete_op_init(&ops_set[2], path_from.c_str(), -1);
  } else {
    zoo_delete_op_init(&ops_set[1], path_from.c_str(), -1);
  }
  if ((int)ZOK == zoo_multi(zk_ptr, op_cnt, ops_set, ret_set)) {
    ret_code = YAPP_MSG_SUCCESS;
  }

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::move_node_with_no_children(
  const string & path_from, const string & path_to,
  const string & data,      const string & lock_to_del)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  if (path_from.empty() || path_to.empty()) { return ret_code; }

  int op_cnt = 2;
  if (false == lock_to_del.empty()) { op_cnt++; }

  zoo_op_result_t ret_set[3];
  zoo_op_t        ops_set[3];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ret_set, 0, sizeof(ret_set));
  memset(ops_set, 0, sizeof(ops_set));

  char ret_path[MAX_PATH_LEN];

  zoo_create_op_init(
    &ops_set[0], path_to.c_str(), data.c_str(), data.size(),
    &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
  );
  if (3 == op_cnt) {
    zoo_delete_op_init(&ops_set[1], lock_to_del.c_str(), -1);
    zoo_delete_op_init(&ops_set[2], path_from.c_str(), -1);
  } else {
    zoo_delete_op_init(&ops_set[1], path_from.c_str(), -1);
  }
  if ((int)ZOK == zoo_multi(zk_ptr, op_cnt, ops_set, ret_set)) {
    ret_code = YAPP_MSG_SUCCESS;
  }

  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::batch_move_node_with_no_children(
  const vector<string> & path_from_arr, const vector<string> & path_to_arr,
  const vector<string> & data_arr)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  if (!((path_from_arr.size() == path_to_arr.size()) &&
        (path_to_arr.size()   == data_arr.size()))) { return ret_code; }

  int total_node_cnt = path_from_arr.size() * 2;
  int loop_cnt = path_from_arr.size();
  zoo_op_t * ops_set = new zoo_op_t[total_node_cnt];
  zoo_op_result_t * ret_set = new zoo_op_result_t[total_node_cnt];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ops_set, 0, sizeof(zoo_op_t) * total_node_cnt);
  memset(ret_set, 0, sizeof(zoo_op_result_t) * total_node_cnt);
  char ret_path[MAX_PATH_LEN];

  for (int i = 0; i < loop_cnt; i++) {
    zoo_create_op_init(
      &ops_set[i], path_to_arr[i].c_str(), data_arr[i].c_str(),
      data_arr[i].size(), &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
    );
    zoo_delete_op_init(&ops_set[i + 1], path_from_arr[i].c_str(), -1);
  }

  if ((int)ZOK == zoo_multi(zk_ptr, total_node_cnt, ops_set, ret_set)) {
    ret_code = YAPP_MSG_SUCCESS;
  }

  delete[] ops_set;
  delete[] ret_set;
  return ret_code;
}


YAPP_MSG_CODE ZkClusterProxy::batch_set_create_and_delete(
  const vector<string> & upd_path_arr, const vector<string> & upd_data_arr,
  const vector<string> & path_to_cr_arr, const vector<string> & data_to_cr_arr,
  const vector<string> & path_to_del)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  if ((upd_path_arr.size()   != upd_data_arr.size()) ||
      (path_to_cr_arr.size() != data_to_cr_arr.size())
  ) { return ret_code; }

  int upd_cnt = upd_path_arr.size();
  int cre_cnt = path_to_cr_arr.size();
  int del_cnt = path_to_del.size();
  int total_node_cnt = upd_cnt + cre_cnt + del_cnt;

  zoo_op_t * ops_set = new zoo_op_t[total_node_cnt];
  zoo_op_result_t * ret_set = new zoo_op_result_t[total_node_cnt];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ops_set, 0, sizeof(zoo_op_t) * total_node_cnt);
  memset(ret_set, 0, sizeof(zoo_op_result_t) * total_node_cnt);
  char ret_path[MAX_PATH_LEN];

  int base = 0;
  for (int i = 0; i < upd_cnt; i++) {
    zoo_set_op_init(&ops_set[i], upd_path_arr[i].c_str(), upd_data_arr[i].c_str(),
                    upd_data_arr[i].size(), -1, NULL);
  }

  base += upd_cnt;
  for (int i = 0; i < cre_cnt; i++) {
    zoo_create_op_init(
      &ops_set[i + base], path_to_cr_arr[i].c_str(), data_to_cr_arr[i].c_str(),
      data_to_cr_arr[i].size(), &ZOO_OPEN_ACL_UNSAFE, 0, ret_path,
      sizeof(ret_path)
    );
  }

  base += cre_cnt;
  for (int i = 0; i < del_cnt; i++) {
    zoo_delete_op_init(&ops_set[i + base], path_to_del[i].c_str(), -1);
  }

  int rv = zoo_multi(zk_ptr, total_node_cnt, ops_set, ret_set);
  if ((int)ZOK == rv) {
    ret_code = YAPP_MSG_SUCCESS;
  }

  delete[] ops_set;
  delete[] ret_set;
  return ret_code;

}



YAPP_MSG_CODE ZkClusterProxy::batch_move_node_with_no_children_and_create_del(
  const vector<string> & path_from_arr, const vector<string> & path_to_arr,
  const vector<string> & data_arr,      const vector<string> & path_to_cr_arr,
  const vector<string> & data_to_cr_arr,const vector<string> & path_to_del)
{
  YAPP_MSG_CODE ret_code = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);

  if (!((path_from_arr.size() == path_to_arr.size()) &&
        (path_to_arr.size()   == data_arr.size())) ||
      !(path_to_cr_arr.size() == data_to_cr_arr.size())) { return ret_code; }

  int mov_cnt = path_from_arr.size();
  int cre_cnt = path_to_cr_arr.size();
  int del_cnt = path_to_del.size();
  int total_node_cnt = mov_cnt * 2 + cre_cnt + del_cnt;

  zoo_op_t * ops_set = new zoo_op_t[total_node_cnt];
  zoo_op_result_t * ret_set = new zoo_op_result_t[total_node_cnt];
  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ops_set, 0, sizeof(zoo_op_t) * total_node_cnt);
  memset(ret_set, 0, sizeof(zoo_op_result_t) * total_node_cnt);
  char ret_path[MAX_PATH_LEN];

  int base = 0;

  for (int i = 0; i < cre_cnt; i++) {
    zoo_create_op_init(
      &ops_set[i], path_to_cr_arr[i].c_str(), data_to_cr_arr[i].c_str(),
      data_to_cr_arr[i].size(), &ZOO_OPEN_ACL_UNSAFE, 0, ret_path,
      sizeof(ret_path)
    );
  }

  base += cre_cnt;
  int tot_mov_op_cnt = mov_cnt * 2;
  for (int i = 0; i < tot_mov_op_cnt; i += 2) {
    zoo_create_op_init(
      &ops_set[i + base], path_to_arr[i / 2].c_str(), data_arr[i / 2].c_str(),
      data_arr[i / 2].size(), &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
    );
    zoo_delete_op_init(&ops_set[i + base + 1], path_from_arr[i / 2].c_str(), -1);
  }

  base += tot_mov_op_cnt;
  for (int i = 0; i < del_cnt; i++) {
    zoo_delete_op_init(&ops_set[i + base], path_to_del[i].c_str(), -1);
  }

  int rv = zoo_multi(zk_ptr, total_node_cnt, ops_set, ret_set);
  if ((int)ZOK == rv) {
    ret_code = YAPP_MSG_SUCCESS;
  }

  delete[] ops_set;
  delete[] ret_set;
  return ret_code;
}

YAPP_MSG_CODE ZkClusterProxy::batch_unlock_delete_and_create(
  const vector<string> & nodes_to_del, const vector<string> & nodes_to_cr)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  vector<string> path_to_del;
  int nodes_cnt = nodes_to_del.size();

  vector<string> node_arr;
  int lock_cnt = 0;
  for (int i = 0; i < nodes_cnt; i++) {
    rc = get_node_arr(nodes_to_del[i], node_arr);
    if (YAPP_MSG_SUCCESS != rc) { break; }
#ifdef DEBUG_YAPP_UTIL
    if (1 < node_arr.size()) {
      std::cerr << ">>>> LOCK UNDER: " << nodes_to_del[i] << std::endl;
      for (size_t c = 0; c < node_arr.size(); c++) {
        std::cerr << ">>>> LOCK FOUND : " << node_arr[c] << std::endl;
      }
    }
#endif
    lock_cnt = node_arr.size();
    for (int c = 0; c < lock_cnt; c++) {
//    if (1 == node_arr.size()) {
      if (0 == node_arr.front().find(WRITE_LOCK_PREFIX_STR)) {
        path_to_del.push_back(nodes_to_del[i] + "/" + node_arr[c]);
      } else {
        rc = YAPP_MSG_INVALID;
        break;
      }
    }
    path_to_del.push_back(nodes_to_del[i]);
    node_arr.clear();
  }
  if (YAPP_MSG_SUCCESS != rc) { return rc; }

  int cre_cnt = nodes_to_cr.size();
  int del_cnt = path_to_del.size();
  int total_node_cnt = cre_cnt + del_cnt;

  zoo_op_result_t * ret_set = new zoo_op_result_t[total_node_cnt];
  zoo_op_t *        ops_set = new zoo_op_t[total_node_cnt];

  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ops_set, 0, sizeof(zoo_op_t) * total_node_cnt);
  memset(ret_set, 0, sizeof(zoo_op_result_t) * total_node_cnt);

  char ret_path[MAX_PATH_LEN];

  for (int i = 0; i < cre_cnt; i++) {
    zoo_create_op_init(
      &ops_set[i], nodes_to_cr[i].c_str(), "", 0,
      &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
    );
#ifdef DEBUG_YAPP_UTIL
    std::cerr << ">>>> NODE TO CRET: " << nodes_to_cr[i] << std::endl;
#endif
  }

  for (int i = 0; i < del_cnt; i++) {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << ">>>> NODE TO UDEL: " << path_to_del[i] << std::endl;
#endif
    zoo_delete_op_init(&ops_set[i + cre_cnt], path_to_del[i].c_str(), -1);
  }

  rc = YAPP_MSG_INVALID;

  if ((int)ZOK == zoo_multi(zk_ptr, total_node_cnt, ops_set, ret_set)) {
    rc = YAPP_MSG_SUCCESS;
  }
 
  delete[] ops_set;
  delete[] ret_set;
  return rc;
}

YAPP_MSG_CODE ZkClusterProxy::batch_unlock_delete_and_create_and_set(
  const vector<string> & nodes_to_del, const vector<string> & nodes_to_cr,
  const vector<string> & nodes_to_set, const vector<string> & data_to_set)
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  zhandle_t * zk_ptr = get_cur_zkptr_atomic();
  assert(NULL != zk_ptr);
  vector<string> path_to_del;
  int nodes_cnt = nodes_to_del.size();

  vector<string> node_arr;
  int lock_cnt = 0;
  for (int i = 0; i < nodes_cnt; i++) {
    rc = get_node_arr(nodes_to_del[i], node_arr);
    if (YAPP_MSG_SUCCESS != rc) { break; }
#ifdef DEBUG_YAPP_UTIL
    if (1 < node_arr.size()) {
      std::cerr << ">>>> LOCK UNDER: " << nodes_to_del[i] << std::endl;
      for (size_t c = 0; c < node_arr.size(); c++) {
        std::cerr << ">>>> LOCK FOUND : " << node_arr[c] << std::endl;
      }
    }
#endif
    lock_cnt = node_arr.size();
    for (int c = 0; c < lock_cnt; c++) {
      if (0 == node_arr.front().find(WRITE_LOCK_PREFIX_STR)) {
        path_to_del.push_back(nodes_to_del[i] + "/" + node_arr[c]);
      } else {
        rc = YAPP_MSG_INVALID;
        break;
      }
    }
    path_to_del.push_back(nodes_to_del[i]);
    node_arr.clear();
  }
  if (YAPP_MSG_SUCCESS != rc) { return rc; }

  int cre_cnt = nodes_to_cr.size();
  int del_cnt = path_to_del.size();
  int set_cnt = nodes_to_set.size();
  int total_node_cnt = cre_cnt + del_cnt + set_cnt;

  zoo_op_result_t * ret_set = new zoo_op_result_t[total_node_cnt];
  zoo_op_t *        ops_set = new zoo_op_t[total_node_cnt];

  assert(NULL != ops_set);
  assert(NULL != ret_set);
  memset(ops_set, 0, sizeof(zoo_op_t) * total_node_cnt);
  memset(ret_set, 0, sizeof(zoo_op_result_t) * total_node_cnt);

  char ret_path[MAX_PATH_LEN];

  for (int i = 0; i < cre_cnt; i++) {
    zoo_create_op_init(
      &ops_set[i], nodes_to_cr[i].c_str(), "", 0,
      &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
    );
#ifdef DEBUG_YAPP_UTIL
    std::cerr << ">>>> NODE TO CRET: " << nodes_to_cr[i] << std::endl;
#endif
  }

  for (int i = 0; i < del_cnt; i++) {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << ">>>> NODE TO UDEL: " << path_to_del[i] << std::endl;
#endif
    zoo_delete_op_init(&ops_set[i + cre_cnt], path_to_del[i].c_str(), -1);
  }

  for (int i = 0; i < set_cnt; i++) {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << ">>>> NODE TO SET: " << nodes_to_set[i] << std::endl;
#endif
    zoo_set_op_init(
      &ops_set[i + cre_cnt + del_cnt], nodes_to_set[i].c_str(),
      data_to_set[i].c_str(), data_to_set[i].size(), -1, NULL
    );
  }
  rc = YAPP_MSG_INVALID;

  if ((int)ZOK == zoo_multi(zk_ptr, total_node_cnt, ops_set, ret_set)) {
    rc = YAPP_MSG_SUCCESS;
  }

  delete[] ops_set;
  delete[] ret_set;
  return rc;

}

bool ConfigureUtil::load_zk_cluster_cfg(const string & file_str) {
  string file_path(file_str);

  StringUtil::trim_string(file_path);
  assert(file_path.size() > 0);

  ifstream fin;
  fin.open(file_path.c_str(), ios::in);
  if (false == fin.is_open()) { return false; }

#ifdef DEBUG_YAPP_UTIL
  std::cerr << "file: " << file_str << std::endl;
#endif

  zk_srv_host_arr.clear();

  string buffer = "";
  buffer.reserve(256);

  /**
   * The contents of the configuration file may like the following:
   *
   * - zkserver.1=192.168.1.1:2181
   *   zkserver.2=192.168.1.2:2181
   *   zkserver.3=192.168.1.3:2181
   */
  size_t kv_delim_pos = -1;
  size_t zk_delim_pos = -1;
  string zk_srv_host_str;
  string zk_srv_port_str;

  int opt_cnt = YAPP_DAEMON_CFG_FILE_OPTION_COUNT;
  int opt_int = 0;
  string opt_str;
  bool b_opt_found = false;
  while (std::getline(fin, buffer)) {
    StringUtil::trim_string(buffer);
    if (buffer.length() < 1) { continue; }
    if (CONFIG_COMMENTS_PREFIX == buffer[0]) { continue; }
    /** lines not key=val fashion would be skipped. **/
    kv_delim_pos = buffer.find(CONFIG_KEY_VALUE_DELIM, 0) + 1;
    if (string::npos == kv_delim_pos) { continue; }
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "line: " << buffer << std::endl;
#endif
    b_opt_found = false;
    /** trying to match with one of current cfg option. **/
    for (int i = 0; i < opt_cnt; i++) {
      if (0 == buffer.find(YAPP_DAEMON_CFG_FILE_OPTION_ENTRY[i])) {
        b_opt_found = true;
      } else {
        continue;
      }
      opt_str = buffer.substr(strlen(YAPP_DAEMON_CFG_FILE_OPTION_ENTRY[i]));
      opt_int = atol(opt_str.c_str()); 
      switch(i) {
        case YAPP_DAEMON_CFG_FILE_OPTION_PORT: {
          port_num = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_THRD_POOL_SIZE: {
          thrd_pool_size = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_MAX_QUEUED_TSK: {
          max_queued_task = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_MAX_JOB_SPLIT: {
          max_auto_job_split = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_REF_SCHEDULE_RATE: {
          rftask_scheule_polling_rate_sec = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_TSK_SCHEDULE_RATE: {
          subtask_scheule_polling_rate_sec = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_ZBM_CHECKING_RATE: {
          zombie_check_polling_rate_sec = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_REF_AUTOSPLT_RATE: {
          rftask_autosp_polling_rate_sec = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_UTL_CHECKING_RATE: {
          utility_thread_checking_rate_sec = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_MAS_CHECKING_RATE: {
          master_check_polling_rate_sec = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_LOG_CHECKING_RATE: {
          check_point_polling_rate_sec = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_BATCH_SCHEDULE_LIMIT: {
          batch_task_schedule_limit = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_FENCING_SCRIPT_PATH: {
          fencing_script_path = opt_str; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_LOG_STDOUT: {
          yappd_log_stdout = opt_str; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_LOG_STDERR: {
          yappd_log_stderr = opt_str; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_TMP_FOLDER: {
          yappd_tmp_folder = opt_str; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_ROOT_PATH: {
          yappd_root_path = opt_str; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_PID_FILE: {
          yappd_pid_file = opt_str; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_RUN_MODE: {
          if (opt_str == "master") { yapp_mode_code = YAPP_MODE_MASTER; } 
          if (opt_str == "worker") { yapp_mode_code = YAPP_MODE_WORKER; } 
          break; 
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_ZKC_TIMEOUT: {
          max_zkc_timeout = opt_int; break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_VERBOSE: {
          if (opt_str == "true")  { b_verbose = true;  } 
          if (opt_str == "false") { b_verbose = false; } 
          break;
        }
        case YAPP_DAEMON_CFG_FILE_OPTION_ZKC_SERV: {
          zk_delim_pos = buffer.find(ZK_SRV_HOST_PORT_DELIM, 0) + 1;
          if (string::npos == zk_delim_pos) { continue; }
          zk_srv_host_str = buffer.substr(
            kv_delim_pos, zk_delim_pos - kv_delim_pos - 1
          );
          zk_srv_port_str = buffer.substr(
            zk_delim_pos, buffer.size() - zk_delim_pos
          );
          if (false == StringUtil::is_integer(zk_srv_port_str)) { continue; }
          zk_srv_host_arr.push_back(ZkServer(zk_srv_host_str, zk_srv_port_str));
          break;
        }
        default: break;
      }
      if (true == b_opt_found) { break; }
    }
  }
  zk_conn_str = generate_zk_cluster_conn_str();
  fin.close();
  return true;
}

string ConfigureUtil::get_zk_cluster_conn_str() {
  return zk_conn_str;
}

string ConfigureUtil::generate_zk_cluster_conn_str() {
  string conn_str;
  int server_cnt = zk_srv_host_arr.size();
  for (int i = 0; i < server_cnt; i++) {
    conn_str += zk_srv_host_arr[i].get_connection_str();
    if ((server_cnt - 1) != i) {
      conn_str += ZK_SRV_DELIM;
    }
  }
  return conn_str;
}

bool ConfigureUtil::split_range_by_fixed_step(int id_from, int id_to, int step,
                                              vector<RangePair> & pair_arr_ret)
{
  bool status = false;
  if (id_from > id_to || step <= 0) { return status; }
  int cur_idx = id_from;
  while (cur_idx <= id_to) {
    pair_arr_ret.push_back(
      RangePair(
        cur_idx, ((id_to >= (cur_idx + step - 1)) ? (cur_idx + step - 1) : id_to)
      )
    );
    cur_idx += step;
  }
  if (pair_arr_ret.size() >= 1) { status = true; }
  return status;
}



bool StringUtil::is_integer(const std::string & str) {
  if (str.empty()) { return false; }
  int len = str.size();
  for (int i = 0; i < len; i++) {
    if ('0' > str[i] || '9' < str[i]) {
      return false;
    }
  }
  return true;
}

void StringUtil::trim_string(std::string & str) {
  int len = str.size(), start_pos = 0, end_pos = str.size() - 1;
  for (; start_pos < len; start_pos++) {
    if (' ' != str[start_pos]  && '\t' != str[start_pos] &&
      '\r' != str[start_pos] && '\n' != str[start_pos]) {
      break;
    }
  }
  if (start_pos == len) { str = ""; return; }
  for (; end_pos > 0; end_pos--) {
    if (' ' != str[end_pos]  && '\t' != str[end_pos] &&
      '\r' != str[end_pos] && '\n' != str[end_pos]) {
      break;
    }
  }
  if (end_pos >= start_pos) {
    str = str.substr(start_pos, end_pos - start_pos + 1);
  } else {
    str = "";
  }
}

bool StringUtil::parse_full_zk_node_path(const string & fpath,
                                         vector<string> & ret_dir_arr,
                                         string & ret_node_name)
{
#ifdef DEBUG_YAPP_UTIL
  std::cout << "-- parsing " << fpath << std::endl;
#endif

  bool status = true;
  string path_str(fpath);
  trim_string(path_str);
  string dirs_str(path_str);
  string node_str(path_str);

  ret_dir_arr.clear();
  ret_node_name.clear();

  if (false == path_str.empty() && '/' == path_str[0] &&
      std::string::npos == path_str.find(".") &&
      std::string::npos == path_str.find("\\")) {
    do {
      node_str = get_path_basename(dirs_str);
      dirs_str = get_path_dirname (dirs_str);
      /** boundary condition for input "/", -> { [], '' } **/
      if (ret_node_name.empty() && "/" != node_str) {
        ret_node_name = node_str;
      } else {
        ret_dir_arr.push_back(node_str);
      }
#ifdef DEBUG_YAPP_UTIL
      std::cout << "###############:" << node_str << std::endl;
#endif
      if (std::string::npos != node_str.find("/") || "zookeeper" == node_str) {
        status = false;
        break;
      }
      trim_string(dirs_str);
#ifdef DEBUG_YAPP_UTIL
      std::cout << "-- dirs_str:" << dirs_str << " "
                << "-- node_str:" << node_str << std::endl;
#endif
    } while ("/" != dirs_str);
    if (true == status) {
      std::reverse(ret_dir_arr.begin(), ret_dir_arr.end());
    } else {
      ret_dir_arr.clear();
      ret_node_name.clear();
    }
  } else {
    status = false;
  }

#ifdef DEBUG_YAPP_UTIL
  std::cout << "-- node: " << ret_node_name << std::endl;
  std::cout << "-- path: ";
  for (unsigned i = 0; i < ret_dir_arr.size(); i++) {
    std::cout << ret_dir_arr[i] << "|";
  }
  std::cout << std::endl;
#endif
  return status;
}

string StringUtil::get_path_dirname(const string & trimed_path) {
  string dir;
  if (0 < trimed_path.size()) {
    size_t pos = trimed_path.find_last_of("/");
    if (std::string::npos == pos) {
      dir = ".";
    } else {
      /** boundary condition for input "/", -> { [], '' } **/
      if (0 == pos) { pos += 1; }
      dir = trimed_path.substr(0, pos);
      trim_string(dir);
    }
  }
  return dir;
}

string StringUtil::get_path_basename (const string & trimed_path) {
  string base;
  if (0 < trimed_path.size()) {
    size_t pos = trimed_path.find_last_of("/");
    if (std::string::npos == pos) {
      base = trimed_path;
    } else if (pos < trimed_path.size() - 1) {
      /* only call the substr when there is any char left. **/
      base = trimed_path.substr(pos + 1);
      trim_string(base);
    }
  }
  return base;
}


string StringUtil::convert_int_to_str(long long a) {
  std::ostringstream num_to_str_in;
  num_to_str_in << a;
  return num_to_str_in.str();

}

string IPC_CONTEX::YAPPD_SHARED_CONDV_NAME;
string IPC_CONTEX::YAPPD_SHARED_MUTEX_NAME;

void IPC_CONTEX::init_ipc_contex() {
  YAPPD_SHARED_CONDV_NAME = string("yappd_shared_condv_handle");
  YAPPD_SHARED_MUTEX_NAME = string("yappd_shared_mutex_handle");
}
