#include "./yapp_admin.h"

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/server/TThreadPoolServer.h>

using namespace apache::thrift;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

using namespace yapp::admin;

YappAdmin::YappAdmin(vector<string> args, bool b_verb, bool b_test) :
    YappBase(args, b_verb, b_test)
{
  zkc_proxy_ptr  = NULL;
  yapp_admin_opt_idx = YAPP_ADMIN_OPT_INVALID;
}

YappAdmin::~YappAdmin() {
  delete zkc_proxy_ptr;
}

void YappAdmin::usage() {
  std::cerr << "Usage When Run Yapp in Admin Mode(Choose One Each time):"
            << std::endl
            << std::endl
            << "ypadmin --qstat | --print (${job_hndl}|${task_hndl}|${proc_hndl})*"
            << std::endl
            << "        --pause | --resume (${job_hndl}|${task_hndl}|${proc_hndl})*"
            << std::endl
            << "        --restart-failed | --restart-full | --restart-continue (${job_hndl}|${task_hndl}|${proc_hndl})*"
            << std::endl
            << "        --kill | --purge (${job_hndl})* --> ONLY WORKS WHEN ENTIRE JOB TERMINATED."
            << std::endl
            << "        --list-jobs | --list-job-by-owner (${owner})*"
            << std::endl
            << "        --list-host | --list-job-by-host  (${host})*"
            << std::endl
            << "        --list-envs"
            << std::endl
            << "Note: [] --> optional, | --> OR, * --> any number of"
            << std::endl;
}

void YappAdmin::print_warnings_and_getopt(const string & cust_info, string & opt) {
  std::cerr << std::endl
            << "You Are Trying to Use the TRUE Admin Mode, And This Opertaion Will:"
            << std::endl
            << std::endl
            << "     - " << cust_info << std::endl
            << std::endl
            << "METADATA On Zookeeper May Change And You Clearly KNOW WHAT YOU ARE DOING, "
            << "AND NOTHING COULD BE ROLLBACK!"
            << std::endl
            << std::endl
            << "You Sure You Want to Continue? [Y/N]_\b";
  std::getline(std::cin, opt);
}

YAPP_MSG_CODE YappAdmin::parse_arguments()
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID_ARGS;

  int argc = arg_arr.size();
  bool b_arg_valid = false;

  size_t kv_delim_pos = -1;
  string key_str;
  string val_str;

  for (int i = 0; i < argc; i++) {
    key_str = arg_arr[i];
    /** 1st check to see if any connection info. was provided or not **/
    if (0 == i) {
      kv_delim_pos = arg_arr[i].find(KEY_VALUE_DELIM, 0);
      if (string::npos != kv_delim_pos) {
        key_str = arg_arr[i].substr(0, kv_delim_pos + 1);
        val_str = arg_arr[i].substr(kv_delim_pos + 1);
        if (key_str == YAPP_ADMIN_OPTION_ENTRY[YAPP_ADMIN_OPT_CONN_STR]) {
          zk_conn_str = val_str;
          continue;
        } else if (key_str == YAPP_ADMIN_OPTION_ENTRY[YAPP_ADMIN_OPT_ZKCFG]) {
          if (true == cfg_obj.load_zk_cluster_cfg(val_str)) {
            zk_conn_str = cfg_obj.get_zk_cluster_conn_str();
            continue;
          }
        } else { break; }
      }
    }
    /** 2nd, trying to get the actual opt, query, print, kill/restart ... **/
    if (0 == i || (1 == i && false == val_str.empty())) {
      for (int c = 0; c < YAPP_ADMIN_OPT_COUNT - 2; c++) {
        if (key_str != YAPP_ADMIN_OPTION_ENTRY[c]) {
          continue;
        }
        b_arg_valid = true;
        yapp_admin_opt_idx = (YAPP_ADMIN_OPTION_ENTRY_MAPPING)c;
        switch ((YAPP_ADMIN_OPTION_ENTRY_MAPPING)c) {
        case YAPP_ADMIN_OPT_QSTAT:
        case YAPP_ADMIN_OPT_PRINT:
        case YAPP_ADMIN_OPT_KILL:
        case YAPP_ADMIN_OPT_RESTART_FAILED:
        case YAPP_ADMIN_OPT_RESTART_FULL:
        case YAPP_ADMIN_OPT_RESTART_CONTINUE:
        case YAPP_ADMIN_OPT_PAUSE:
        case YAPP_ADMIN_OPT_RESUME:
        case YAPP_ADMIN_OPT_PURGE:

        case YAPP_ADMIN_OPT_LIST_JOB:
        case YAPP_ADMIN_OPT_LIST_JOB_BY_OWNER:
        case YAPP_ADMIN_OPT_LIST_JOB_BY_HOST:
        case YAPP_ADMIN_OPT_LIST_HOST:
        case YAPP_ADMIN_OPT_LIST_ENVS:

        case YAPP_ADMIN_OPT_INIT:
        case YAPP_ADMIN_OPT_LS:
        case YAPP_ADMIN_OPT_TREE:
        case YAPP_ADMIN_OPT_GET:
        case YAPP_ADMIN_OPT_SET:
        case YAPP_ADMIN_OPT_RM:
        case YAPP_ADMIN_OPT_RMTREE:
        case YAPP_ADMIN_OPT_CREATE:

#ifdef DEBUG_YAPP_ADMIN
          std::cerr << "argument: " << key_str << ":" << val_str << std::endl;
#endif
          b_arg_valid = true;
          break;
        default:
#ifdef DEBUG_YAPP_ADMIN
          std::cerr << "inv. argument: " << key_str << ":" << val_str << std::endl;
#endif
          b_arg_valid = false;
          break;
        } /* switch ((YAPP_ADMIN_OPTION_ENTRY_MAPPING)c) */
      } /* for (int c = 0; c < YAPP_ADMIN_OPT_COUNT - 2; c++) */
      if (true == b_arg_valid) { continue; } else { break; }
    } /* if (0 == i || 1 == i) */

    /** 3rd, all remaining params would be treated as array of task hndle. **/
    task_hndl_arr.push_back(arg_arr[i]);

  } /* for (int i = 0; i < argc; i++) */
  if (true == b_arg_valid) { rc = YAPP_MSG_SUCCESS; }
  return rc;
};

YAPP_MSG_CODE YappAdmin::init_zk_cluster_conn_by_str() {
  YAPP_MSG_CODE ret_val = YAPP_MSG_INVALID;
  if (false == zk_conn_str.empty()) {
    zkc_proxy_ptr = new ZkClusterProxy(
      b_testing, cfg_obj.port_num, cfg_obj.max_queued_task,
      cfg_obj.max_zkc_timeout, cfg_obj.yappd_root_path
    );
    assert(NULL != zkc_proxy_ptr);
    //zkc_proxy_ptr->set_zk_namespace(cfg_obj.yappd_root_path);
    ret_val = zkc_proxy_ptr->init_zk_conn(zk_conn_str);
  }
  return ret_val;
}

YAPP_MSG_CODE YappAdmin::run() {
  if (true == b_verbose) {
    std::cout << "-- running yapp at admin mode" << std::endl;
  }
  cfg_obj.load_zk_cluster_cfg(def_cfg_fp);
  zk_conn_str = cfg_obj.get_zk_cluster_conn_str();
  YAPP_MSG_CODE ret_val = parse_arguments();
  if (YAPP_MSG_SUCCESS != ret_val) {
    usage();
    return ret_val;
  }

  assert(YAPP_MSG_SUCCESS == init_zk_cluster_conn_by_str());

  string opt;
  vector<string> ret_str_arr;
  vector<bool>   ret_bln_arr;
    
  switch(yapp_admin_opt_idx) {
    case YAPP_ADMIN_OPT_INIT:
      print_warnings_and_getopt(
        "Clear ALL Existing Meta Data And RE-SET Everything in Zookeeper Service Within Namespace: " +
          (cfg_obj.yappd_root_path.empty() ?  SYS_ROOT_PATH : cfg_obj.yappd_root_path), opt
      );
      if (true == is_opt_chosen(opt)) {
        init_yapp_service_env(ret_str_arr);
        print_init_yapp_service_env_result(ret_str_arr);
      }
      break;
    case YAPP_ADMIN_OPT_LS:
      print_warnings_and_getopt(
        "List All Meta Data Under A Given Path in Zookeeper Service", opt
      );
      if (true == is_opt_chosen(opt)) {
        if (false == task_hndl_arr.empty()) {
          ls_child_node_arr(task_hndl_arr.front(), ret_str_arr);
        }
        print_ls_child_node_arr(ret_str_arr);
      }
      break;

    case YAPP_ADMIN_OPT_TREE:
      print_warnings_and_getopt(
        "Recursively List All Meta Data Under A Given Path in Zookeeper Service", opt
      );
      if (true == is_opt_chosen(opt)) {
        if (false == task_hndl_arr.empty()) {
          tree_child_node_arr(task_hndl_arr.front(), ret_str_arr);
        }
        print_tree_child_node_arr(ret_str_arr);
      }
      break;

    case YAPP_ADMIN_OPT_GET:
      print_warnings_and_getopt(
        "Print The Value Associated With A Given Node in Zookeeper Service", opt
      );
      if (true == is_opt_chosen(opt)) {
        if (false == task_hndl_arr.empty()) {
          get_node_val(task_hndl_arr.front(), ret_str_arr);
        }
        print_get_node_val(ret_str_arr);
      }
      break;

    case YAPP_ADMIN_OPT_SET:
      print_warnings_and_getopt(
        "Set Key-Value Pair For A Existing Node(ONLY UPDATE NODE If EXISTED) in Zookeeper Service", opt
      );
      if (true == is_opt_chosen(opt)) {
        if (false == task_hndl_arr.empty()) {
          string data_str = "";
          if (task_hndl_arr.size() > 1) { data_str = task_hndl_arr[1]; }
          set_node(task_hndl_arr.front(), data_str, ret_str_arr);
        }
        print_set_node(ret_str_arr);
      }
      break;

    case YAPP_ADMIN_OPT_RM:
      print_warnings_and_getopt(
        "Delete A Given Node(With No Children) in Zookeeper Service", opt
      );
      if (true == is_opt_chosen(opt)) {
        if (false == task_hndl_arr.empty()) {
          rm_node(task_hndl_arr.front(), ret_str_arr);
        }
        print_rm_node(ret_str_arr);
      }
      break;

    case YAPP_ADMIN_OPT_RMTREE:
      print_warnings_and_getopt(
        "Recursively Delete The Whole Tree Under A Given Path in Zookeeper Service", opt
      );
      if (true == is_opt_chosen(opt)) {
        if (false == task_hndl_arr.empty()) {
          rm_node_tree(task_hndl_arr.front(), ret_str_arr);
        }
        print_rm_node_tree(ret_str_arr);
      }
      break;

    case YAPP_ADMIN_OPT_CREATE:
      print_warnings_and_getopt(
        "Create Key-Value Pair As A Node(Path Got Created If NOT EXISTED) in Zookeeper Service", opt
      );
      if (true == is_opt_chosen(opt)) {
        if (false == task_hndl_arr.empty()) {
          string data_str = "";
          if (task_hndl_arr.size() > 1) { data_str = task_hndl_arr[1]; }
          create_node(task_hndl_arr.front(), data_str, ret_str_arr);
        }
        print_create_node(ret_str_arr);
      }

      break;
    default: {
      string master_host, master_port, master_pcid;

      zkc_proxy_ptr->get_yapp_master_addr(master_host, master_port, master_pcid);

      TSocket * socket_ptr = new TSocket(master_host, atol(master_port.c_str()));
      assert(NULL != socket_ptr);
      socket_ptr->setConnTimeout(cfg_obj.max_zkc_timeout);

      shared_ptr<TTransport> socket(socket_ptr);
      shared_ptr<TTransport> transport(new TBufferedTransport(socket));
      shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
      YappServiceClient client(protocol);
    
      try {
        transport->open();
        switch(yapp_admin_opt_idx) {
        case YAPP_ADMIN_OPT_QSTAT:
          print_queue_info(client, ret_str_arr, task_hndl_arr);     break;
        case YAPP_ADMIN_OPT_PRINT:
          print_tasks_info(client, ret_str_arr, task_hndl_arr);     break;
        case YAPP_ADMIN_OPT_PAUSE:
          pause_tasks(client, ret_bln_arr, task_hndl_arr);          break;
        case YAPP_ADMIN_OPT_RESUME:
          resume_tasks(client, ret_bln_arr, task_hndl_arr);         break;
        case YAPP_ADMIN_OPT_KILL:
          kill_tasks(client, ret_bln_arr, task_hndl_arr);           break;
        case YAPP_ADMIN_OPT_RESTART_FAILED:
          restart_failed_tasks(client, ret_bln_arr, task_hndl_arr); break;
        case YAPP_ADMIN_OPT_RESTART_FULL:
          fully_restart_tasks(client, ret_bln_arr, task_hndl_arr);  break;
        case YAPP_ADMIN_OPT_RESTART_CONTINUE:
          restart_continue_tasks(client,ret_bln_arr,task_hndl_arr); break;
        case YAPP_ADMIN_OPT_PURGE:
          purge_tasks(client, ret_bln_arr, task_hndl_arr);
          print_purge_result(task_hndl_arr, ret_bln_arr);           break;

        case YAPP_ADMIN_OPT_LIST_JOB:
          print_job_hndl(client);                                   break;
        case YAPP_ADMIN_OPT_LIST_JOB_BY_OWNER:
          print_job_hndl_by_owner(client, task_hndl_arr);           break;
        case YAPP_ADMIN_OPT_LIST_JOB_BY_HOST:
          print_job_hndl_by_host(client, task_hndl_arr);            break;
        case YAPP_ADMIN_OPT_LIST_HOST:
          print_hosts_list();                                       break;
        case YAPP_ADMIN_OPT_LIST_ENVS:
          print_yappd_envs(client);                                 break; 

        default: break;
        }
        transport->close();
      } catch (TException &tx) {
        ret_val = YAPP_MSG_INVALID;
        std::cerr << tx.what() << std::endl;
      }
      break;
    }
  }
  return ret_val;
}

bool YappAdmin::is_opt_chosen(const string & opt) {
  return (opt == "y" || opt == "Y");
}

void YappAdmin::ls_child_node_arr(const string & path, vector<string> & ret_arr)
{
  if (NULL == zkc_proxy_ptr) { return; }
  zkc_proxy_ptr->get_node_arr(path, ret_arr);
}

void YappAdmin::print_ls_child_node_arr(const vector<string> & ret_arr) {
  std::cout << std::endl;
  std::cout << "Nodes Under Path: " << task_hndl_arr.front() << std::endl;
  std::cout << std::endl;
  for (size_t i = 0; i < ret_arr.size(); i++) {
    std::cout << ret_arr[i] << std::endl;
  }
  std::cout << std::endl;
}

void YappAdmin::get_node_val(const string & path, vector<string> & ret_arr) {
  if (NULL == zkc_proxy_ptr) { return; }
  string data_str;
  zkc_proxy_ptr->get_node_data(path, data_str);
  ret_arr.push_back(path);
  ret_arr.push_back(data_str);
}

void YappAdmin::print_get_node_val(const vector<string> & ret_arr) {
  std::cout << std::endl;
  std::cout << "Key : Value For Node: " << task_hndl_arr.front() << std::endl;
  if (2 != ret_arr.size()) { return; }
  std::cout << std::endl;
  std::cout << ret_arr[0] << " : " << ret_arr[1] << std::endl;
  std::cout << std::endl;
}

void YappAdmin::rm_node(const string & path, vector<string> & ret_arr) {
  if (NULL == zkc_proxy_ptr) { return; }
  if (YAPP_MSG_SUCCESS == zkc_proxy_ptr->del_node(path)) {
    ret_arr.push_back(path);
  }
}

void YappAdmin::print_rm_node(const vector<string> & ret_arr) {
  std::cout << std::endl;
  if (ret_arr.size() > 0) {
    std::cout << "Node: " << task_hndl_arr.front()
              << " Deleted!" << std::endl;
  } else {
    std::cout << "Failed To Delete Node: " << task_hndl_arr.front() << std::endl;
  }
  std::cout << std::endl;
}

void YappAdmin::rm_node_tree(const string & path, vector<string> & ret_arr) {
  if (NULL == zkc_proxy_ptr) { return; }
  string tree_str;
  if (YAPP_MSG_SUCCESS == zkc_proxy_ptr->print_node_recur(tree_str, path, false, false)) {
    if (YAPP_MSG_SUCCESS == zkc_proxy_ptr->delete_node_recur(path)) {
      ret_arr.push_back(path);
      ret_arr.push_back(tree_str);
    }
  }
}

void YappAdmin::print_rm_node_tree(const vector<string> & ret_arr) {
  if (2 == ret_arr.size()) {
    std::cout << std::endl;
    std::cout << "Node Tree Under Path: " << ret_arr[0];
    std::cout << std::endl;
    std::cout << std::endl;
    std::cout << ret_arr[1];
    std::cout << std::endl;
    std::cout << std::endl;
    std::cout << " Deleted!" << std::endl;
    std::cout << std::endl;
  } else {
    std::cout << "Failed To Delete Node Tree: " << task_hndl_arr.front() << std::endl;
  }
}

void YappAdmin::init_yapp_service_env(vector<string> & ret_arr) {
  if (NULL == zkc_proxy_ptr) { return; }
  zkc_proxy_ptr->delete_node_recur(
    (cfg_obj.yappd_root_path.empty() ? SYS_ROOT_PATH : cfg_obj.yappd_root_path)
  );

  vector<string> path_prfx_arr;
  if (false == cfg_obj.yappd_root_path.empty()) {
    vector<string> ret_dir_arr;
    string ret_node_name, cur_path = "/";
    if (true == StringUtil::parse_full_zk_node_path(
          cfg_obj.yappd_root_path, ret_dir_arr, ret_node_name))
    {
      for (size_t i = 0; i < ret_dir_arr.size(); i++) {
        cur_path += ret_dir_arr[i];
        path_prfx_arr.push_back(cur_path);
        cur_path += "/";
      }
      if (false == ret_node_name.empty()) { 
        path_prfx_arr.push_back(cur_path + ret_node_name);
      }
    }
  }

  vector<string> path_arr;
  vector<string> data_arr;
  for (size_t i = 0; i < path_prfx_arr.size(); i++) {
    path_arr.push_back(path_prfx_arr[i]);
    data_arr.push_back("");
    ret_arr.push_back(path_prfx_arr[i]);
  }
  for (size_t i = 0; i < sizeof(YAPP_ENV) / sizeof(char *); i++) {
    path_arr.push_back(cfg_obj.yappd_root_path + YAPP_ENV[i]);
    data_arr.push_back("");
    ret_arr.push_back(cfg_obj.yappd_root_path + string(YAPP_ENV[i]));
  }
  if (YAPP_MSG_SUCCESS != zkc_proxy_ptr->batch_create(path_arr, data_arr)) {
    ret_arr.clear();
  }
}

void YappAdmin::tree_child_node_arr(const string& path, vector<string>& ret_arr)
{
  if (NULL == zkc_proxy_ptr) { return; }
  string tree_str;
  zkc_proxy_ptr->print_node_recur(tree_str, path, true, false);
  ret_arr.push_back(tree_str);
}

void YappAdmin::set_node(
  const string & path, const string & data_str, vector<string> & ret_arr)
{
  if (NULL == zkc_proxy_ptr) { return; }
  if (YAPP_MSG_SUCCESS == zkc_proxy_ptr->set_node_data(path, data_str)) {
    ret_arr.push_back(path);
    ret_arr.push_back(data_str);
  }
}

void YappAdmin::print_set_node(const vector<string> & ret_arr) {
  std::cout << std::endl;
  if (2 == ret_arr.size()) {
    std::cout << std::endl;
    std::cout << "Key-Value: [" << ret_arr[0] << " : " << ret_arr[1] << "] Updated!" << std::endl;
    std::cout << std::endl;
  } else {
    std::cout << "Failed To Update Value for Node: " << task_hndl_arr.front() << std::endl;
  }
}


void YappAdmin::create_node(
  const string & path, const string & data_str, vector<string> & ret_arr)
{
  if (NULL == zkc_proxy_ptr) { return; }
  string path_ret;
  if (YAPP_MSG_SUCCESS == zkc_proxy_ptr->create_node_recur(
        path, data_str, path_ret))
  {
    ret_arr.push_back(path);
    ret_arr.push_back(data_str);
  }
}

void YappAdmin::print_create_node(const vector<string> & ret_arr) {
  std::cout << std::endl;
  if (2 == ret_arr.size()) {
    std::cout << std::endl;
    std::cout << "Node: [" << ret_arr[0] << " : " << ret_arr[1] << "] Created!" << std::endl;
    std::cout << std::endl;
  } else {
    std::cout << "Failed To Create Node: " << task_hndl_arr.front() << std::endl;
  }
}


void YappAdmin::print_tree_child_node_arr(const vector<string> & ret_arr) {
  std::cout << std::endl;
  std::cout << "Node Tree Under Path: " << task_hndl_arr.front() << std::endl;
  std::cout << std::endl;
  for (size_t i = 0; i < ret_arr.size(); i++) {
    std::cout << ret_arr[i] << std::endl;
  }
  std::cout << std::endl;
}

void YappAdmin::print_init_yapp_service_env_result(
  const vector<string> & ret_arr) {
  std::cout << "All Data Under Path: "
            << (cfg_obj.yappd_root_path.empty() ? SYS_ROOT_PATH : cfg_obj.yappd_root_path)
            << " Being Cleared!" << std::endl;
  std::cout << "Following Folders Have Been Created:" << std::endl;
  for (size_t i = 0; i < ret_arr.size(); i++) {
    std::cout << "-- " << ret_arr[i] << std::endl;
  }
}

void YappAdmin::print_queue_info(YappServiceClient & client,
                                 vector<string> & tree_str_arr,
                           const vector<string> & hndl_str_arr) {
  int hndl_str_type = TaskHandleUtil::TASK_HANDLE_TYPE_INVALID;
  if (hndl_str_arr.empty()) {
    string ret_tree_str;
    client.print_queue_stat(ret_tree_str, string(""));
    tree_str_arr.push_back(ret_tree_str);
  }
  for (size_t i = 0; i < hndl_str_arr.size(); i++) {
    string ret_tree_str;
    hndl_str_type = TaskHandleUtil::get_task_handle_type(hndl_str_arr[i]);
    switch(hndl_str_type) {
    case TaskHandleUtil::TASK_HANDLE_TYPE_JOBS:
    case TaskHandleUtil::TASK_HANDLE_TYPE_TASK:
    case TaskHandleUtil::TASK_HANDLE_TYPE_PROC:
      client.print_queue_stat(ret_tree_str, hndl_str_arr[i]); break;
    default:                                                  break;
    }
    tree_str_arr.push_back(ret_tree_str);
  }
  for (size_t i = 0; i < tree_str_arr.size(); i++) { 
    std::cout << tree_str_arr[i] << std::endl;
  }
}

void YappAdmin::print_tasks_info(YappServiceClient & client,
                                 vector<string> & tree_str_arr,
                           const vector<string> & hndl_str_arr) {
  int hndl_str_type = TaskHandleUtil::TASK_HANDLE_TYPE_INVALID;
  for (size_t i = 0; i < hndl_str_arr.size(); i++) {
    string ret_tree_str;
    hndl_str_type = TaskHandleUtil::get_task_handle_type(hndl_str_arr[i]);
    switch(hndl_str_type) {
    case TaskHandleUtil::TASK_HANDLE_TYPE_JOBS:
      client.print_job_tree_rpc(ret_tree_str, hndl_str_arr[i]);  break;
    case TaskHandleUtil::TASK_HANDLE_TYPE_TASK:
      client.print_task_tree_rpc(ret_tree_str, hndl_str_arr[i]); break;
    case TaskHandleUtil::TASK_HANDLE_TYPE_PROC:
      client.print_proc_tree_rpc(ret_tree_str, hndl_str_arr[i]); break;
    default:                                                     break;
    }
    tree_str_arr.push_back(ret_tree_str);
  }
  for (size_t i = 0; i < tree_str_arr.size(); i++) { 
    std::cout << tree_str_arr[i] << std::endl;
  }
}

void YappAdmin::kill_tasks(YappServiceClient & client,
                           vector<bool> & kill_req_ret_arr,
                     const vector<string> & hndl_str_arr)
{
  vector<string> active_hndl_arr;
  client.get_all_active_subtask_hndl_arr_rpc(active_hndl_arr, hndl_str_arr);
  if (active_hndl_arr.size() > 0) {
    client.terminate_proc_arr_rpc(kill_req_ret_arr, active_hndl_arr);
  }
  print_kill_result(active_hndl_arr, kill_req_ret_arr);
}

void YappAdmin::print_kill_result(const vector<string> & hndl_arr,
                                        vector<bool> & ret_arr) {
  if (hndl_arr.size() != ret_arr.size()) { return; }
  int kill_cnt = 0;
  for (size_t i = 0; i < ret_arr.size(); i++) {
    if (true == ret_arr[i]) {
      kill_cnt++;
      std::cout << "==>> Succe. Killing Subtask: "
                << hndl_arr[i] << std::endl;
    } else {
      std::cout << "==>> Failed Killing Subtask: "
                << hndl_arr[i] << std::endl;
    }
  }
  std::cout << "Summary:\n"
            << "- Subtask In Killing: " << ret_arr.size() << std::endl
            << "- Succeed In Killing: " << kill_cnt << std::endl
            << "- Failure In Killing: " << ret_arr.size() - kill_cnt
            << std::endl;
  std::cout << "Note:\n"
            << "- Only those Subtasks currently running(on RUNNING Queue) Can be Killed.\n";
}

void YappAdmin::restart_failed_tasks(YappServiceClient & client,
                                     vector<bool> & req_ret_arr,
                               const vector<string> & hndl_str_arr)
{
  vector<string> fail_hndl_arr;
  client.get_all_failed_subtask_hndl_arr_rpc(fail_hndl_arr, hndl_str_arr);
#ifdef DEBUG_YAPP_ADMIN
  std::cerr << "---- hndls to search:" << std::endl;
  for (size_t i = 0; i < hndl_str_arr.size(); i++) {
    std::cerr << ">>>> " << hndl_str_arr[i] << std::endl;
  }
  std::cerr << "---- proc hndl from failed queue:" << std::endl;
  for (size_t i = 0; i < fail_hndl_arr.size(); i++) {
    std::cerr << ">>>> " << fail_hndl_arr[i] << std::endl;
  }
#endif
  if (fail_hndl_arr.size() > 0) {
    client.restart_failed_proc_arr_rpc(req_ret_arr, fail_hndl_arr);
  }
  print_restart_result(fail_hndl_arr, req_ret_arr);
}

void YappAdmin::fully_restart_tasks(YappServiceClient & client,
                                    vector<bool> & req_ret_arr,
                              const vector<string> & hndl_str_arr)
{
  vector<string> term_hndl_arr;
  vector<string> fail_hndl_arr;
  client.get_all_failed_subtask_hndl_arr_rpc(fail_hndl_arr, hndl_str_arr);
  client.get_all_terminated_subtask_hndl_arr_rpc(term_hndl_arr, hndl_str_arr);
#ifdef DEBUG_YAPP_ADMIN
  std::cerr << "---- hndls to search:" << std::endl;
  for (size_t i = 0; i < hndl_str_arr.size(); i++) {
    std::cerr << ">>>> " << hndl_str_arr[i] << std::endl;
  }
  std::cerr << "---- proc hndl from failed queue:" << std::endl;
  for (size_t i = 0; i < fail_hndl_arr.size(); i++) {
    std::cerr << ">>>> " << fail_hndl_arr[i] << std::endl;
  }
  std::cerr << "---- proc hndl from termin queue:" << std::endl;
  for (size_t i = 0; i < term_hndl_arr.size(); i++) {
    std::cerr << ">>>> " << term_hndl_arr[i] << std::endl;
  }
#endif
  if (term_hndl_arr.size() > 0 || fail_hndl_arr.size() > 0) {
    client.fully_restart_proc_arr_rpc(req_ret_arr, term_hndl_arr, fail_hndl_arr);
  }
  term_hndl_arr.insert(
    term_hndl_arr.end(), fail_hndl_arr.begin(), fail_hndl_arr.end()
  );
  print_restart_result(term_hndl_arr, req_ret_arr);
}

void YappAdmin::restart_continue_tasks(YappServiceClient & client,
                                       vector<bool> & req_ret_arr,
                                 const vector<string> & hndl_str_arr)
{
  vector<string> term_hndl_arr;
  vector<string> fail_hndl_arr;
  client.get_all_failed_subtask_hndl_arr_rpc(fail_hndl_arr, hndl_str_arr);
  client.get_all_terminated_subtask_hndl_arr_rpc(term_hndl_arr, hndl_str_arr);
#ifdef DEBUG_YAPP_ADMIN
  std::cerr << "---- hndls to search:" << std::endl;
  for (size_t i = 0; i < hndl_str_arr.size(); i++) {
    std::cerr << ">>>> " << hndl_str_arr[i] << std::endl;
  }
  std::cerr << "---- proc hndl from failed queue:" << std::endl;
  for (size_t i = 0; i < fail_hndl_arr.size(); i++) {
    std::cerr << ">>>> " << fail_hndl_arr[i] << std::endl;
  }
  std::cerr << "---- proc hndl from termin queue:" << std::endl;
  for (size_t i = 0; i < term_hndl_arr.size(); i++) {
    std::cerr << ">>>> " << term_hndl_arr[i] << std::endl;
  }
#endif
  if (term_hndl_arr.size() > 0 || fail_hndl_arr.size() > 0) {
    client.restart_from_last_anchor_proc_arr_rpc(
      req_ret_arr, term_hndl_arr, fail_hndl_arr
    );
  }
  term_hndl_arr.insert(
    term_hndl_arr.end(), fail_hndl_arr.begin(), fail_hndl_arr.end()
  );
  print_restart_result(term_hndl_arr, req_ret_arr);
}

void YappAdmin::print_restart_result(const vector<string> & hndl_arr,
                                vector<bool> & ret_arr) {
  if (hndl_arr.size() != ret_arr.size()) { return; }
  int restart_cnt = 0;
  for (size_t i = 0; i < ret_arr.size(); i++) {
    if (true == ret_arr[i]) {
      restart_cnt++;
      std::cout << "==>> Succe. Restarting Subtask: "
                << hndl_arr[i] << std::endl;
    } else {
      std::cout << "==>> Failed Restarting Subtask: "
                << hndl_arr[i] << std::endl;
    }
  }
  std::cout << "Summary:\n"
            << "- Subtask In Restarting: " << ret_arr.size() << std::endl
            << "- Succeed In Restarting: " << restart_cnt << std::endl
            << "- Failure In Restarting: " << ret_arr.size() - restart_cnt
            << std::endl;
  std::cout << "Note:\n"
            << "- Only those Subtasks currently Stopped(on TERMINATED/FAILED Q) Can be Restarted.\n";
}

void YappAdmin::pause_tasks(YappServiceClient & client,
                            vector<bool> & pause_req_ret_arr,
                      const vector<string> & hndl_str_arr)
{
  vector<string> active_hndl_arr;
  client.get_all_active_subtask_hndl_arr_rpc(active_hndl_arr, hndl_str_arr);
  if (active_hndl_arr.size() > 0) {
    client.pause_proc_arr_rpc(pause_req_ret_arr, active_hndl_arr);
  }
  print_pause_result(active_hndl_arr, pause_req_ret_arr);           
}

void YappAdmin::print_pause_result(const vector<string> & hndl_arr,
                                         vector<bool> & ret_arr)
{
  if (hndl_arr.size() != ret_arr.size()) { return; }
  int pause_cnt = 0;
  for (size_t i = 0; i < ret_arr.size(); i++) {
    if (true == ret_arr[i]) {
      pause_cnt++;
      std::cout << "==>> Succe. Pausing Subtask: "
                << hndl_arr[i] << std::endl;
    } else {
      std::cout << "==>> Failed Pausing Subtask: "
                << hndl_arr[i] << std::endl;
    }
  }
  std::cout << "Summary:\n"
            << "- Subtask In Pausing: " << ret_arr.size() << std::endl
            << "- Succeed In Pausing: " << pause_cnt << std::endl
            << "- Failure In Pausing: " << ret_arr.size() - pause_cnt
            << std::endl;
  std::cout << "Note:\n"
            << "- Only those Subtasks currently running(on RUNNING Queue) Can be Paused.\n";
}

void YappAdmin::resume_tasks(YappServiceClient & client,
                             vector<bool> & resume_req_ret_arr,
                       const vector<string> & hndl_str_arr)
{
  vector<string> paused_hndl_arr;
  client.get_all_paused_subtask_hndl_arr_rpc(paused_hndl_arr, hndl_str_arr);
  if (paused_hndl_arr.size() > 0) {
    client.resume_proc_arr_rpc(resume_req_ret_arr, paused_hndl_arr);
  }
  print_resume_result(paused_hndl_arr, resume_req_ret_arr);
}

void YappAdmin::print_resume_result(const vector<string> & hndl_arr,
                                          vector<bool> & ret_arr)
{
  if (hndl_arr.size() != ret_arr.size()) { return; }
  int resume_cnt = 0;
  for (size_t i = 0; i < ret_arr.size(); i++) {
    if (true == ret_arr[i]) {
      resume_cnt++;
      std::cout << "==>> Succe. Resuming Subtask: "
                << hndl_arr[i] << std::endl;
    } else {
      std::cout << "==>> Failed Resuming Subtask: "
                << hndl_arr[i] << std::endl;
    }
  }
  std::cout << "Summary:\n"
            << "- Subtask In Resuming: " << ret_arr.size() << std::endl
            << "- Succeed In Resuming: " << resume_cnt << std::endl
            << "- Failure In Resuming: " << ret_arr.size() - resume_cnt
            << std::endl;
  std::cout << "Note:\n"
            << "- Only those Subtasks currently paused(on PAUSED Queue) Can be Paused.\n";
}


void YappAdmin::purge_tasks(YappServiceClient & client,
                            vector<bool> & purge_req_ret_arr,
                      const vector<string> & hndl_str_arr)
{
  int hndl_str_type = TaskHandleUtil::TASK_HANDLE_TYPE_INVALID;
  for (size_t i = 0; i < hndl_str_arr.size(); i++) {
    bool b_ret = false;
    hndl_str_type = TaskHandleUtil::get_task_handle_type(hndl_str_arr[i]);
    switch(hndl_str_type) {
    case TaskHandleUtil::TASK_HANDLE_TYPE_JOBS:
      b_ret = client.purge_job_tree_rpc(hndl_str_arr[i]);  break;
    case TaskHandleUtil::TASK_HANDLE_TYPE_TASK:
    case TaskHandleUtil::TASK_HANDLE_TYPE_PROC:
    default: break;
    }
    purge_req_ret_arr.push_back(b_ret);
  }
}

void YappAdmin::print_purge_result(const vector<string> & hndl_arr,
                                         vector<bool> & ret_arr)
{
  if (hndl_arr.size() != ret_arr.size()) { return; }
  int purge_cnt = 0;
  for (size_t i = 0; i < ret_arr.size(); i++) {
    if (true == ret_arr[i]) {
      purge_cnt++;
      std::cout << "==>> Succe. Purging Job Nodes For: "
                << hndl_arr[i] << std::endl;
    } else {
      std::cout << "==>> Failed Purging for Job Nodes: "
                << hndl_arr[i] << std::endl;
    }
  }
  std::cout << "Summary:\n"
            << "- Total Job To Purge: " << ret_arr.size() << std::endl
            << "- Succeed In Purging: " << purge_cnt << std::endl
            << "- Failure In Purgine: " << ret_arr.size() - purge_cnt
            << std::endl;
  std::cout << "Note:\n"
            << "- Only the Entire job tree could be purged.\n"
            << "- Jobs must Got every Running Subasks Done before Purging.\n";
}

void YappAdmin::print_hosts_list() {
  vector<string> hosts_arr;
  if (NULL == zkc_proxy_ptr) { return; }
  tree_child_node_arr(zkc_proxy_ptr->get_node_path_prefix(), hosts_arr);
  task_hndl_arr.push_back(zkc_proxy_ptr->get_node_path_prefix());
  print_tree_child_node_arr(hosts_arr);
}

void YappAdmin::print_job_hndl_list(YappServiceClient & client,
                                    const vector<string> & host,
                                    const vector<string> & owner) {
  /**
   * tsk_grp_arr {
   *   $ip-1 => tsk_grp-1 {
   *              $owner-1 => job-hndl-1, job-hndl-2...
   *              ...
   *              $owner-n => job-hndl-1, job-hndl-2...
   *            }
   *   ...
   *   $ip-n => tsk_grp-n {
   *              $owner-1 => job-hndl-1, job-hndl-2...
   *              ...
   *              $owner-n => job-hndl-1, job-hndl-2...
   *            }
   * }
   * ==>> Host: ${ip-n}, Owner: ${owner-n}, Task: job-hndl-n
   *      ...
   * ~~>> Host: ${ip-n}, ${job-hndl-m}: 4, ... ${job-hndl-n}: 8
   * ...
   */
  map<string, map<string, vector<string> > > host_to_tsk_grp_map;
  client.query_running_task_basic_info_rpc(host_to_tsk_grp_map, host, owner);

  if (host_to_tsk_grp_map.empty()) { return; }
  /** { job-1 => { host-1 => 5, host-2 => 6 } ... } **/
  map<string, map<string, int> > job_hndl_to_dist_map;
  map<string, string> job_hndl_to_owner_map;
  map<string, int> job_cnt_map;

  map<string, map<string, vector<string> > >::iterator
    tsk_grp_itr = host_to_tsk_grp_map.begin();
  for (; tsk_grp_itr != host_to_tsk_grp_map.end(); tsk_grp_itr++) {
    map<string, int> job_hndl_to_cnt_map;
    map<string, vector<string> >::iterator tsk_itr = tsk_grp_itr->second.begin();
    for (; tsk_itr != tsk_grp_itr->second.end(); tsk_itr++) {
      int job_cnt = tsk_itr->second.size();
      for (int i = 0; i < job_cnt; i++) {
        std::cout << "-->> " << "Host: "  << tsk_grp_itr->first << ", "
                             << "Owner: " << tsk_itr->first     << ", "
                             << "Task: "  << tsk_itr->second[i] << std::endl;
        string job_hndl;
        size_t pos = tsk_itr->second[i].find(TASK_HNDL_DELM);
        if (string::npos != pos) {
          job_hndl = tsk_itr->second[i].substr(0, pos);
          if (job_hndl_to_cnt_map.end() != job_hndl_to_cnt_map.find(job_hndl)) {
            job_hndl_to_cnt_map[job_hndl]++;
          } else {
            job_hndl_to_cnt_map[job_hndl] = 1;
          }
          if (job_hndl_to_owner_map.end() == job_hndl_to_owner_map.find(job_hndl)) {
            job_hndl_to_owner_map[job_hndl] = tsk_itr->first;
          }
          if (job_hndl_to_dist_map.end() != job_hndl_to_dist_map.find(job_hndl)) {
            job_hndl_to_dist_map[job_hndl][tsk_grp_itr->first]++;
          } else {
            map<string, int> host_to_cnt_map;
            host_to_cnt_map[tsk_grp_itr->first] = 1;
            job_hndl_to_dist_map[job_hndl] = host_to_cnt_map;
          }
          if (job_cnt_map.end() != job_cnt_map.find(job_hndl)) {
            job_cnt_map[job_hndl]++;
          } else {
            job_cnt_map[job_hndl] = 1;
          }
        } // if (string::npos != pos)
      } // for (int i = 0; i < job_cnt; i++)
    } // for (; tsk_itr != tsk_grp_itr->second.end(); tsk_itr++)
    size_t cnt = 0, tsk_cnt = 0;
    map<string, int>::iterator job_cnt_itr = job_hndl_to_cnt_map.begin();
    std::cout << "==>> " << "Host: " << tsk_grp_itr->first << ", "
                         << "Job#: " << job_hndl_to_cnt_map.size() << ", "
                         << "[ ";
    for (; job_cnt_itr != job_hndl_to_cnt_map.end(); job_cnt_itr++, cnt++) {
      std::cout << job_cnt_itr->first << "("
                << job_hndl_to_owner_map[job_cnt_itr->first] << ")" << " => "
                << job_cnt_itr->second;
      tsk_cnt += job_cnt_itr->second;
      if (cnt != job_hndl_to_cnt_map.size() - 1) { std::cout << ", "; }
    }
    std::cout << " ], Total: " << tsk_cnt << std::endl;
  }

  map<string, map<string, int> >::iterator job_to_dist_itr =
    job_hndl_to_dist_map.begin();
  for (; job_to_dist_itr != job_hndl_to_dist_map.end(); job_to_dist_itr++) {
    std::cout << "++>> Task Distibution for Job " << job_to_dist_itr->first
              << " (" << job_cnt_map[job_to_dist_itr->first] << "): ";
    size_t cnt = 0;
    map<string, int>::iterator host_cnt_itr = job_to_dist_itr->second.begin();
    std::cout << "[ ";
    for (; host_cnt_itr != job_to_dist_itr->second.end(); host_cnt_itr++, cnt++) {
      std::cout << host_cnt_itr->first << "("
                << job_hndl_to_owner_map[job_to_dist_itr->first] << ")"
                << " => " << host_cnt_itr->second;
      if (cnt != job_to_dist_itr->second.size() - 1) { std::cout << ", "; }
    }
    std::cout << " ]" << std::endl;
  }
}

void YappAdmin::print_job_hndl_by_host(YappServiceClient & client, const vector<string> & host) {
  vector<string> owner;
  print_job_hndl_list(client, host, owner);
}

void YappAdmin::print_job_hndl_by_owner(YappServiceClient & client, const vector<string> & owner) {
  vector<string> host;
  print_job_hndl_list(client, host, owner);
}

void YappAdmin::print_job_hndl(YappServiceClient & client) {
  vector<string> host, owner;
  print_job_hndl_list(client, host, owner);
}

void YappAdmin::print_yappd_envs(YappServiceClient & client) {
   map<string, map<string, string> > host_to_env_kv_pair_map;
   client.query_yappd_envs_rpc(host_to_env_kv_pair_map);
   map<string, map<string, string> >::iterator env_map_itr = host_to_env_kv_pair_map.begin();
   for (; env_map_itr != host_to_env_kv_pair_map.end(); env_map_itr++) {
     std::cout << env_map_itr->first << ": {" << std::endl;
     map<string, string>::iterator kv_itr = env_map_itr->second.begin();
     for (; kv_itr != env_map_itr->second.end(); kv_itr++) {
       std::cout << "    " << kv_itr->first << kv_itr->second << std::endl;
     }
     std::cout << "}" << std::endl;
   }
}
