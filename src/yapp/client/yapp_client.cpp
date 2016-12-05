#include <fstream>

#include "./yapp_client.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "../domain/gen-cpp/YappService.h"

using std::ifstream;
using std::ios;

using namespace yapp::client;

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace boost;

YAPP_MSG_CODE YappClient::parse_arguments()
{
  YAPP_MSG_CODE ret_val = YAPP_MSG_INVALID_ARGS;
  /** arg_arr here comes from its base class YappBase **/
  int argc = arg_arr.size();
  bool b_arg_valid = true;

  size_t kv_delim_pos = -1;
  string key_str;
  string val_str;

  if (1 > argc) { b_arg_valid = false; }

  /** when there is only 1 argument, yapp would take it as an input file. **/
  if (1 == argc) {
    arg_arr.clear();
    ifstream fin;
    fin.open(arg_arr.front().c_str(), ios::in);
    if (true == fin.is_open()) {
      string buffer = "";
      while (std::getline(fin, buffer)) {
        StringUtil::trim_string(buffer);
        if (buffer.empty()) { continue; }
        if (0 == buffer.find(CONFIG_COMMENTS_PREFIX)) { continue; }
        arg_arr.push_back(buffer);
      }
      fin.close();
    }
    argc = arg_arr.size();
  }

  for (int i = 0; i < argc; i++) {
    if (false == b_arg_valid) { break; }
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "parsing argument: " << arg_arr[i] << std::endl;
#endif 
    kv_delim_pos = arg_arr[i].find(KEY_VALUE_DELIM, 0);
    if (string::npos == kv_delim_pos) {
      b_arg_valid = false;
      break;
    }
    key_str = arg_arr[i].substr(0, kv_delim_pos + 1);
    val_str = arg_arr[i].substr(kv_delim_pos + 1);

    StringUtil::trim_string(key_str);
    StringUtil::trim_string(val_str);

    if (key_str.empty() || val_str.empty()) { continue; }
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "<key_str: " << key_str << "> "
              << "<val_str: " << val_str << "> " << std::endl;
#endif

    for (size_t c = 0; c < sizeof(YAPP_CLIENT_OPTION_ENTRY) /
                           sizeof(char *); c++) {

      if (false == b_arg_valid) { break; }
      if (key_str != YAPP_CLIENT_OPTION_ENTRY[c]) { continue; }

      switch ((YAPP_CLIENT_OPTION_ENTRY_MAPPING)c) {
        case YAPP_CLIENT_OPT_ZK_CONN_STR:      { zk_conn_str = val_str; break; }
        case YAPP_CLIENT_OPT_ZK_CFG_FILE:      {
          if (true == cfg_obj.load_zk_cluster_cfg(val_str)) {
            zk_conn_str = cfg_obj.get_zk_cluster_conn_str();
            break;
          }
        }
        case YAPP_CLIENT_OPT_AUTO_SPLIT:       {
          if ("true" == val_str || "false" == val_str) {
            b_auto_split = ("true" == val_str) ? true : false;
            break;
          }
        }
        case YAPP_CLIENT_OPT_AUTO_MIGRATE:     {
          if ("true" == val_str || "false" == val_str) {
            b_auto_migrate = ("true" == val_str) ? true : false;
            break;
          }
        }
        case YAPP_CLIENT_OPT_PROC_NUMBER:      {
          if (StringUtil::is_integer(val_str)) {
            proc_num = atol(val_str.c_str());
            break;
          }
        }
        case YAPP_CLIENT_OPT_APP_ENV:          { app_env = val_str; break; }
        case YAPP_CLIENT_OPT_APP_BIN:          { app_bin = val_str; break; }
        case YAPP_CLIENT_OPT_APP_SRC:          { app_src = val_str; break; }
        case YAPP_CLIENT_OPT_RANGE_INPUT:      {
          int range_delim_pos = val_str.find(RANGE_NUM_DELIM, 0);
          string from_str = val_str.substr(0, range_delim_pos);
          string to_str   = val_str.substr(range_delim_pos + 1);
          if (StringUtil::is_integer(from_str)&&StringUtil::is_integer(to_str)){
            range_from = atoll(from_str.c_str());
            range_to   = atoll(to_str.c_str());
            if (range_from < range_to) {
              task_type = YAPP_CLIENT_RANGE_TASK;
              break;
            }
          }
        }

        case YAPP_CLIENT_OPT_RANGE_STEP:       {
          if (StringUtil::is_integer(val_str)) {
            range_step = atoll(val_str.c_str());
            task_type = YAPP_CLIENT_DYNAMIC_RANGE_TASK;
            break;
          }
        }

        case YAPP_CLIENT_OPT_RANGE_FILE_INPUT: {
          if (YAPP_CLIENT_TASK_TYPE_INVALID == task_type && !val_str.empty()) {
            long long int line_cnt = 0;
            range_file = val_str;
            StringUtil::trim_string(range_file);
            ifstream fin;
            fin.open(range_file.c_str(), ios::in);
            if (true == fin.is_open()) {
              string buffer = "";
              while (std::getline(fin, buffer)) { line_cnt++; }
              fin.close();
              range_from = 0;
              range_to   = line_cnt - 1;
              task_type = YAPP_CLIENT_RANGE_FILE_TASK;
              break;
            }
          }
        }
        case YAPP_CLIENT_OPT_WORKING_DIR:     { working_dir = val_str; break; }
        case YAPP_CLIENT_OPT_ANCHOR_PRFX:     { anchor_prfx = val_str; break; }
        case YAPP_CLIENT_OPT_APP_ARGS:        { app_args    = val_str; break; }
        case YAPP_CLIENT_OPT_STDOUT:          { fstdout     = val_str; break; }
        case YAPP_CLIENT_OPT_STDERR:          { fstderr     = val_str; break; }
        case YAPP_CLIENT_OPT_JOB_OWNER:       { job_owner   = val_str; break; }
        default: {
          /** once reach here, something wrong detected for the arguments. **/
          b_arg_valid = false;
#ifdef DEBUG_YAPP_CLIENT
          std::cerr << "INVALID ARGS FOUND: "
                    << "<key_str: " << key_str << "> "
                    << "<val_str: " << val_str << "> " << std::endl;
#endif
        }
      } /* switch ((YAPP_CLIENT_OPTION_ENTRY_MAPPING)c) */
    } /* for (size_t c = 0; c < sizeof(YAPP_CLIENT_OPTION_ENTRY) */
  } /* for (int i = 0; i < argc; i++) */

  if (true == b_arg_valid) {
    /** here is mainly for detect any missing arugments. **/
    if (app_bin.empty() || zk_conn_str.empty()) {
#ifdef DEBUG_YAPP_CLIENT
      std::cerr << "MISSING ARGS." << std::endl;
#endif
      b_arg_valid = false;
    } else {
      switch(task_type) {
        case YAPP_CLIENT_RANGE_TASK: {
          /** range task should specify valid proc num and id ranges. **/
          if (YAPP_CLIENT_INVALID == proc_num || YAPP_CLIENT_INVALID == range_from ||
              YAPP_CLIENT_INVALID == range_to) {
            b_arg_valid = false;
            break;
          }
          job_obj.owner = job_owner;
          job_obj.created_tmstp_sec = time(NULL);
          job_obj.task_arr.push_back(
            YappDomainFactory::create_task(
              range_from, range_to, proc_num, app_env, app_bin, app_src,
              app_args, fstdout, fstderr, b_auto_split, b_auto_migrate,
              working_dir, anchor_prfx
            )
          );
#ifdef DEBUG_YAPP_CLIENT
      std::cerr << "job_obj.owner: " << job_obj.owner << std::endl;
      std::cerr << "job_obj.task#: " << job_obj.task_arr.size() << std::endl;
#endif
          break;
        }

        case YAPP_CLIENT_DYNAMIC_RANGE_TASK: {
          /** range task should specify valid proc num and id ranges. **/
          if (YAPP_CLIENT_INVALID == proc_num || YAPP_CLIENT_INVALID == range_from ||
              YAPP_CLIENT_INVALID == range_to || YAPP_CLIENT_INVALID == range_step ||
              range_from > range_to || range_step < 0) {
            b_arg_valid = false;
            break;
          }
          job_obj.owner = job_owner;
          job_obj.created_tmstp_sec = time(NULL);
          job_obj.task_arr.push_back(
            YappDomainFactory::create_task(
              range_from, range_to, proc_num, app_env, app_bin, app_src,
              app_args, fstdout, fstderr, b_auto_split, b_auto_migrate,
              working_dir, anchor_prfx, cfg_obj.yappd_tmp_folder,
              TASK_INPUT_TYPE::DYNAMIC_RANGE, range_step
            )
          );
#ifdef DEBUG_YAPP_CLIENT
      std::cerr << "job_obj.owner: " << job_obj.owner << std::endl;
      std::cerr << "job_obj.task#: " << job_obj.task_arr.size() << std::endl;
#endif
          break;
        }

        case YAPP_CLIENT_RANGE_FILE_TASK: {
          /** range file task should specify valid proc num and range file. **/
          if(YAPP_CLIENT_INVALID == proc_num || range_file.empty()) {
            b_arg_valid = false;
            break;
          }
          job_obj.owner = job_owner;
          job_obj.created_tmstp_sec = time(NULL);
          job_obj.task_arr.push_back(
            YappDomainFactory::create_task(
              range_from, range_to, proc_num, app_env, app_bin, app_src,
              app_args, fstdout, fstderr, b_auto_split, b_auto_migrate,
              working_dir, anchor_prfx, range_file, TASK_INPUT_TYPE::RANGE_FILE
            )
          );
#ifdef DEBUG_YAPP_CLIENT
      std::cerr << "job_obj.owner: " << job_obj.owner << std::endl;
      std::cerr << "job_obj.task#: " << job_obj.task_arr.size() << std::endl;
#endif
          break;
        }
        default: {
          /** once reach here, something wrong detected for the arguments. **/
          b_arg_valid = false;
        }
      }
    }
  }
  if (true == b_arg_valid) {
    ret_val = YAPP_MSG_SUCCESS;
  }
  return ret_val;
}

YappClient::YappClient(vector<string> args, bool b_verb, bool b_test) :
    YappBase(args, b_verb, b_test)
{
  zkc_proxy_ptr  = NULL;

  b_auto_split   = true;
  b_auto_migrate = true;
  proc_num       = YAPP_CLIENT_DEF_PROC_NUM;
  range_from     = YAPP_CLIENT_INVALID;
  range_step     = YAPP_CLIENT_INVALID;
  range_to       = YAPP_CLIENT_INVALID;
  task_type      = YAPP_CLIENT_TASK_TYPE_INVALID;
}

YappClient::~YappClient() {
  delete zkc_proxy_ptr;
}

YAPP_MSG_CODE YappClient::run() {
  if (true == b_verbose) {
    std::cout << "-- running yapp at client mode" << std::endl;
  }
  cfg_obj.load_zk_cluster_cfg(def_cfg_fp);
  zk_conn_str = cfg_obj.get_zk_cluster_conn_str();
  YAPP_MSG_CODE ret_val = parse_arguments();
  if (YAPP_MSG_SUCCESS != ret_val) {
    usage();
    return ret_val;
  }

  assert(YAPP_MSG_SUCCESS == init_zk_cluster_conn_by_str());

  string master_host;
  string master_port;
  string master_pcid;

  zkc_proxy_ptr->get_yapp_master_addr(master_host, master_port, master_pcid);

  TSocket * socket_ptr = new TSocket(master_host, atol(master_port.c_str()));
  assert(NULL != socket_ptr);
  socket_ptr->setConnTimeout(cfg_obj.max_zkc_timeout);

  shared_ptr<TTransport> socket(socket_ptr);
  shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  YappServiceClient client(protocol);
  Job job_ret;

  try {
    transport->open();
    client.submit_job_rpc(job_ret, job_obj);

    if (true == job_ret.job_hnd.empty()) {
      ret_val = (YAPP_MSG_CODE)job_obj.yapp_service_code;
      std::cerr << "Failed To Submit the Jobs: " << ret_val << " >> "
                << YAPP_MSG_ENTRY[0 - ret_val] << std::endl;
    } else {
      string job_tree;
      client.print_job_tree_rpc(job_tree, job_ret.job_hnd);
      std::cout << "-- Job Spec.:" << std::endl;
      std::cout << job_tree;
      job_obj = job_ret;
    }

    transport->close();
  } catch (TException &tx) {
    ret_val = YAPP_MSG_INVALID;
    std::cerr << tx.what() << std::endl;
    std::cerr << ">>>>>> ERROR <<<<<<" << std::endl;
  }

  return ret_val;
}

void YappClient::usage() {
  std::cerr << "Usage When Run Yapp in Client Mode, Use Either:"
            << std::endl
            << std::endl
            << "yapp ${conf. file path} (Which is Recommended)" << std::endl
            << std::endl
            << "or" << std::endl
            << std::endl
            << "yapp [--mode=client]"
            << std::endl
            << "     --zk_conn_str=192.168.1.1:2181 | --zk_cfg_file='/etc/yapp.cfg'"
            << std::endl
            << "     [--autosplit=(true|false)] [--automigrate=(true|false)]"
            << std::endl
            << "     [--proc_num=${num}<16 def.>]"
            << std::endl
            << "     [--working_dir='/var/www/test.foo.com/current']"
            << std::endl
            << "     [--app_env='RAILS_ENV=production']"
            << std::endl
            << "     --app_bin='/bin/ruby' [--app_src='./lib/data/update_ra.rb']"
            << std::endl
            << "     --range='0 59'  | --range_file='/var/tmp/range.info'"
            << std::endl
            << "     [--arg_str='-test -verbose']"
            << std::endl
            << "     --stdout='/var/tmp/update_ra.stdout'"
            << std::endl
            << "     --stderr='/var/tmp/update_ra.stdout'"
            << std::endl;
}

YAPP_MSG_CODE YappClient::init_zk_cluster_conn_by_str() {
  YAPP_MSG_CODE ret_val = YAPP_MSG_INVALID;
  if (false == zk_conn_str.empty()) {
    zkc_proxy_ptr = new ZkClusterProxy(
      b_testing, cfg_obj.port_num, cfg_obj.max_queued_task,
      cfg_obj.max_zkc_timeout, cfg_obj.yappd_root_path
    );
    assert(NULL != zkc_proxy_ptr);
    // zkc_proxy_ptr->set_zk_namespace(cfg_obj.yappd_root_path);
    ret_val = zkc_proxy_ptr->init_zk_conn(zk_conn_str);
  }
  return ret_val;
}
