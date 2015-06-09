/**
 * Desc:
 * - This files contains the interface of YAPP admin instance, which will act as
 *   the central point for manully job control(kill/restart, pause/resume, purge
 *   and running status query.
 *
 * - Server would be per request Threaded, using either blocking or non-blocking
 *   I/O and implemented based on RPC framework provided by Apache Thrift.
 */

#ifndef YAPP_ADMIN_H_
#define YAPP_ADMIN_H_
#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include <boost/shared_ptr.hpp>

#include "../util/yapp_util.h"
#include "../base/yapp_base.h"
#include "../domain/yapp_service_handler.h"

namespace yapp {
namespace admin {

using std::string;
using std::vector;
using boost::shared_ptr;

using namespace yapp::base;
using namespace yapp::util;
using namespace yapp::domain;

const static char * const YAPP_ADMIN_OPTION_ENTRY[] = {
  "--qstat",          "--print",             "--kill",
  "--restart-failed", "--restart-full",      "--restart-continue",
  "--pause",          "--resume",            "--purge",
  "--list-jobs",      "--list-job-by-owner", "--list-job-by-host",
  "--list-host",      "--list-envs",

  "--init",           "--ls",                "--tree",
  "--get",            "--set",               "--rm",
  "--rmtree",         "--create",
  "--zk_conn_str=",   "--zk_cfg_file=",
};

enum YAPP_ADMIN_OPTION_ENTRY_MAPPING {
  YAPP_ADMIN_OPT_INVALID = -1,

  YAPP_ADMIN_OPT_QSTAT,
  YAPP_ADMIN_OPT_PRINT,
  YAPP_ADMIN_OPT_KILL,
  YAPP_ADMIN_OPT_RESTART_FAILED,
  YAPP_ADMIN_OPT_RESTART_FULL,
  YAPP_ADMIN_OPT_RESTART_CONTINUE,
  YAPP_ADMIN_OPT_PAUSE,
  YAPP_ADMIN_OPT_RESUME,
  YAPP_ADMIN_OPT_PURGE,

  YAPP_ADMIN_OPT_LIST_JOB,
  YAPP_ADMIN_OPT_LIST_JOB_BY_OWNER,
  YAPP_ADMIN_OPT_LIST_JOB_BY_HOST,
  YAPP_ADMIN_OPT_LIST_HOST,
  YAPP_ADMIN_OPT_LIST_ENVS,

  YAPP_ADMIN_OPT_INIT,
  YAPP_ADMIN_OPT_LS,
  YAPP_ADMIN_OPT_TREE,
  YAPP_ADMIN_OPT_GET,
  YAPP_ADMIN_OPT_SET,
  YAPP_ADMIN_OPT_RM,
  YAPP_ADMIN_OPT_RMTREE,
  YAPP_ADMIN_OPT_CREATE,

  YAPP_ADMIN_OPT_CONN_STR,
  YAPP_ADMIN_OPT_ZKCFG, 

  YAPP_ADMIN_OPT_COUNT
};

class YappAdmin : public YappBase {
public:
  YappAdmin(vector<string> args, bool b_verb, bool b_test = false);

  virtual ~YappAdmin();
  virtual YAPP_MSG_CODE run();
  virtual YAPP_MSG_CODE parse_arguments();
  virtual void usage();

  void print_warnings_and_getopt(const string & cust_info, string & opt);
  bool is_opt_chosen(const string & opt);

  YAPP_MSG_CODE init_zk_cluster_conn_by_str();

  string get_zk_conn_str() { return zk_conn_str; }

protected:

  void init_yapp_service_env(vector<string> & ret_arr);
  void print_init_yapp_service_env_result(const vector<string> & ret_arr);

  void ls_child_node_arr(const string & path, vector<string> & ret_arr);
  void print_ls_child_node_arr(const vector<string> & ret_arr);

  void tree_child_node_arr(const string & path, vector<string> & ret_arr);
  void print_tree_child_node_arr(const vector<string> & ret_arr);

  void get_node_val(const string & path, vector<string> & ret_arr);
  void print_get_node_val(const vector<string> & ret_arr);

  void set_node(const string & path, const string & data_str, vector<string> & ret_arr);
  void print_set_node(const vector<string> & ret_arr);

  void rm_node(const string & path, vector<string> & ret_arr);
  void print_rm_node(const vector<string> & ret_arr);

  void rm_node_tree(const string & path, vector<string> & ret_arr);
  void print_rm_node_tree(const vector<string> & ret_arr);

  void create_node(const string & path, const string & data_str, vector<string> & ret_arr);
  void print_create_node(const vector<string> & ret_arr);

  void print_queue_info(YappServiceClient & client, vector<string> & ret_arr,
                                              const vector<string> & hndl_arr);

  void print_tasks_info(YappServiceClient & client, vector<string> & ret_arr,
                                              const vector<string> & hndl_arr);

  void kill_tasks(YappServiceClient & client, vector<bool> & ret_arr,
                                        const vector<string> & hndl_str_arr);
  void print_kill_result(const vector<string> & hndl_arr,
                               vector<bool> & ret_arr);

  void restart_failed_tasks(YappServiceClient & client, vector<bool> & ret_arr,
                            const vector<string> & hndl_str_arr);
  void fully_restart_tasks(YappServiceClient & client, vector<bool> & ret_arr,
                           const vector<string> & hndl_str_arr);

  void restart_continue_tasks(YappServiceClient & client,vector<bool> & ret_arr,
                              const vector<string> & hndl_str_arr);

  void print_restart_result(const vector<string> & hndl_arr,
                                  vector<bool> & ret_arr);

  void pause_tasks(YappServiceClient & client, vector<bool> & ret_arr,
                                         const vector<string> & hndl_str_arr);
  void print_pause_result(const vector<string> & hndl_arr,
                                vector<bool> & ret_arr);

  void resume_tasks(YappServiceClient & client, vector<bool> & ret_arr,
                                          const vector<string> & hndl_str_arr);
  void print_resume_result(const vector<string> & hndl_arr,
                                 vector<bool> & ret_arr);

  void purge_tasks(YappServiceClient & client, vector<bool> & ret_arr,
                                         const vector<string> & hndl_str_arr);

  void print_purge_result(const vector<string> & hndl_arr,
                                vector<bool> & ret_arr);

  void print_hosts_list();

  void print_job_hndl_list(YappServiceClient & client, const vector<string> & host,
                                                       const vector<string> & owner);

  void print_job_hndl_by_host(YappServiceClient & client, const vector<string> & host);

  void print_job_hndl_by_owner(YappServiceClient & client, const vector<string> & owner);

  void print_job_hndl(YappServiceClient & client);

  void print_yappd_envs(YappServiceClient & client);

private:
  ZkClusterProxy * zkc_proxy_ptr;

  string zk_conn_str;
  YAPP_ADMIN_OPTION_ENTRY_MAPPING yapp_admin_opt_idx;
  vector<string> task_hndl_arr;

  ConfigureUtil cfg_obj;
};

}
}
#endif // YAPP_ADMIN_H_
