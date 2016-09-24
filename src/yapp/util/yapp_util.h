#ifndef YAPP_UTIL_H_
#define YAPP_UTIL_H_

#include <iostream>
#include <vector>
#include <string>
#include <pthread.h>
#include <cstring>
#include <cstdlib>
#include <climits>
#include <algorithm>
#include <sstream>
#include <map>

#include <fcntl.h>

#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>

#include "../base/yapp_base.h"

namespace yapp {
namespace util {

using std::string;
using std::vector;
using std::map;

using namespace yapp::base;

const static string YAPP_DAEMON_DEF_LOG_FD = "/dev/null";
const static string YAPP_DAEMON_DEF_TMP_DR = "/var/tmp/yappd";
const static string YAPP_DAEMON_DEF_PID_FD = "/var/run/yappd/yappd.pid";
const static string YAPP_DAEMON_DEF_ROT_PT = "";

const static int YAPP_COMMUNICATION_PORT = 9527;
const static int YAPP_DAEMON_THRD_POOL_SIZE = 32;
const static int YAPP_MASTER_SERV_MAX_QUEUED_TASK_CNT = 2048;
const static int YAPP_MASTER_SERV_MAX_JOB_SPLIT = 1000000;
const static int MAX_SESSION_TIME_OUT = 10000;
const static int YAPP_SERVICE_RFILE_TASK_SCHEDULE_POLLING_RATE_IN_SECOND = 5;
const static int YAPP_SERVICE_RESCHEDULE_POLLING_RATE_IN_SECOND = 5;
const static int YAPP_SERVICE_ZOMBIE_CHECK_POLLING_RATE_IN_SECOND = 20;
const static int YAPP_SERVICE_RFILE_TASK_AUTOSP_POLLING_RATE_IN_SECOND = 10;
const static int YAPP_SERVICE_UTILITY_THREAD_CHECKING_RATE_IN_SECOND = 30;
const static int YAPP_SERVICE_MASTER_CHECK_POLLING_RATE_IN_SECOND = 30;
const static int YAPP_SERVICE_CHECK_POINT_POLLING_RATE_IN_SECOND = 30;

const static int YAPP_SERVICE_DEF_SCHEDULE_BATCH_SIZE = 64;

const static mode_t YAPP_WORKER_SERV_HNDL_LOG_PERM =
    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;

const static int YAPP_WORKER_SERV_HNDL_LOG_MODE =
    O_APPEND | O_WRONLY | O_CREAT;

const static string MAX_LINE_STR = "_max_";
const static string CUR_HNDL_STR = "_curhnd_";
const static string CUR_LINE_STR = "_curln_";
const static string NXT_LINE_STR = "_nxtln_";
const static string TOT_PROC_STR = "_proccnt_";
const static string NEW_DELM_STR = "_delim_";

const static string JOBS_HNDL_DELM = "job-";
const static string TASK_HNDL_DELM = "_task-";
const static string PROC_HNDL_DELM = "_proc-";

const static string PROC_HOST_STR_DELM = ":";

const static int MAX_SEQ_SIZE = 32;
const static int MAX_PATH_LEN = 256;

const static char * const CONFIG_KEY_VALUE_DELIM = "=";
const static char CONFIG_COMMENTS_PREFIX = '#';
/**
 * 0000 0000 -> { 4 MSB: reserved.
 *   5th: Master, 6th: Worker, 7th: Client, 8th: Verbose
 */
const static unsigned char MASK_BIT_MASTER = 8;
const static unsigned char MASK_BIT_WORKER = 4;
const static unsigned char MASK_BIT_CLIENT = 2;
const static unsigned char MASK_BIT_VERBOS = 1;

const static char * const ELECTION_PATH = "/conf/nodes";
const static char * const ELECTION_NODE_DATA_DELIM = ":";
const static char * const ELECTION_NODE_INV_IP_V4 = "127.0.0.1";

const static string NODE_PREFX_MASTER_PREFERRED = "master-";
const static string NODE_PREFX_NORMAL_PRIORITY = "yapp-";

const static string LOCK_PREFIX_STR = "_lock";
const static string READ_LOCK_PREFIX_STR = "_lock_read-";
const static string WRITE_LOCK_PREFIX_STR = "_lock_wirte";

const static int MAX_COND_WAIT_TIME_OUT = 40;
const static int CALLBACK_RET_INVALID_VAL = INT_MIN;

const static int MAX_ELECTION_NODE_WAIT_SECS = 1;
const static int MAX_ELECTION_NODE_DATA_LEN = 256;
const static int MAX_ELECTION_NODE_INTERFACES_CNT = 16;
const static int MAX_BATCH_CHUNK = 1024;
const static int MAX_BATCH_CREATE_CHUNK = 64;
const static unsigned int BATCH_OP_NICE_TIME_IN_MICRO_SEC = 120000;

const static char * const PROC_STATUS_CODE_ENTRY[] = {
  "PROC_STATUS_NEW",
  "PROC_STATUS_READY",
  "PROC_STATUS_RUNNING",
  "PROC_STATUS_BLOCKED",
  "PROC_STATUS_TERMINATED",
};

const static char * const PROC_PRORITY_CODE_ENTRY[] = {
  "PROC_PRORITY_HIGH",
  "PROC_PRORITY_NORMAL",
  "PROC_PRORITY_LOW",
};

const static char * const TASK_INPUT_TYPE_ENTRY[] = {
  "UNSPECIFIED",
  "ID_RANGE",
  "TEXT_FILE",
  "BINARY_FILE",
  "RANGE_FILE",
  "DYNAMIC_RANGE",
};

class SystemIOUtil {
public:
  static bool setup_io_redirection(const string & stdout_file,
                                   const string & stderr_file)
  {
    int fd_stdout = -1, fd_stderr = -1;
    const char * out_fptr = stdout_file.empty() ? NULL : stdout_file.c_str();
    const char * err_fptr = stderr_file.empty() ? NULL : stderr_file.c_str();
    bool status = false;

    if (NULL == out_fptr) {
      if (0 == close(STDOUT_FILENO)) { status = true; }
    } else {
      if (0 < (fd_stdout = open(out_fptr, YAPP_WORKER_SERV_HNDL_LOG_MODE,
                                          YAPP_WORKER_SERV_HNDL_LOG_PERM))) {
        dup2(fd_stdout, STDOUT_FILENO);
        close(fd_stdout);
        status = true;
      }
    }

    /** only proceed if the setting up of stdout succeeded. **/
    if (false == status) { return status; } else { status = false; }

    if (NULL == err_fptr) {
      if (0 == close(STDERR_FILENO)) { status = true; }
    } else {
      if (0 < (fd_stderr = open(err_fptr, YAPP_WORKER_SERV_HNDL_LOG_MODE,
                                          YAPP_WORKER_SERV_HNDL_LOG_PERM))) {
        dup2(fd_stderr, STDERR_FILENO);
        close(fd_stderr);
        status = true;
      }
    }

    return status;
  }
};

class StringUtil {
public:
  /**
   * Desc:
   * - Convert an integer to a string obj.
   */
  static string convert_int_to_str(long long a);

  /**
   * Desc:
   * - check if a string is a valid integer or not.
   */
  static bool is_integer(const std::string & str);

  /**
   * Desc:
   * - Stripping all the blanks(\t, \n, '\r' or ' ') of a string for both sides,
   *   the param str passed is subjected to change(pass by ref.).
   * Params:
   * - string & str: the ref. of the new string.
   */
  static void trim_string(std::string & str);

  /**
   * Desc:
   * - This method will parse a zookeeper node path into node name and a set of
   *   dir. The name may refer to a actual node or the name of any folder, which
   *   does not matter since zookeeper treats all of them as node.
   *
   * - Since this method is intended to be used for parsing zookeeper path str,
   *   any node along the path str should never contain '/' or anything violates
   *   the zookeeper naming rule, and any string using relative path is invalid,
   *   given that the whole point of this call is turning any full zk node path
   *   into a complete, usable set of node sets to support recursive operations.
   *   ALSO, the path string DOES NOT SUPPORT ESCAPING.
   *
   *   eg. /home/xfan/projects/yapp      -> { [ home, xfan, projects], yapp }
   *       /home                         -> { [], home }
   *       /                             -> { [], '' }
   *       /zookeeper                    -> INVALID, conflicts with the keywords
   *       /home.h                       -> INVALID, '.' not allowed in zk.
   *       /home/xfan/pro\/jects/yapp    -> INVALID, contain '/'
   *       test                          -> INVALID, using relative path.
   *       (${PATH}/)?./test(/${PATH})?  -> INVALID
   *       (${PATH}/)?../test(/${PATH})? -> INVALID
   *
   * Params:
   * - const string & fpath: the path string to be parsed.
   * - vector<string> ret_dir_arr: the vector used for storing all the dirs.
   * - string & ret_node_name): the file name without all its ancestor dirs.
   * Return:
   * - true if the path string is valid, false otherwise.
   */
  static bool parse_full_zk_node_path(const string & fpath,
                                      vector<string> & ret_dir_arr,
                                      string & ret_node_name);

  /**
   * Desc:
   * - Pretty much a c++ version of basename & dirname from libgen.h, so as to
   *   leverage the power of std::string without the need of operating raw ptr.
   *   Maybe a little bit of slowing down, but definitely eliminate the pitfall
   *   of segmentation falut in the old version.
   * NOTE!!!:
   * - the path string is supposed to be both left & right trimed before being
   *   passed as parameter, and if any of end(left or right) in string contains
   *   blank, the behavior is undefined.
   * - both methods are not supposed to handle the case of escaping chars.
   */
  static string get_path_basename(const string & trimed_path);
  static string get_path_dirname (const string & trimed_path);
};

/**
 * Desc:
 * - This class would be a wrapper for all common operations towards zookeeper
 *   cluster, providing some core APIs for yapp_master & yapp_worker.
 * 
 */
class ZkClusterProxy {
public:
  /**
   * Desc:
   * - Basically a simple constructor.
   * Note:
   * - always set b_test to true when running your unit testing cases!
   */
  ZkClusterProxy(bool b_test = false, int port = YAPP_COMMUNICATION_PORT,
                 int max_qsize = YAPP_MASTER_SERV_MAX_QUEUED_TASK_CNT,
                 int max_zkto = MAX_SESSION_TIME_OUT,
                 const string & root_pt = YAPP_DAEMON_DEF_ROT_PT); 

  /**
   * Desc:
   * - Simple destructor, would do the following {
   *   - closing handle served as connection to zookeeper cluster.
   *   - destroying mutex(used together with the conditioanl variable durint
   *     call of initiating the connection).
   *   }
   */
  ~ZkClusterProxy();

  void set_zk_namespace(const string & root_pt = YAPP_DAEMON_DEF_ROT_PT) {
    root_path = root_pt;
  }

  /**
   * Desc:
   * - This method will be used for setting up a live connection(session) to the
   *   zookeeper cluster based on the connection string provided.
   * - originally the zookeeper_init returns immediately after making the call,
   *   which means the session may not be created yet(very likely) even we got a
   *   valid handle returned.
   * - The idea is to provide a blocking call so that we do not need to worry
   *   about the connection after returning from this method.
   *
   * Note:
   * - THIS IS A BLOCKING CALL FOR SETTING UP A LIVE SESSION!
   *
   * Params:
   * - const string & conn_str { zookeeper cluster connection string }
   * Returns:
   * - ret_code { YAPP_MSG_SUCCESS if everything is OK. }
   */
  YAPP_MSG_CODE init_zk_conn(const string & conn_str);

  /**
   * Basic Leader Election Logic Flow
   * - from http://zookeeper.apache.org/doc/trunk/recipes.html
   *
   * One way to do leader election with ZooKeeper is to use SEQUENCE|EPHEMERAL
   * flags when creating znodes that represent "proposals" of clients. The idea
   * is to have a znode, say "/election", such that each znode creates a child
   * znode "/election/n_" with both flags SEQUENCE|EPHEMERAL. With the sequence
   * flag, ZooKeeper automatically append a sequence number that is greater that
   * any previously appended to a child of "/election". The process that created
   * the znode with the smallest appended sequence number is the leader.
   *
   * That's not all, though. It is important to watch for failures of the leader
   * so that a new client arises as a new leader in the case the current leader
   * fails. A trivial solution is to have all application process watching upon
   * the current smallest znode and checking if they are the new leader when the
   * smallest znode goes away (note that the smallest znode will go away if the
   * leader fails because the node is ephemeral). But this causes a herd effect:
   * upon of failure of the current leader, all processes receive a notification
   * and execute getChildren on /election to obtain the current list of children
   * of "/election". If the number of clients is large, it causes a spike on the
   * number of operations that ZooKeeper servers have to process. To avoid herd
   * effect it is sufficient to watch for the next znode down on the sequence of
   * znodes. If a client receives notification that the znode it is watching is
   * gone, then it becomes the new leader in the case that there is no smaller
   * znode. Note this avoids the herd effect by not having all clients watching
   * the same znode.
   *
   * Here's the pseudo code:
   *
   * Let ELECTION be path of choice of application. To volunteer to be a leader:
   * - Create znode z with path "ELECTION/n_" with SEQUENCE and EPHEMERAL flags;
   * - Let C be the children of "ELECTION", and i be the sequence number of z;
   * - Watch for changes on ELECTION/n_j, where j is the largest sequence number
   *   such that j < i and n_j is a znode in C;
   *
   * Upon receiving a notification of znode deletion:
   * - Let C be the new set of children of ELECTION;
   * - If z is the smallest node in C, then execute leader procedure;
   *   Otherwise, watch for changes on "ELECTION/n_j", where j is the largest
   *   sequence number such that j < i and n_j is a znode in C;
   * - Note that a znode having no preceding znode on the list of children does
   *   not imply that the creator of this znode is aware that it is the current
   *   leader. Application may consider creating a separate znode to acknowledge
   *   that the leader has executed the leader procedure.
   * Params:
   * - bool b_master_pref {
   *   - This variable is to mark if a certain process is preferred to win the
   *     election when other processes have no such priority sat, otherwise it
   *     depends on the sequence id given by zookeeper cluster for tie-breaking,
   *     thread having higher priority with lower seq number(if we have other
   *     thread with same priority) will win the election.
   *   }
   * Return:
   * - YAPP_MSG_CODE {
   *   YAPP_MSG_SUCCESS if everything is ok, YAPP_MSG_INVALID otherwise;
   * }
   */
  YAPP_MSG_CODE run_master_election(bool b_master_pref = false);

  /**
   * Desc:
   * - This method is only supposed to be used after successfully creating the 
   *   election node or re-detecting the change of the master node.
   */
  YAPP_MSG_CODE config_node_info();

  /**
   * Desc:
   * - This func will test if a node exists.
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if exist, YAPP_MSG_INVALID_NONODE if not.
   *             YAPP_MSG_INVALID for general problem(network, session...)
   */
  YAPP_MSG_CODE node_exists(const string & node_path);

  /**
   * Desc:
   * - This func will delete a node using the full path specified from zookeeper
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   */
  YAPP_MSG_CODE del_node(const string & node_path);

  /**
   * TODO:
   * - unit test
   * Desc:
   * - This func will perform the batch delete over a single tcp channel.
   * Params:
   * - const vector<string> & path_arr: the full path for the node to delete.
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   * NOTE!!!
   * - ALL NODE TO BE DELETED SHOULD NOT CONTAIN ANY CHILD, FOR RECURSIVE DELETE
   *   API FOR SINGLE NODE, PLEASE LOOK FOR delete_node_recur
   * - THIS WILL DEL ALL THINGS IN ONE BATCH TRXN.
   */
  YAPP_MSG_CODE batch_delete(const vector<string> & path_arr);

  /**
   * Desc:
   * - This func will create a node using the given params in zookeeper cluster.
   * Params:
   * - const string & path_str: the full path for the node to be created.
   * - const string & data_str: the data for the node to be created.
   * - int node_flag: flags(ZOO_SEQUENCE|ZOO_EPHEMERAL for leader election).
   * Return:
   * - string & path_created: full path for the node gets created(pass by ref.)
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   * NOTE!!!
   * - THIS METHOD ASSUMES THAT THE PARENT FOLDER ALWAYS EXISTED! FOR RECURSIVE
   *   CREATE API, PLEASE LOOK FOR create_node_recur
   */
  YAPP_MSG_CODE create_node(const string & path_str,
                            const string & data_str,
                            string & path_created,
                            int node_flag = 0);

  /**
   * TODO:
   * - unit test
   * Desc:
   * - This func will perform the batch create over a single tcp channel.
   * Params:
   * - const vector<string> & path_arr: the full path for the node to be created.
   * - const vector<string> & data_arr: the data for the node to be created.
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   * NOTE!!!
   * - THIS METHOD ASSUMES THAT THE PARENT FOLDER ALWAYS EXISTED! FOR RECURSIVE
   *   CREATE API, PLEASE LOOK FOR create_node_recur
   * - IT ONLY SUPPORTS NORMAL NODE NO ZOO_SEQUENCE OR ZOO_EPHEMERAL!
   */
  YAPP_MSG_CODE batch_create_atomic(const vector<string> & path_arr,
                                    const vector<string> & data_arr);
  YAPP_MSG_CODE batch_create(const vector<string> & path_arr,
                             const vector<string> & data_arr);


  /**
   * Desc:
   * - This method almost does the same thing compared to create_node api, the
   *   only DIFFERENCE is that it will create the parent folder/node as needed.
   *   This would be very useful every time creating & logging a new task.
   * Params:
   * - const string & path_str: the full path for the node to be created.
   * - const string & data_str: the data for the node to be created.
   * - int node_flag: flags(ZOO_SEQUENCE|ZOO_EPHEMERAL for leader election).
   * Return:
   * - string & path_created: full path for the node gets created(pass by ref.)
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   * NOTE!!!
   * - This method is not supposed to be thread-safe and it won't acquire any
   *   global lock to provide any degree of isolation to run the whole thing as
   *   a general transaction (YAPP runs single master, which could handle this
   *   by doing any needed synchronization within the process scope).
   */
  YAPP_MSG_CODE create_node_recur(const string & path_str,
                                  const string & data_str,
                                  string & path_created,
                                  int node_flag = 0);
  /**
   * Desc:
   * - This method will print all children node(not full path) with a given root
   *   (all descendants & itself), based on ASC sorted post-order treversal.
   * - would be useful for debugging.
   */
  YAPP_MSG_CODE print_node_recur(string & node_tree_ret,
                                 const string & root_path,
                                 bool b_show_node_value = false,
                                 bool b_show_full_path = false);

  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - This method will print all the necessary queuing info. for the given hndl
   *   (either job, task or a simple proc. handle).
   * - Note that this method is not a dup. of the method above:
   *   -- print_node_recur
   *   since the tracing level and the paths needs to print are already known,
   *   and they all defined as constants in file ../base/yapp_base.h.
   * - It also supports the adding of the filters for leaf nodes so that only
   *   those procs we want in the queue will be displayed.
   * Params:
   * - string & node_tree_ret: the results returned as a str(pass by ref.)
   * - const string & root_path: the job, task or proc we interested in.
   * - const string & leaf_filter_to_disp: {
   *   - only those procs. whose handle contains leaf_filter_to_disp its prefix
   *     will gets displayed(essentailly a filter).
   *   }
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   */
  YAPP_MSG_CODE print_queue_stat(string & node_tree_ret,
                           const string & leaf_filter_to_disp,
                           bool is_display_failed_only = false);

  /**
   * Desc:
   * - This method will grab every node's full path under a given root(all
   *   descendants & itself), based on ASC sorted post-order treversal.
   */
  YAPP_MSG_CODE get_node_arr_recur_post_asc(const string & root_path,
                                            vector<string> & nodes_array);

  /**
   * Desc:
   * - This method will grab every node's full path under a given root(all
   *   descendants & itself), based on DESC sorted pre-order treversal.
   */
  YAPP_MSG_CODE get_node_arr_recur_pre_desc(const string & root_path,
                                            vector<string> & revr_nodes_array);


  /**
   * Desc:
   * - This method will purge everything for a node(all descendants & itself),
   *   and deletion will be performed using ASC sorted post-order treversal.
   * Params:
   * - const string & path_str: the full path for the node to delete.
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   * NOTE!!!
   * - This method is not supposed to be thread-safe and it won't acquire any
   *   global lock to provide any degree of isolation to run the whole thing as
   *   a general transaction (YAPP runs single master, which could handle this
   *   by doing any needed synchronization within the process scope).
   */
  YAPP_MSG_CODE delete_node_recur(const string & path_str);

  /**
   * Desc:
   * - This method will purge everything for a job(includes job nodes, current
   *   job related information in the queue) and deletion will be performed
   *   using ASC sorted post-order treversal.
   * Params:
   * - const string & job_hndl: job handle for a job node(short form)
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   * NOTE!!!
   * - This method is not supposed to be thread-safe and it won't acquire any
   *   global locks if needed, simply a raw batch deletion api for all nodes
   *   related to a specific job handle.
   */
  YAPP_MSG_CODE purge_job_info(const string & job_hndl);

  /**
   * Desc:
   * - This method is impelemented specific for leader election, which is going
   *   to create a node used for election and maintaining heart-beat information
   *   only, no other thread or process should touch the node.
   * Params:
   * - bool b_master_pref {
   *   - This variable is to mark if a certain process is preferred to win the
   *     election when other processes have no such priority sat, otherwise it
   *     depends on the sequence id given by zookeeper cluster for tie-breaking,
   *     thread having higher priority with lower seq number(if we have other
   *     thread with same priority) will win the election.
   *   }
   * Return:
   * - string & path_created: the full path for the node created.
   * - string & data_str: the data string for the node created.
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   */
  YAPP_MSG_CODE create_election_node(string & path_created,
                                     string & data_created,
                                     bool b_master_pref = false);

  /**
   * Desc:
   * - return all child nodes associated under a certain node speicified by a
   *   full zookeeper path, results would be sorted in ASC. order.
   * Params:
   * - const string & dir_path: full zookeeper path for the node.
   * Return:
   * - vector<string> & node_arr: child nodes list(pass by ref.)
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   * Note:
   * - ALL CHILD NODES RETURNED INCLUDE NODE NAME ONLY, NOT A FULL, VALID PATH!
   */
  YAPP_MSG_CODE get_node_arr(const string & dir_path,
                             vector<string> & node_arr);

  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - set data associated with the node speicified by a full zookeeper path.
   * Params:
   * - const string & node_path: full zookeeper path for the node.
   * - const string & node_data: data string the node.
   * - int version: the version for the node to be checked.
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   */
  YAPP_MSG_CODE set_node_data(const string & node_path,
                              const string & node_data, int version = -1);

  /**
   * TODO:
   * - unit test
   * Desc:
   * - This func will perform the batch set over a single tcp channel.
   * Params:
   * - const vector<string> & path_arr: the full path for the node to set.
   * - const vector<string> & data_arr: the data for the node to set.
   * Return:
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   * NOTE!!!
   * - THIS METHOD ASSUMES THAT THE NODE PATH WILL ALWAYS EXISTED! FOR RECURSIVE
   *   CREATE API, PLEASE LOOK FOR create_node_recur
   * - IT ONLY SUPPORTS NODE SETTING WITHOUT VERSION CHECKING!
   * - SUPPOSE TO BE ATOMIC & TRANSACTIONAL
   */
  YAPP_MSG_CODE batch_set(const vector<string> & path_arr,
                          const vector<string> & data_arr);

  YAPP_MSG_CODE batch_set_create_and_delete(
    const vector<string> & upd_path_arr, const vector<string> & upd_data_arr,
    const vector<string> & path_to_cr_arr, const vector<string> & data_to_cr_arr,
    const vector<string> & path_to_del
  );

  /**
   * Desc:
   * - return data associated with the node speicified by a full zookeeper path.
   * Params:
   * - const string & node_path: full zookeeper path for the node.
   * Return:
   * - string & data: the data associated(pass by ref.)
   * - struct Stat * stat_ptr: the pointer to mem holding metadata for the node.
   * - ret_code: YAPP_MSG_SUCCESS if everything is ok.
   */
  YAPP_MSG_CODE get_node_data(const string & node_path, string & data,
                              struct Stat * stat_ptr = NULL);

  /**
   * Desc:
   * - Return the mode of current running thread/process, worker or master.
   */
  YAPP_RUNNING_MODE_CODE get_yapp_mode() { return yapp_mode_code; }

  /**
   * Desc:
   * - Return the complate node path of current session.
   */
  string get_node_path() { return self_node_path; }

  /**
   * Desc:
   * - This method would get called to get and refresh the current info. about
   *   the info. used to communicate with the yapp master instance.
   * Params:
   * Return:
   * - string & ret_host, string & ret_port }-+
   *   string & ret_pcid                    }-+ pass by reference as ret value
   * - ret_val: YAPP_MSG_SUCCESS if everything is ok,
   *            YAPP_MSG_INVALID_MASTER_STATUS if no master node is online.
   */
  YAPP_MSG_CODE get_yapp_master_addr(string & ret_host,
                                     string & ret_port,
                                     string & ret_pcid);

  /**
   * Desc:
   * - This method is amied for providing a blocking primitive when a sync. op.
   *   is required for a certain node before applying certain logics, which will
   *   block until the leader node has flushed its 'current' node version to all
   *   the other followers.
   * Params:
   * - const string & path_to_sync: path of the node to sync.
   * Return:
   * - ret_val: YAPP_MODE_SUCCESS if ok.
   */
  YAPP_MSG_CODE sync(const string & path_to_sync);

/**
 * Following part contains All Primitives for Distributed Locking Implementation
 * Also note that user should be careful to use locking given its big overhead
 * on communication, especially for those blocking calls(api does not start with
 * try_), since they may block for a unbounded period of time. The main idea is
 * to create a SEQ|EMP node like /${node_to_lock/_lock/read-0001 as READ LOCK or
 * /${node_to_lock/_lock/write-0001 as WRITE LOCK.
 */
public:
  /**
   * TODO
   * Desc:
   * - This method will try to obtain the RL without blocking, returns either
   *   indicating grabbed the RL (YAPP_MSG_SUCCESS) or failed (by indicating
   *   YAPP_MSG_INVALID_FAILED_GRAB_RLOCK.
   */ 
  YAPP_MSG_CODE try_acquire_read_lock(const string & fpath_to_lock,
                                            string & lock_hnd);

  /**
   * TODO
   * Desc:
   * - This method will try to obtain the WL without blocking, returns either
   *   indicating grabbed the WL (YAPP_MSG_SUCCESS) or failed (by indicating
   *   YAPP_MSG_INVALID_FAILED_GRAB_WLOCK
   */ 
  YAPP_MSG_CODE try_acquire_write_lock(const string & fpath_to_lock,
                                             string & lock_hnd);

  /**
   * TODO
   * Desc:
   * - This method will try to obtain the RL using blocking, returns either
   *   indicating grabbed the RL (YAPP_MSG_SUCCESS) or failed (by indicating
   *   other errors.
   */ 
  YAPP_MSG_CODE acquire_read_lock(const string & fpath_to_lock,
                                                 string & lock_hnd);

  /**
   * TODO
   * Desc:
   * - This method will try to obtain the WL using blocking, returns either
   *   indicating grabbed the RL (YAPP_MSG_SUCCESS) or failed (by indicating
   *   other errors.
   */
  YAPP_MSG_CODE acquire_write_lock(const string & fpath_to_lock,
                                                  string & lock_hnd);

  /**
   * TODO
   * Desc:
   * - This method will try to release the lock, either indicating released lock
   *  (YAPP_MSG_SUCCESS) or failed (YAPP_MSG_INVALID_FAILED_RELEASE_LOCK)
   */
  YAPP_MSG_CODE release_lock(const string & lock_hnd);

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This method tries to combine the operation of releasing the EX lock and
   *   delete the node using one single atomic operation.
   */
  YAPP_MSG_CODE release_lock_and_delete(const string & node_path);

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This method tries to combine the operation of releasing multi EX lock and
   *   delete nodes set using one single atomic operation.
   */ 
  YAPP_MSG_CODE release_lock_and_delete(const vector<string> & node_path_arr);

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This method would try to create a new node & apply the write lock on it
   *   in 1 single atomic operation.
   */
  YAPP_MSG_CODE create_and_acquire_ex_lock(const string & node_path,
                                           const string & data_str);

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This method would try to create a new node & apply the write lock on it
   *   in 1 single atomic operation.
   * - Deprecated.
   */
  YAPP_MSG_CODE create_seq_node_and_acquire_ex_lock(
    string & path_ret, string & lock_hnd, const string & node_path,
                                          const string & data_str
  );

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This method would try to create a set of new nodes & apply write lock on
   *   each of them in 1 single atomic operation.
   */
  YAPP_MSG_CODE create_and_acquire_ex_lock(const vector<string> & path_arr,
                                           const vector<string> & data_arr);

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This method would immitate the action of 'mv' in a file system using 1
   *   single batch operation, with an ex lock on the new node(only for nodes
   *   having no child)
   */
  YAPP_MSG_CODE move_and_ex_lock_node_with_no_children(
    const string & path_from, const string & path_to,
    const string & node_data,       string & lock_hnd
  );

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This method would immitate the action of 'mv' in a file system using 1
   *   single batch operation, only for the nodes having no child.
   */
  YAPP_MSG_CODE move_node_with_no_children(
    const string & path_from, const string & path_to,
    const string & data,      const string & lock_to_del
  );

  YAPP_MSG_CODE move_node_with_no_children(
    const string & path_from, const string & path_to, const string & node_data
  );

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This method would immitate the action of 'mv' in a file system using 1
   *   single batch operation, only for the nodes having no child.
   */
  YAPP_MSG_CODE batch_move_node_with_no_children(
    const vector<string> & path_from_arr, const vector<string> & path_to_arr,
    const vector<string> & data_arr
  );

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This method would immitate the action of 'mv' in a file system using 1
   *   single batch operation, only for the nodes having no child, plus the
   *   extra create and delete ops.
   */
  YAPP_MSG_CODE batch_move_node_with_no_children_and_create_del(
    const vector<string> & path_from_arr, const vector<string> & path_to_arr,
    const vector<string> & data_arr,      const vector<string> & path_to_cr_arr,
    const vector<string> & data_to_cr_arr,const vector<string> & path_to_del
  );

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This API is intended for marking running range file tasks as terminated,
   *   since those terminated ones needs to be stored in different queues.
   */
  YAPP_MSG_CODE batch_unlock_delete_and_create(
    const vector<string> & nodes_to_del, const vector<string> & nodes_to_cr
  );

  /**
   * TODO
   * - unit testing
   * Desc:
   * - This API is intended to be used to fully restart a set of processes.
   */
  YAPP_MSG_CODE batch_unlock_delete_and_create_and_set(
    const vector<string> & nodes_to_del, const vector<string> & nodes_to_cr,
    const vector<string> & nodes_to_set, const vector<string> & data_to_set
  );
public:
  /**
   * TODO:
   * - unit testing.
   * Desc:
   * - This method will return the host_str for current node running yapp worker:
   *   ==>> ${node_path}:${host_addr}:${yapp_port}:${yapp_pid}
   * NOTE:
   * - only call this method after successful return of run_master_election()!
   */
  string get_host_str_in_pcb() {
    return self_node_path + ELECTION_NODE_DATA_DELIM + self_node_valu;
  }

  string get_node_path_prefix() {
    return root_path + (b_testing ? YAPP_CONF_NODES_PATH_FOR_TEST : YAPP_CONF_NODES_PATH);
  }

  string get_queue_path_prefix() {
    return root_path + (b_testing ? YAPP_QUEUED_TASK_PATH_FOR_TEST : YAPP_QUEUED_TASK_PATH);
  }

  string get_failed_task_queue_path_prefix() {
    return root_path + (b_testing ? YAPP_QUEUED_FAILED_TASK_PATH_FOR_TEST :
                                    YAPP_QUEUED_FAILED_TASK_PATH);
  }

  string get_rfile_task_queue_path_prefix() {
    return root_path + (b_testing ? YAPP_QUEUED_RANGE_FILE_TASK_PATH_FOR_TEST :
                                    YAPP_QUEUED_RANGE_FILE_TASK_PATH);
  }

  string get_newtsk_queue_path_prefix() {
    return root_path + (b_testing ? YAPP_QUEUED_NEW_TASK_PATH_FOR_TEST :
                                    YAPP_QUEUED_NEW_TASK_PATH);
  }

  string get_ready_queue_path_prefix() {
    return root_path + (b_testing ? YAPP_QUEUED_READY_TASK_PATH_FOR_TEST :
                                    YAPP_QUEUED_READY_TASK_PATH);
  }

  string get_running_queue_path_prefix() {
    return root_path + (b_testing ? YAPP_QUEUED_RUNNING_TASK_PATH_FOR_TEST :
                                    YAPP_QUEUED_RUNNING_TASK_PATH);
  }

  string get_paused_queue_path_prefix() {
    return root_path + (b_testing ? YAPP_QUEUED_PAUSED_TASK_PATH_FOR_TEST :
                                    YAPP_QUEUED_PAUSED_TASK_PATH);
  }

  string get_terminated_queue_path_prefix() {
    return root_path + (b_testing ? YAPP_QUEUED_TERMIN_TASK_PATH_FOR_TEST :
                                    YAPP_QUEUED_TERMIN_TASK_PATH);
  }

  string get_job_path_prefix() {
    return root_path + (b_testing ? YAPP_JOB_PATH_FOR_TEST : YAPP_JOB_PATH);
  }

  string get_job_full_path(const string & job_hnd) {
    return get_job_path_prefix() + "/" + job_hnd;
  }

  string get_task_full_path(const string & task_hnd) {
    size_t pos = task_hnd.find("_");
    if (string::npos == pos) { return ""; }
    string job_hnd = task_hnd.substr(0, pos);
    return get_job_full_path(job_hnd) + "/task_arr/" +
           task_hnd.substr(pos + 1);
  }

  string get_proc_full_path(const string & proc_hnd) {
    size_t pos = proc_hnd.rfind("_");
    if (string::npos == pos) { return ""; }
    string task_hnd = proc_hnd.substr(0, pos);
    return get_task_full_path(task_hnd) + "/proc_arr/" +
           proc_hnd.substr(pos + 1);
   
  }

  string get_task_hnd_by_proc_hnd(const string & proc_hnd) {
    size_t pos = proc_hnd.rfind("_");
    if (string::npos == pos) { return ""; }
    return proc_hnd.substr(0, pos);
  }

  string get_master_host_str() { return master_host_str; }
  string get_master_port_str() { return master_port_str; }
  string get_master_proc_pid() { return master_proc_pid; }

  /**
   * Desc:
   * - This method will return actual path(or folder) used for leader election.
   * Return:
   * - full path for leader election.
   */
  string get_election_path();

  /**
   * Desc:
   * - Everytime the zk api gets called, it will 1st try to obtain a copy of the
   *   address for that zookeeper handle in a safe way to avoid some possible
   *   race condition (may happen during a session timeout and the zk handle get
   *   updated while some other threads still running the api)
   */
  zhandle_t * get_cur_zkptr_atomic();

  /**
   * Desc:
   * - Provides a safe way to update the zookeeper handle when needed.
   */
  bool set_cur_zkptr_atomic(zhandle_t * new_zk_ptr);

  /**
   * Desc:
   * - This method will parse the data string associated with a election node.
   * Params:
   * - const string & data_str: the data string of a election node.
   * Return(all by ref.):
   * - string & out_host: host ip of current running node.
   * - string & out_port: communication port of current process(reserved)
   * - string & out_pidn: id for current process(or thread for unit testing).
   */
  static YAPP_MSG_CODE parse_election_node_data_str(const string & data_str,
                                                          string & out_host,
                                                          string & out_port,
                                                          string & out_pidn);
  /**
   * This method will return primary IPv4 non-local ip(any ip address other than
   * 127.0.0.1) associated with any local interface used in the LAN.
   */
  static string get_ip_addr_v4_lan();
protected:
  /**
   * Desc:
   * - following 2 methods will be used for sorting full zookeeper paths list.
   */
  static int vstrcmp(const void* str1, const void* str2);
  static void sort_child_nodes_arr(struct String_vector * child_nodes_arr);

  /**
   * Desc:
   * - callback used for initiating a connection to zookeeper cluster.
   * - basically a implemenation for the interface defined as a watcher_fn in
   *   zookeeper.h.
   */
  static void zk_init_callback(zhandle_t * zk_ptr, int type, int state,
                               const char * path,  void * zk_proxy_ptr);
  /**
   * Desc:
   * - Callback used for implementing the sync method.
   */
  static void zk_sync_callback(int ret_val, const char * ret_str,
                                            const void * zk_proxy_ptr);

  /**
   * Desc:
   * - Callback (or watcher function) for dealing with the nodes management for
   *   all nodes consists up the yappd service. Note that this will only be a
   *   one time callback such that it needs to be set everytime before using.
   */
  static void node_change_watcher(zhandle_t *zh, int type, int state,
                                  const char * path, void * ctx);
private:
  vector<zhandle_t *> closed_zk_ptr_arr;
  zhandle_t * cur_zk_ptr;
  string zk_conn_str;

  pthread_cond_t cond_var;
  pthread_mutex_t zk_mutex;
  int callback_ret_val;

  pthread_rwlock_t zk_ptr_latch;

  YAPP_RUNNING_MODE_CODE yapp_mode_code; 
  string self_node_path;
  string self_node_valu;

  string master_node_path;
  string master_host_str;
  string master_port_str;
  /** process id of the daemon running on master node, thread id if testing. */
  string master_proc_pid;
  /**
   * - port_num: the port number used for communication in YAPP,
   *   -- master would listen on this port for accepting client job submission.
   *   -- worker would listen on this port for receiving messages from master.
   */
  int port_num;

  int max_zkc_timeout;
  int max_queued_task;

  string root_path;

  bool b_testing;
};
 
/**
 * Desc:
 * - This class will be used as the central point for handling queries related
 *   to the configuration(set at /config/yapp.cfg), includes: {
 *   1. server host & port list for locating the zookeeper cluster.
 *   }
 */
const static char * const YAPP_DAEMON_CFG_FILE_OPTION_ENTRY[] = {
  "zkserver", "port=", "thrd_pool_size=", "max_queued_task=", "max_auto_job_split=",
  "mode=", "max_zkc_timeout=", "range_file_task_scheule_polling_rate_sec=",
  "subtask_scheule_polling_rate_sec=", "zombie_check_polling_rate_sec=",
  "rftask_autosplit_polling_rate_sec=", "utility_thread_checking_rate_sec=",
  "master_check_polling_rate_sec=", "check_point_polling_rate_sec=",

  "batch_task_schedule_limit=",

  "fencing_script_path=", "yappd_log_stdout=", "yappd_log_stderr=",
  "yappd_tmp_folder=", "pid_file=", "root_path=", "verbose=",
};

enum YAPP_DAEMON_CFG_FILE_OPTION_ENTRY_MAPPING {
  YAPP_DAEMON_CFG_FILE_OPTION_ZKC_SERV,
  YAPP_DAEMON_CFG_FILE_OPTION_PORT,
  YAPP_DAEMON_CFG_FILE_OPTION_THRD_POOL_SIZE,
  YAPP_DAEMON_CFG_FILE_OPTION_MAX_QUEUED_TSK,
  YAPP_DAEMON_CFG_FILE_OPTION_MAX_JOB_SPLIT,
  YAPP_DAEMON_CFG_FILE_OPTION_RUN_MODE,
  YAPP_DAEMON_CFG_FILE_OPTION_ZKC_TIMEOUT,

  YAPP_DAEMON_CFG_FILE_OPTION_REF_SCHEDULE_RATE,
  YAPP_DAEMON_CFG_FILE_OPTION_TSK_SCHEDULE_RATE,
  YAPP_DAEMON_CFG_FILE_OPTION_ZBM_CHECKING_RATE,
  YAPP_DAEMON_CFG_FILE_OPTION_REF_AUTOSPLT_RATE,
  YAPP_DAEMON_CFG_FILE_OPTION_UTL_CHECKING_RATE,
  YAPP_DAEMON_CFG_FILE_OPTION_MAS_CHECKING_RATE,
  YAPP_DAEMON_CFG_FILE_OPTION_LOG_CHECKING_RATE,

  YAPP_DAEMON_CFG_FILE_OPTION_BATCH_SCHEDULE_LIMIT,

  YAPP_DAEMON_CFG_FILE_OPTION_FENCING_SCRIPT_PATH,
  YAPP_DAEMON_CFG_FILE_OPTION_LOG_STDOUT,
  YAPP_DAEMON_CFG_FILE_OPTION_LOG_STDERR,
  YAPP_DAEMON_CFG_FILE_OPTION_TMP_FOLDER,
  YAPP_DAEMON_CFG_FILE_OPTION_PID_FILE,
  YAPP_DAEMON_CFG_FILE_OPTION_ROOT_PATH,

  YAPP_DAEMON_CFG_FILE_OPTION_VERBOSE,
  YAPP_DAEMON_CFG_FILE_OPTION_COUNT,
};

class ConfigureUtil {
public:
  class RangePair {
  public:
    RangePair(int start, int end) { start_id = start; end_id = end; }
    int start_id, end_id;
  };
  ConfigureUtil() {
    port_num = YAPP_COMMUNICATION_PORT;
    thrd_pool_size = YAPP_DAEMON_THRD_POOL_SIZE;
    max_queued_task = YAPP_MASTER_SERV_MAX_QUEUED_TASK_CNT;
    max_auto_job_split = YAPP_MASTER_SERV_MAX_JOB_SPLIT;
    max_zkc_timeout = MAX_SESSION_TIME_OUT;

    rftask_scheule_polling_rate_sec =
      YAPP_SERVICE_RFILE_TASK_SCHEDULE_POLLING_RATE_IN_SECOND;
    subtask_scheule_polling_rate_sec =
      YAPP_SERVICE_RESCHEDULE_POLLING_RATE_IN_SECOND;
    zombie_check_polling_rate_sec =
      YAPP_SERVICE_ZOMBIE_CHECK_POLLING_RATE_IN_SECOND;
    rftask_autosp_polling_rate_sec =
      YAPP_SERVICE_RFILE_TASK_AUTOSP_POLLING_RATE_IN_SECOND;
    utility_thread_checking_rate_sec =
      YAPP_SERVICE_UTILITY_THREAD_CHECKING_RATE_IN_SECOND;   

    yappd_pid_file   = YAPP_DAEMON_DEF_PID_FD;
    yappd_root_path  = YAPP_DAEMON_DEF_ROT_PT;
    yappd_log_stdout = YAPP_DAEMON_DEF_LOG_FD;
    yappd_log_stderr = YAPP_DAEMON_DEF_LOG_FD;
    yappd_tmp_folder = YAPP_DAEMON_DEF_TMP_DR;

    master_check_polling_rate_sec =
      YAPP_SERVICE_MASTER_CHECK_POLLING_RATE_IN_SECOND;
    check_point_polling_rate_sec =
      YAPP_SERVICE_CHECK_POINT_POLLING_RATE_IN_SECOND;

    batch_task_schedule_limit = YAPP_SERVICE_DEF_SCHEDULE_BATCH_SIZE;

    b_verbose = true;
    yapp_mode_code = YAPP_MODE_WORKER;
  }

  static bool split_range_by_fixed_step(int id_from, int id_to, int step,
                                        vector<RangePair> & pair_arr_ret);

  /**
   * Desc:
   * - This method will be used for loading & parsing the configuration file to
   *   get the hosts and ports array 
   */
  bool load_zk_cluster_cfg(const string & file_path);
  string get_zk_cluster_conn_str();
  string generate_zk_cluster_conn_str();

  map<string, string> get_envs_map(bool b_master = true) {
    map<string, string> kv_env_map;

    kv_env_map["y_namespace: "] = yappd_root_path;
    kv_env_map["server mode: "] = b_master ? "master" : "worker";
    kv_env_map["zkconn host: "] = zk_conn_str;
    kv_env_map["commun port: "] = StringUtil::convert_int_to_str(port_num);
    kv_env_map["daemon pidf: "] = yappd_pid_file;
    kv_env_map["daemon tmpf: "] = yappd_tmp_folder;
    kv_env_map["stdout logf: "] = yappd_log_stdout;
    kv_env_map["stderr logf: "] = yappd_log_stderr;
    kv_env_map["maxi zktmout secs: "] = StringUtil::convert_int_to_str(max_zkc_timeout);
    kv_env_map["maxi queuing size: "] = StringUtil::convert_int_to_str(max_queued_task);
    kv_env_map["maxi j-split limt: "] = StringUtil::convert_int_to_str(max_auto_job_split);
    kv_env_map["maxi running task: "] = StringUtil::convert_int_to_str(thrd_pool_size);

    if (true == b_master) {
      kv_env_map["task polling rate: "] = StringUtil::convert_int_to_str(rftask_scheule_polling_rate_sec);
      kv_env_map["task schedul rate: "] = StringUtil::convert_int_to_str(subtask_scheule_polling_rate_sec);
      kv_env_map["task autospl rate: "] = StringUtil::convert_int_to_str(rftask_autosp_polling_rate_sec);
      kv_env_map["zomb chcking rate: "] = StringUtil::convert_int_to_str(zombie_check_polling_rate_sec);
      kv_env_map["thrd chcking rate: "] = StringUtil::convert_int_to_str(utility_thread_checking_rate_sec);
      kv_env_map["node chcking rate: "] = StringUtil::convert_int_to_str(master_check_polling_rate_sec);
      kv_env_map["logs chk-pnt rate: "] = StringUtil::convert_int_to_str(check_point_polling_rate_sec);
      kv_env_map["task schdule limt: "] = StringUtil::convert_int_to_str(batch_task_schedule_limit);
      kv_env_map["fenc scripts path: "] = fencing_script_path;
    }
    return kv_env_map;
  }

  vector<ZkServer> zk_srv_host_arr;
  int port_num;
  int thrd_pool_size;
  int max_queued_task;
  int max_auto_job_split;
  int max_zkc_timeout;

  int rftask_scheule_polling_rate_sec;
  int subtask_scheule_polling_rate_sec;
  int zombie_check_polling_rate_sec;
  int rftask_autosp_polling_rate_sec;
  int utility_thread_checking_rate_sec;

  int master_check_polling_rate_sec;
  int check_point_polling_rate_sec;

  int batch_task_schedule_limit;

  string fencing_script_path;
  string yappd_log_stdout;
  string yappd_log_stderr;
  string yappd_pid_file;
  string yappd_root_path;
  string yappd_tmp_folder;

  bool b_verbose;
  string zk_conn_str;
  YAPP_RUNNING_MODE_CODE yapp_mode_code;
};

class IPC_CONTEX {
public:
  /** Named Semaphore for Protecting Shared Memory **/
  static string YAPPD_SHARED_MUTEX_NAME;
  /** Named Semaphore for Notifying the Arrival of Signals **/
  static string YAPPD_SHARED_CONDV_NAME;

  static void init_ipc_contex();
};

} // namespace util
} // namespace yapp

#endif
