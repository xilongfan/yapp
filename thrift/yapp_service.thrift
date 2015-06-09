/**
 * Desc:
 * - A job contains a set of tasks needs to be executed in certain order(either
 *   fully serialized or partial ordering will be fine). For every task, we want
 *   to run as many processes as possible, and the topological ordering for all
 *   tasks specified needs to be guaranteed.
 * 
 * - Eg. rebuild_reverse_address.sh
 *   job_begin
 *     1. rebuild table reverse_tmp_addresses.
 *     barrier_block;
 *     2. rebuild table reverse_tmp_address_street_datas; 
 *     3. rebuild table reverse_tmp_address_neighborhoods;
 *     barrier_block;
 *     4. swapping current table with tmp table.
 *   job_end
 *
 * - Following shows the important files will be generated {
 *   1. src/yapp/domain/Task.cpp
 *   1. src/yapp/domain/RangeTask.cpp
 *   2. src/yapp/domain/FileTask.cpp
 *   3. src/yapp/domain/yapp_service_constants.cpp
 *   4. src/yapp/domain/yapp_service_types.cpp
 *   }
 *
 * - This .thrift file is intended to define the class for task(or processing
 *   unit used by yapp) using IDL provided by Thrift so as to simplify the
 *   serialization and possible RPC calls between YAPP client <-> YAPP master
 *   or YAPP master <-> YAPP worker.
 *
 * - All .thrift files(under thrift folder) will be included in the tarball for
 *   distribution and will be compiled into several .h and .cpp files after
 *   user runnning the ./configure to generate necessary Makefiles.
 *
 * - Appendix for types in Thrift IDL {
 *     bool        Boolean, one byte
 *     byte        Signed byte
 *     i16         Signed 16-bit integer
 *     i32         Signed 32-bit integer
 *     i64         Signed 64-bit integer
 *     double      64-bit floating point value
 *     string      String
 *     binary      Blob (byte array)
 *     map<t1,t2>  Map from one type to another
 *     list<t1>    Ordered list of one type
 *     set<t1>     Set of unique elements of one type
 *   }
 *
 * - Note that for simplicity, only rules for CPP projects are defined.
 */

namespace cpp yapp.domain

enum PROC_STATUS_CODE {
  PROC_STATUS_NEW = 0,
  PROC_STATUS_READY = 1,
  PROC_STATUS_RUNNING = 2,
  PROC_STATUS_BLOCKED = 3,
  PROC_STATUS_TERMINATED = 4,
}

enum PROC_PRORITY_CODE {
  PROC_PRORITY_HIGH = 0,
  PROC_PRORITY_NORMAL = 1,
  PROC_PRORITY_LOW = 2,
}

enum TASK_INPUT_TYPE {
  UNSPECIFIED = 0,
  ID_RANGE = 1,
  TEXT_FILE = 2,
  BINARY_FILE = 3,
  RANGE_FILE = 4,
  DYNAMIC_RANGE = 5,
}

enum TASK_ATTR_MASK {
  UNSPECIFIED = 0,
  AUTO_SPLIT = 2,
  AUTO_MIGRATE = 4,
}
/**
 * Used as their default value to indicate of the N/A status when the process
 * is still running that task(not terminated yet), or be on the top task tree
 */
const i32    PROC_CTRL_BLCK_FRESH_START        = 0
const i32    PROC_CTRL_BLCK_ANCHR_START        = 1

const i32    PROC_CTRL_BLCK_RET_FAIL_SET_IORED = 127
const i32    PROC_CTRL_BLCK_RET_FAIL_CHANGEDIR = 126
const i32    PROC_CTRL_BLCK_RET_FAIL_GENRNGFLE = 125

const i32    PROC_CTRL_BLCK_DEFAULT_RETURN_VAL = 2147483647
const i32    PROC_CTRL_BLCK_DEFAULT_TERMIN_SIG = 2147483647
const i32    PROC_CTRL_BLCK_DEFAULT_RESOUR_USG = -1
const i32    PROC_CTRL_BLCK_DEFAULT_PROCESS_ID = -1
const i64    PROC_CTRL_BLCK_DEFAULT_INPUT_RANG = -2147483647
const i64    PROC_CTRL_BLCK_DEFAULT_TIME_STAMP = -1
const string PROC_CTRL_BLCK_DEFAULT_ANCHOR_POS = "-2147483647"
const string PROC_CTRL_BLCK_DEFAULT_OUT_STREAM = "/dev/null"

const i64    TASK_DEFAULT_INPUT_RANG = -2147483647
const i32    TASK_DEFAULT_RANGE_STEP = -2147483647
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
 *    18: cur_anchor             : 25 the cur val written by proc stdout log file.
 *   }
 */
struct ProcessControlBlock {
  1: i64               range_from             = PROC_CTRL_BLCK_DEFAULT_INPUT_RANG,
  2: i64               range_to               = PROC_CTRL_BLCK_DEFAULT_INPUT_RANG,
  3: string            std_out                = PROC_CTRL_BLCK_DEFAULT_OUT_STREAM,
  4: string            std_err                = PROC_CTRL_BLCK_DEFAULT_OUT_STREAM,
  5: i64               created_tmstp_sec      = PROC_CTRL_BLCK_DEFAULT_TIME_STAMP,
  6: i64               last_updated_tmstp_sec = PROC_CTRL_BLCK_DEFAULT_TIME_STAMP,
  7: PROC_STATUS_CODE  cur_status             = PROC_STATUS_CODE.PROC_STATUS_NEW,
  8: i32               return_val             = PROC_CTRL_BLCK_DEFAULT_RETURN_VAL,
  9: i32               terminated_signal      = PROC_CTRL_BLCK_DEFAULT_TERMIN_SIG,
 10: PROC_PRORITY_CODE priority               = PROC_PRORITY_CODE.PROC_PRORITY_NORMAL,
 11: string            host_str               = "",
 12: i32               host_pid               = PROC_CTRL_BLCK_DEFAULT_PROCESS_ID,
 13: string            proc_hnd               = "",
 14: i64               mem_req_in_bytes       = PROC_CTRL_BLCK_DEFAULT_RESOUR_USG,
 15: i64               disk_req_in_bytes      = PROC_CTRL_BLCK_DEFAULT_RESOUR_USG,
 16: i64               mem_used_in_bytes      = PROC_CTRL_BLCK_DEFAULT_RESOUR_USG,
 17: i64               disk_used_in_bytes     = PROC_CTRL_BLCK_DEFAULT_RESOUR_USG,
 18: string            cur_anchor             = PROC_CTRL_BLCK_DEFAULT_ANCHOR_POS,
 19: i32               start_flag             = PROC_CTRL_BLCK_ANCHR_START, 
}

/**
 * Desc:
 * - Command line in yapp for submitting a Task taking id range as its input in
 *   yapp would be like follows:
 *   yapp --zk_conn_str='192.168.1.1:2818' | --zk_cfg_file='./test_cfg_util_load_cfg.input'
 *        --proc_num=3
 *        --range='0 59'
 *        --app_env='RAILS_ENV=production'
 *        --app_bin='/usr/bin/ruby'
 &        --working_dir='/var/www/test.foo.com/current'
 *        --app_src='./lib/data/location_data/update_ra.rb'
 *        --arg_str='-test -verbose'
 *        --stdout=${log_dir}/update_ra.stdout
 *        --stderr=${log_dir}/update_ra.stderr
 *        --autosplit=true
 *        --automigrate=true
 *        --anchor_prfx='YAPP_LOG_ANCHOR='
 *   In here could be translated to the RangeTask Structure as follows.
 * NOTE:
 * - autosplit {
 *     After turning on this feature, the YAPP System would dynamically re-split
 *     reschedule tasks when the number of concurrent jobs dropped below certain
 *     threshold(a fundamental different approach when compare to the strategy
 *     of running muti-redundant job copy used by any other MapReduce instance.
 *  }
 * - automigate {
 *     After turning on this feature, the YAPP System would dynamically do the
 *     load balancing by migrating the sub-task from one node to another node.
 *     when any of the worker nodes exceeds the system load limit and some other
 *     worker nodes have a system load below a certain threshold.
 *   }
 * - Since no inheritance for defining struct in Thrift IDL, use composition.
 * - No support for method overloading, needs to define separate method with a
 *   unique name for each kind of task.
 */

struct Task {
  1: string          app_env = "",
  2: string          app_bin = "",
  3: string          app_src = "",
  4: string          arg_str = "",
  5: string          out_pfx = "",
  6: string          err_pfx = "",
  7: i32             proc_cn = 0,
  8: TASK_INPUT_TYPE input_type  = TASK_INPUT_TYPE.UNSPECIFIED,
  9: string          input_file  = "",
 10: string          working_dir = "",
 11: i64             range_from  = TASK_DEFAULT_INPUT_RANG,
 12: i64             range_to    = TASK_DEFAULT_INPUT_RANG,
 13: i32             range_step  = TASK_DEFAULT_RANGE_STEP,
 14: TASK_ATTR_MASK  task_att    = TASK_ATTR_MASK.UNSPECIFIED,
 15: string          task_hnd    = "",
 16: string          anchor_prfx = "",
 17: list<ProcessControlBlock> proc_arr, 
 /** only used for client side hints, not logged in zookeeper **/
 18: string          task_owner  = "",
} 

struct Job {
  1: string     owner = "",
  2: i64        created_tmstp_sec = 0,
  3: string     job_hnd = "",
  4: list<Task> task_arr,
  /** only used for client side hints, not logged in zookeeper **/
  5: i32        yapp_service_code,
}

service YappMasterService {
  Job submit_job_rpc(1:Job job_to_sub),
  bool notify_proc_completion_rpc(1:string proc_handle, 2:i32 ret_val, 3:i32 term_sig),

  string print_job_tree_rpc(1:string job_handle),
  string print_task_tree_rpc(1:string task_handle),
  string print_proc_tree_rpc(1:string proc_handle),

  string print_queue_stat(1:string hndl_str);

  list<bool> pause_proc_arr_rpc(1:list<string> proc_hndl_arr),
  list<bool> resume_proc_arr_rpc(1:list<string> proc_hndl_arr),
  list<bool> terminate_proc_arr_rpc(1:list<string> proc_hndl_arr),
  list<bool> restart_failed_proc_arr_rpc(1:list<string> fail_proc_hndls),
  list<bool> fully_restart_proc_arr_rpc(1:list<string> term_proc_hndls, 2:list<string> fail_proc_hndls),
  list<bool> restart_from_last_anchor_proc_arr_rpc(1:list<string> term_proc_hndls, 2:list<string> fail_proc_hndls),

  bool purge_job_tree_rpc(1:string job_handle),
}

service YappWorkerService {
  bool execute_sub_task_async_rpc(1:Task sub_task),
  list<bool> signal_sub_task_async_rpc(1:list<i32> pid_arr, 2:list<string> proc_hnd_arr, 3:i32 signal),
}

/**
 * Desc:
 * - This is actually a mixture of the above 2 interface which would enable the
 *   automaitc failover for the entire yapp system.
 */
service YappService {
  /** 1. interfaces that applied to all yappd instances. **/
  map<string, map<string, string>> query_yappd_envs_rpc(),
  map<string, string> get_running_envs_rpc(),

  map<string, map<string, list<string>>> query_running_task_basic_info_rpc(1:list<string> host_arr, 2:list<string> owner_arr),
  map<string, list<string>> get_running_task_basic_info_rpc(1:list<string> owner_arr),

  list<string> get_all_active_subtask_hndl_arr_rpc(1:list<string> rndm_hndl_arr),
  list<string> get_all_paused_subtask_hndl_arr_rpc(1:list<string> rndm_hndl_arr),
  list<string> get_all_failed_subtask_hndl_arr_rpc(1:list<string> rndm_hndl_arr),
  list<string> get_all_terminated_subtask_hndl_arr_rpc(1:list<string> rndm_hndl_arr),

  /** 2. interfaces that applied to yappd master only. **/
  Job submit_job_rpc(1:Job job_to_sub),
  bool notify_proc_completion_rpc(1:string proc_handle, 2:i32 ret_val, 3:i32 term_sig),

  string print_job_tree_rpc(1:string job_handle),
  string print_task_tree_rpc(1:string task_handle),
  string print_proc_tree_rpc(1:string proc_handle),

  string print_queue_stat(1:string hndl_str);
 
  list<bool> pause_proc_arr_rpc(1:list<string> proc_hndl_arr),
  list<bool> resume_proc_arr_rpc(1:list<string> proc_hndl_arr),
  list<bool> terminate_proc_arr_rpc(1:list<string> proc_hndl_arr),
  list<bool> restart_failed_proc_arr_rpc(1:list<string> fail_proc_hndls),
  list<bool> fully_restart_proc_arr_rpc(1:list<string> term_proc_hndls, 2:list<string> fail_proc_hndls),
  list<bool> restart_from_last_anchor_proc_arr_rpc(1:list<string> term_proc_hndls, 2:list<string> fail_proc_hndls),

  bool purge_job_tree_rpc(1:string job_handle),


  /** 3. interfaces that applied to yappd worker only. **/
  bool execute_sub_task_async_rpc(1:Task sub_task),
  list<bool> signal_sub_task_async_rpc(1:list<i32> pid_arr, 2:list<string> proc_hnd_arr, 3:i32 signal),
}
