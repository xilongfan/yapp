#include <sstream>
#include <unistd.h>
#include <signal.h>

#include "yapp_master_service_handler.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "gen-cpp/YappService.h"
#include "gen-cpp/yapp_service_types.h"
#include "gen-cpp/yapp_service_constants.h"

using namespace yapp::domain;

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace boost;

static pthread_mutex_t yapp_master_mutex = PTHREAD_MUTEX_INITIALIZER;

YappMasterServiceHandler::YappMasterServiceHandler(
  ZkClusterProxy * zkc_ptr, int max_queued_size, bool b_verb, bool b_test)
{
  max_queued_tsk_cnt = max_queued_size;
  zkc_proxy_ptr = zkc_ptr;
  b_verbose = b_verb;
  b_testing = b_test;
}

YappMasterServiceHandler::~YappMasterServiceHandler() {}

YAPP_MSG_CODE YappMasterServiceHandler::sync_newtsk_queue() {
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL != zkc_proxy_ptr) {
    newly_created_task_set.set_queue_path(
      zkc_proxy_ptr->get_newtsk_queue_path_prefix()
    );
    vector<string> tmp;
    rc = newly_created_task_set.sync_and_get_queue(tmp, zkc_proxy_ptr);
  }
  return rc;
}

void YappMasterServiceHandler::set_max_queued_task(int qsz) {
  if (qsz >= 0) {
    pthread_mutex_lock(&yapp_master_mutex);
    max_queued_tsk_cnt = qsz;
    pthread_mutex_unlock(&yapp_master_mutex);
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << ">>>> Master Obj for Child Proc. " << getpid()
              << " Reset Max Queued Task to : " << qsz << std::endl;
#endif
  }
}

int YappMasterServiceHandler::get_max_queued_task() {
  int qsz = 0;
  pthread_mutex_lock(&yapp_master_mutex);
  qsz = max_queued_tsk_cnt;
  pthread_mutex_unlock(&yapp_master_mutex);
  return qsz;
}

void YappMasterServiceHandler::submit_job_rpc(Job & job_obj,
                                              const Job & job_to_sub) {
  job_obj = job_to_sub;
  int task_cnt = job_obj.task_arr.size();
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "-- Enter YappMasterServiceHandler::submit_task" << std::endl;
  std::cerr << "-- Task#: " << task_cnt << std::endl;
  std::cerr << "-- owner: " << job_obj.owner << std::endl;
#endif
  job_obj.yapp_service_code = YAPP_MSG_CLIENT_ERROR_EXCEED_MAX_JOB_CNT;

  if (task_cnt <= 0) { return; }

  int proc_cnt = 0;
  for (int i = 0; i < task_cnt; i++) { proc_cnt += job_obj.task_arr[i].proc_cn;}
 
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  /**
   * Here we do not use a lock to protect the whole R-W process intentionally,
   * so as to avoid the case which only 1 user is allowed to submit jobs. A
   * small variance over the queue's limit won't be a problem since there won't
   * be too many users submit huge # of jobs simultaneously.
   */
  int tsk_cnt = newly_created_task_set.size() + proc_cnt;
  if (get_max_queued_task() > tsk_cnt) {
    /** 1. splitting & initiating a job object. **/
    for (int i = 0; i < task_cnt; i++) {
      this->split_task(job_obj.task_arr[i]);
    }
    /** 2. perform WAL before initiating any new tasks(task & queue logging) **/
    rc = this->initiate_new_job(job_obj);
    if (YAPP_MSG_SUCCESS == rc) {
    /** 3.1 Logging OK, best effort scheduling use def. policy(round-robin). **/
      this->schedule_job(job_obj);
    } else {
    /** 3.2 Logging FAILDED, nil out job_hnd to indicate failure for user. **/
#ifdef DEBUG_YAPP_JOB_SUBMISSION
      std::cerr << "-- failed in logging err: " << rc
                << YAPP_MSG_ENTRY[0 - rc] << std::endl;
#endif
      job_obj.job_hnd = "";
    }
    job_obj.yapp_service_code = (int)rc;

    if (true == b_verbose) {
      for (size_t c = 0; c < job_obj.task_arr.size(); c++) {
        std::cout << "==>> Task: " << c << " splitted!" << std::endl;
        int sub_task_cnt = job_obj.task_arr[c].proc_arr.size();
        for (int i = 0; i < sub_task_cnt; i++) {
          YappMasterServiceHandler::print_task(job_obj.task_arr[c], i);
        }
      }
    }
  } else {
    job_obj.yapp_service_code = YAPP_MSG_CLIENT_ERROR_EXCEED_MAX_JOB_CNT;
#ifdef DEBUG_YAPP_JOB_SUBMISSION
    std::cerr << ">>>> submit_job_rpc: " << job_obj.yapp_service_code
              << std::endl;
#endif
  }
}

void YappMasterServiceHandler::query_running_task_basic_info(
  map<string, map<string, vector<string> > > & tsk_grp_arr,
  const vector<string> & host_arr, const vector<string> & owner_arr) {

  vector<YappServerInfo> yapp_worker_arr;
  /** get the latest list of available worker nodes in Yapp services. **/
  if (YAPP_MSG_SUCCESS != get_worker_hosts(yapp_worker_arr)) { return; }

  int worker_cnt = yapp_worker_arr.size(), host_cnt = host_arr.size();
  if (0 >= worker_cnt) { return; }

  for (int i = 0; i < worker_cnt; i++) {
    bool b_host_to_query = false;
    for (int c = 0; c < host_cnt; c++) {
      if (yapp_worker_arr[i].host_str == host_arr[c]) {
        b_host_to_query = true; break;
      }
    }
    if ((false == b_host_to_query) && (host_cnt > 0)) { continue; }
    shared_ptr<TTransport> socket(
      new TSocket(yapp_worker_arr[i].host_str, yapp_worker_arr[i].port_num)
    );
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    YappServiceClient worker_client(protocol);
    try {
      transport->open();
      map<string, vector<string> > subtsk_grp_info;
      worker_client.get_running_task_basic_info_rpc(subtsk_grp_info, owner_arr);
      tsk_grp_arr[yapp_worker_arr[i].host_str] = subtsk_grp_info;
      transport->close();
    } catch (TException &tx) {
      std::cerr << tx.what() << std::endl;
    }
  }
}


void YappMasterServiceHandler::query_yappd_envs(
  map<string, map<string, string> > & host_to_kv_map, const string & m_host) {

  vector<YappServerInfo> yapp_worker_arr;
  /** get the latest list of available worker nodes in Yapp services. **/
  if (YAPP_MSG_SUCCESS != get_worker_hosts(yapp_worker_arr)) { return; }

  int worker_cnt = yapp_worker_arr.size();
  if (0 >= worker_cnt) { return; }

  for (int i = 0; i < worker_cnt; i++) {
    if (yapp_worker_arr[i].host_str == m_host) { continue; }
    shared_ptr<TTransport> socket(
      new TSocket(yapp_worker_arr[i].host_str, yapp_worker_arr[i].port_num)
    );
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    YappServiceClient worker_client(protocol);
    try {
      transport->open();
      map<string, string> kv_env_map;
      worker_client.get_running_envs_rpc(kv_env_map);
      host_to_kv_map[yapp_worker_arr[i].host_str] = kv_env_map;
      transport->close();
    } catch (TException &tx) {
      std::cerr << tx.what() << std::endl;
    }
  }
}

bool YappMasterServiceHandler::schedule_job(const vector<Task> & subtask_arr,
                                            YappWorkerList & yw_srv_list,
                                            int flag)
{
  bool status = false;
  if (subtask_arr.empty()) { return status; }

#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "Worker Count: " << yw_srv_list.size() << std::endl;
#endif

  int worker_cnt = yw_srv_list.size();
  int subtsk_cnt = subtask_arr.size();
  bool b_init_ok = false;

  if (0 >= worker_cnt || 0 >= subtsk_cnt) { return status; }

  switch (flag) {
  case YAPP_MASTER_SERV_HNDL_POLICY_ROUNDROBIN:
    for (int i = 0; i < subtsk_cnt; i++) {
      b_init_ok = false;
      YappServerInfo yapp_srv("");
      /** if fetching returns false, no more free workers at this moment. **/
      if (true != yw_srv_list.fetch_cur_yapp_worker(yapp_srv)) {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
        for (int c = i; c < subtsk_cnt; c++) {
          std::cerr << "++++> NO MORE WORKER AVAILABLE, TASK ==>> "
                    << get_sub_task_string(subtask_arr[c], 0)
                    << " QUEUED!: " << c << std::endl;
        }
#endif
        break;
      } else {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
        std::cerr << "++++> FETCH " << yapp_srv.host_str << " AS WORKER"
                  << " TO ASSIGN, TASK ==>> "
                  << get_sub_task_string(subtask_arr[i], 0) << std::endl;
#endif
      }
      shared_ptr<TTransport> socket(
        new TSocket(yapp_srv.host_str, yapp_srv.port_num)
      );
      shared_ptr<TTransport> transport(new TBufferedTransport(socket));
      shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
      YappServiceClient worker_client(protocol);

      /** 
       * - Before initiating any subtask, first try to do the latching to avoid
       *   double initiating, if no EX lock is available right now, that means a
       *   background scheduling process is holding it & try to do re-schedule,
       *   such that we can just skip to try the next task.
       *
       * - Note that the action of locking is more like latching, which only get
       *   the lock if it is available, and won't block on waiting it. Mostly it
       *   will succeeded imediately given that only master rescheduling threads
       *   might also compete for the lock once every tens of minutes.
       *
       * - To avoid the odd of scheduling the same task twice in case of system
       *   failure, the lock would be released by the worker thread once ACKed,
       *   otherwise, it simply release the lock by itself(no worker available).
       */
      string lock_hndl;
      string tskq_path = subtask_arr[i].proc_arr.front().proc_hnd;

      YAPP_MSG_CODE rc = newly_created_task_set.try_acquire_ex_lock(
        lock_hndl, tskq_path, zkc_proxy_ptr
      );

      if (YAPP_MSG_SUCCESS != rc) {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
        std::cerr << "++++> FAILED TO DO THE LATCH TO ASSIGN TASK "
                  << get_sub_task_string(subtask_arr[i], 0)
                  << " ON " << yapp_srv.host_str << " AS WORKER"
                  << std::endl;
#endif
        continue;
      } else {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
        std::cerr << "++++> FINISHED DOING THE LATCH TO ASSIGN TASK "
                  << get_sub_task_string(subtask_arr[i], 0)
                  << " ON " << yapp_srv.host_str << " AS WORKER"
                  << std::endl;
#endif
      }

      bool b_transport_err = false;
      try {
        transport->open();
        b_init_ok = worker_client.execute_sub_task_async_rpc(subtask_arr[i]);
        transport->close();
      } catch (TException &tx) {
        newly_created_task_set.release_ex_lock(lock_hndl, zkc_proxy_ptr);
        std::cerr << ">>>> RELEASE WRITE LOCK: " << lock_hndl << std::endl;
        std::cerr << tx.what() << std::endl;
        b_transport_err = true;
      }
      if (true == b_transport_err) { break; }
      if (true == b_init_ok) {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
        std::cerr << "Assign SubTask : "
                  << get_sub_task_string(subtask_arr[i], 0)
                  << " on Worker Host: " << yapp_srv.host_str
                  << " Port: " << yapp_srv.port_num << std::endl;
#endif
        /*
        newly_created_task_set.unlock_and_remv_from_task_queue(
          tskq_path, zkc_proxy_ptr
        );
        */
        newly_created_task_set.decrement_queue_cnt();
        yw_srv_list.shift_cur_yapp_worker();
        status = true;
      } else {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
        std::cerr << "Fail to Assign SubTask : "
                  << get_sub_task_string(subtask_arr[i], 0)
                  << " on Worker Host: " << yapp_srv.host_str
                  << " Port: " << yapp_srv.port_num
                  << " Giving Another Try." << std::endl;
#endif
        newly_created_task_set.release_ex_lock(lock_hndl, zkc_proxy_ptr);
        yw_srv_list.remove_and_shift_cur_yapp_worker();
        i--;
      } // if (true == b_init_ok) {
    } // for (int i = 0; i < subtsk_cnt; i++) {
    break;
  } // switch (flag) {
  return status;
}

bool YappMasterServiceHandler::schedule_job(const Job & job_obj, int flag) {
  bool status = true;

#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "++ Begin To Schedule Job" << std::endl;
#endif

  /** 1. extract every single process inside a job obj as a separate subtask **/
  vector<Task> subtask_arr;
  for (size_t i = 0; i < job_obj.task_arr.size(); i++) {
    /**
     * - Only jobs with direct id input will be scheduled directly, other kinds
     *   of jobs will be scheduled asynchronously by various threads.
     * - Note that this will only affect for jobs directly submitted, subtasks
     *   scheduled by daemon threads won't be affected.
     */
    if(TASK_INPUT_TYPE::ID_RANGE != job_obj.task_arr[i].input_type){ continue; }
    for (size_t c = 0; c < job_obj.task_arr[i].proc_arr.size(); c++) {
      Task subtask;
      status = YappDomainFactory::extract_subtask_from_job(subtask,job_obj,i,c);
      if (false == status) { break; }
      subtask_arr.push_back(subtask);
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "Subtask-Generated: "
                << get_sub_task_string(subtask, 0)
                << std::endl;
#endif
    }
    if (false == status) { break; }
  }
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "Subtask Count: " << subtask_arr.size() << std::endl;
#endif
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  vector<YappServerInfo> yapp_worker_arr;
  if (true == status) {
    /** get the latest list of available worker nodes in Yapp services. **/
    rc = get_worker_hosts(yapp_worker_arr);
    if (YAPP_MSG_SUCCESS == rc) {
      YappWorkerList yw_srv_list(yapp_worker_arr, flag);
      status = schedule_job(subtask_arr, yw_srv_list, flag);
    }
  }
  return status;
}

/**
 * /prod/yapp/jobs
 *  +
 *  `- job-001
 *      +
 *      |- owner : xfan@spokeo.com
 *      |- created_tmstp_sec : ${timestamp}
 *      |- job_hnd : "job-001"
 *      `- task_arr
 *          +
 *          `- task-001
 *              +
 *              |- app_env : "RAILS_ENV=production"
 *              |- app_bin : "/bin/ruby"
 *              |- app_src : "./lib/data/location_data/script/update_ra.rb"
 *              |- arg_str : "-test -verbose"
 *              |- out_pfx : "${log_dir}/update_ra.stdout"
 *              |- err_pfx : "${log_dir}/update_ra.stderr"
 *              |- proc_cn : 2
 *              |- working_dir: "/var/www/test.foo.com/current" 
 *              |- input_type : TEXT_FILE(by line) | BINARY_FILE(byte offset) | ID_RANGE
 *              |- input_file : ""(only not null when app uses file as input)
 *              |- range_from : 0
 *              |- range_to   : 39
 *              |- task_att : "AUTO_SPLIT | AUTO_MIGRATION"
 *              |- task_hnd : "job-001_task-001"
 *              `- proc_arr
 *                  +
 *                  |- proc-001
 *                  |   +
 *                  |   |- range_from : 0
 *                  |   |- range_to   : 20
 *                  |   |- std_out : "${log_dir}/update_ra.stdout.job-001_task-001_proc-001"
 *                  |   |- std_err : "${log_dir}/update_ra.stderr.job-001_task-001_proc-001"
 *                  |   |- created_tmstp_sec :
 *                  |   |- last_updated_tmstp_sec :
 *                  |   |- cur_status : PROC_STATUS_NEW
 *                  |   |- return_val :
 *                  |   |- terminated_signal :
 *                  |   |- priority : PROC_PRORITY_NORMAL
 *                  |   |- host_str :
 *                  |   |- host_pid :
 *                  |   |- proc_hnd : "job-001_task-001_proc-001"
 *                  |   |- mem_req_in_bytes,
 *                  |   |- disk_req_in_bytes,
 *                  |   |- mem_used_in_bytes,
 *                  |   `- disk_used_in_bytes,
 *                  `- proc-002
 *                      +
 *                      |- range_from : 20
 *                      |- range_to : 39
 *                      |- std_out : "${log_dir}/update_ra.stdout.job-001_task-001_proc-002"
 *                      |- std_err : "${log_dir}/update_ra.stderr.job-001_task-001_proc-002"
 *                      |- created_tmstp_sec :
 *                      |- last_updated_tmstp_sec :
 *                      |- cur_status : PROC_STATUS_NEW
 *                      |- return_val :
 *                      |- terminated_signal :
 *                      |- priority : PROC_PRORITY_NORMAL
 *                      |- host_str :
 *                      |- host_pid :
 *                      |- proc_han : "job-001_task-001_proc-002"
 *                      |- cur_anchor : 256
 *                      |- start_flag: 0, use anchor.
 *                      |- mem_req_in_bytes,
 *                      |- disk_req_in_bytes,
 *                      |- mem_used_in_bytes,
 *                      `- disk_used_in_bytes,
 */
YAPP_MSG_CODE YappMasterServiceHandler::initiate_new_job(Job & job_obj) {
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  /**
   * 1. initiate constants and setting up connections to zookeeper cluster.
   */
  string job_path_prefix = zkc_proxy_ptr->get_job_path_prefix();
  string job_node_return;
  string lock_hnd;
  job_path_prefix += string("/job-");

  if (NULL == zkc_proxy_ptr) { return YAPP_MSG_INVALID_ZKCONN; }

  /**
   * 2. start to create the process tree for job_obj.
   */
  int task_cnt = job_obj.task_arr.size();
  /** 2-1, obtain the job_node like '/test/foo/yapp/jobs/job-00001' **/
  rc = zkc_proxy_ptr->create_node_recur(job_path_prefix, "", job_node_return,
                                                             ZOO_SEQUENCE);
/**
 *  `- job-001
 *      +
 *      |- owner : xfan@spokeo.com
 *      |- created_tmstp_sec : ${timestamp}
 *      |- job_hnd : "job-001"
 *      `- task_arr
 */
  if (YAPP_MSG_SUCCESS != rc) { return YAPP_MSG_ERROR_CREATE_JOBNODE; }

  rc = zkc_proxy_ptr->try_acquire_write_lock(job_node_return, lock_hnd);
  if (YAPP_MSG_SUCCESS != rc) {
    zkc_proxy_ptr->delete_node_recur(job_node_return);
    return YAPP_MSG_INVALID_FAILED_GRAB_WLOCK;
  }

  vector<string> node_path_arr;
  vector<string> node_valu_arr;
  int proc_cnt = 0;

  node_path_arr.push_back(job_node_return + string("/owner"));
  node_valu_arr.push_back(job_obj.owner);

  node_path_arr.push_back(job_node_return + string("/created_tmstp_sec"));
  std::ostringstream num_to_str_in;
  num_to_str_in << job_obj.created_tmstp_sec;
  node_valu_arr.push_back(num_to_str_in.str());
  num_to_str_in.str("");
  num_to_str_in.clear();
 
/**
  node_path_arr.push_back(job_node_return + string("/job_hnd"));
  job_obj.job_hnd = StringUtil::get_path_basename(job_node_return);
  node_valu_arr.push_back(job_obj.job_hnd);
**/
  job_obj.job_hnd = StringUtil::get_path_basename(job_node_return);

  /** 2-2, create necessary attr. here for a single job. **/
  string task_path_prefix = job_node_return + string("/task_arr/task-");
  for (int i = 0; i < task_cnt; i++) {
    string task_node_return;
/**
 *          `- task-001
 *              +
 *              |- app_env : "RAILS_ENV=production"
 *              |- app_bin : "/bin/ruby"
 *              |- app_src : "./lib/data/location_data/script/update_ra.rb"
 *              |- arg_str : "-test -verbose"
 *              |- out_pfx : "${log_dir}/update_ra.stdout"
 *              |- err_pfx : "${log_dir}/update_ra.stderr"
 *              |- proc_cn : 2
 *              |- working_dir: "/var/www/test.foo.com/current" 
 *              |- input_type : TEXT_FILE(by line) | BINARY_FILE(byte offset) | ID_RANGE
 *              |- input_file : ""(only not null when app uses file as input)
 *              |- range_from : 0
 *              |- range_to   : 39
 *              |- task_att : "AUTO_SPLIT | AUTO_MIGRATION"
 *              |- task_hnd : "job-001_task-001"
 *              `- proc_arr
 */
    rc = zkc_proxy_ptr->create_node_recur(task_path_prefix, "",task_node_return,
                                                               ZOO_SEQUENCE);
    if (YAPP_MSG_SUCCESS != rc) { break; }

    job_obj.task_arr[i].task_hnd = job_obj.job_hnd + string("_") + 
      StringUtil::get_path_basename(task_node_return);

    node_path_arr.push_back(task_node_return + string("/app_env"));
    node_valu_arr.push_back(job_obj.task_arr[i].app_env);

    node_path_arr.push_back(task_node_return + string("/app_bin"));
    node_valu_arr.push_back(job_obj.task_arr[i].app_bin);

    node_path_arr.push_back(task_node_return + string("/app_src"));
    node_valu_arr.push_back(job_obj.task_arr[i].app_src);
 
    node_path_arr.push_back(task_node_return + string("/arg_str"));
    node_valu_arr.push_back(job_obj.task_arr[i].arg_str);

    node_path_arr.push_back(task_node_return + string("/out_pfx"));
    node_valu_arr.push_back(job_obj.task_arr[i].out_pfx);

    node_path_arr.push_back(task_node_return + string("/err_pfx"));
    node_valu_arr.push_back(job_obj.task_arr[i].err_pfx);
 
    num_to_str_in << job_obj.task_arr[i].proc_cn;
    node_path_arr.push_back(task_node_return + string("/proc_cn"));
    node_valu_arr.push_back(num_to_str_in.str());
    num_to_str_in.str("");
    num_to_str_in.clear();
    proc_cnt += job_obj.task_arr[i].proc_cn;
 
    num_to_str_in << job_obj.task_arr[i].input_type;
    node_path_arr.push_back(task_node_return + string("/input_type"));
    node_valu_arr.push_back(num_to_str_in.str());
    num_to_str_in.str("");
    num_to_str_in.clear();
 
    num_to_str_in << job_obj.task_arr[i].range_from;
    node_path_arr.push_back(task_node_return + string("/range_from"));
    node_valu_arr.push_back(num_to_str_in.str());
    string task_rng_from = num_to_str_in.str();
    num_to_str_in.str("");
    num_to_str_in.clear();
 
    num_to_str_in << job_obj.task_arr[i].range_to;
    node_path_arr.push_back(task_node_return + string("/range_to"));
    node_valu_arr.push_back(num_to_str_in.str());
    string task_rng_to = num_to_str_in.str();
    num_to_str_in.str("");
    num_to_str_in.clear();

    num_to_str_in << job_obj.task_arr[i].range_step;
    node_path_arr.push_back(task_node_return + string("/range_step"));
    node_valu_arr.push_back(num_to_str_in.str());
    num_to_str_in.str("");
    num_to_str_in.clear();

    num_to_str_in << job_obj.task_arr[i].task_att;
    node_path_arr.push_back(task_node_return + string("/task_att"));
    node_valu_arr.push_back(num_to_str_in.str());
    num_to_str_in.str("");
    num_to_str_in.clear();
 
    node_path_arr.push_back(task_node_return + string("/working_dir"));
    node_valu_arr.push_back(job_obj.task_arr[i].working_dir);

    node_path_arr.push_back(task_node_return + string("/anchor_prfx"));
    node_valu_arr.push_back(job_obj.task_arr[i].anchor_prfx);

    node_path_arr.push_back(task_node_return + string("/input_file"));
    if (TASK_INPUT_TYPE::DYNAMIC_RANGE == job_obj.task_arr[i].input_type) {
      node_valu_arr.push_back(job_obj.task_arr[i].input_file + "/" +
                              job_obj.task_arr[i].task_hnd + ".rng");
    } else {
      node_valu_arr.push_back(job_obj.task_arr[i].input_file);
    }

    if (YAPP_MSG_SUCCESS != rc) { return rc; }
    /** 2-3, create necessary attr. here for a single proc. **/
    int proc_cnt = job_obj.task_arr[i].proc_arr.size();
    string proc_path_prefix =
      task_node_return + string("/proc_arr/proc-");

    if ((TASK_INPUT_TYPE::DYNAMIC_RANGE == job_obj.task_arr[i].input_type) ||
        (TASK_INPUT_TYPE::RANGE_FILE == job_obj.task_arr[i].input_type)) {
      node_path_arr.push_back(
        zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
        job_obj.task_arr[i].task_hnd
      );
      node_valu_arr.push_back("");
      node_path_arr.push_back(
        zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
        job_obj.task_arr[i].task_hnd + "/running_procs"
      );
      node_valu_arr.push_back("");
      node_path_arr.push_back(
        zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
        job_obj.task_arr[i].task_hnd + "/failed_procs"
      );
      node_valu_arr.push_back("");
      node_path_arr.push_back(
        zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
        job_obj.task_arr[i].task_hnd + "/proc_arr"
      );
      node_valu_arr.push_back("");
    }
    int tot_procs = job_obj.task_arr[i].proc_arr.size();
    string tot_procs_str = StringUtil::convert_int_to_str(tot_procs);
    for (int c = 0; c < proc_cnt; c++) {
      string proc_node_return;
      rc = zkc_proxy_ptr->create_node_recur(
        proc_path_prefix, "", proc_node_return, ZOO_SEQUENCE
      );
/**
 *                  `- proc-002
 *                      +
 *                      |- range_from : 20
 *                      |- range_to : 39
 *                      |- std_out : "${log_dir}/update_ra.stdout.job-001_task-001_proc-002"
 *                      |- std_err : "${log_dir}/update_ra.stderr.job-001_task-001_proc-002"
 *                      |- created_tmstp_sec :
 *                      |- last_updated_tmstp_sec :
 *                      |- cur_status : PROC_STATUS_NEW
 *                      |- return_val :
 *                      |- terminated_signal :
 *                      |- priority : PROC_PRORITY_NORMAL
 *                      |- host_str :
 *                      |- host_pid :
 *                      |- proc_hnd : "job-001_task-001_proc-001"
 *                      |- cur_anchor : 256
 *                      |- start_flag: 0, use anchor.
 *                      |- mem_req_in_bytes,
 *                      |- disk_req_in_bytes,
 *                      |- mem_used_in_bytes,
 *                      `- disk_used_in_bytes,
 */
      job_obj.task_arr[i].proc_arr[c].proc_hnd = (
        job_obj.task_arr[i].task_hnd + string("_") +
        StringUtil::get_path_basename(proc_node_return)
      );

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].range_from;
      node_path_arr.push_back(proc_node_return + string("/range_from"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();
 
      num_to_str_in << job_obj.task_arr[i].proc_arr[c].range_to;
      node_path_arr.push_back(proc_node_return + string("/range_to"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].created_tmstp_sec;
      node_path_arr.push_back(proc_node_return + string("/created_tmstp_sec"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();
 
      num_to_str_in << job_obj.task_arr[i].proc_arr[c].last_updated_tmstp_sec;
      node_path_arr.push_back(proc_node_return + string("/last_updated_tmstp_sec"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].cur_status;
      node_path_arr.push_back(proc_node_return + string("/cur_status"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].return_val;
      node_path_arr.push_back(proc_node_return + string("/return_val"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].terminated_signal;
      node_path_arr.push_back(proc_node_return + string("/terminated_signal"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].priority;
      node_path_arr.push_back(proc_node_return + string("/priority"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].mem_req_in_bytes;
      node_path_arr.push_back(proc_node_return + string("/mem_req_in_bytes"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].disk_req_in_bytes;
      node_path_arr.push_back(proc_node_return + string("/disk_req_in_bytes"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].mem_used_in_bytes;
      node_path_arr.push_back(proc_node_return + string("/mem_used_in_bytes"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].disk_used_in_bytes;
      node_path_arr.push_back(proc_node_return + string("/disk_used_in_bytes"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      num_to_str_in << job_obj.task_arr[i].proc_arr[c].host_pid;
      node_path_arr.push_back(proc_node_return + string("/host_pid"));
      node_valu_arr.push_back(num_to_str_in.str());
      num_to_str_in.str("");
      num_to_str_in.clear();

      if (g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_OUT_STREAM !=
          job_obj.task_arr[i].proc_arr[c].std_out)
      {
        job_obj.task_arr[i].proc_arr[c].std_out =
          job_obj.task_arr[i].out_pfx + string(".") +
          job_obj.task_arr[i].proc_arr[c].proc_hnd;
      }

      node_path_arr.push_back(proc_node_return + string("/std_out"));
      node_valu_arr.push_back(job_obj.task_arr[i].proc_arr[c].std_out);

      if (g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_OUT_STREAM !=
          job_obj.task_arr[i].proc_arr[c].std_err)
      {
        job_obj.task_arr[i].proc_arr[c].std_err =
          job_obj.task_arr[i].err_pfx + string(".") +
          job_obj.task_arr[i].proc_arr[c].proc_hnd;
      }
      node_path_arr.push_back(proc_node_return + string("/std_err"));
      node_valu_arr.push_back(job_obj.task_arr[i].proc_arr[c].std_err);

      node_path_arr.push_back(proc_node_return + string("/host_str"));
      node_valu_arr.push_back("");

      node_path_arr.push_back(proc_node_return + string("/cur_anchor"));
      node_valu_arr.push_back(job_obj.task_arr[i].proc_arr[c].cur_anchor);
//      node_valu_arr.push_back(
//        StringUtil::convert_int_to_str(
//          job_obj.task_arr[i].proc_arr[c].cur_anchor
//        )
//      );

      node_path_arr.push_back(proc_node_return + string("/start_flag"));
      node_valu_arr.push_back(
        StringUtil::convert_int_to_str(
          job_obj.task_arr[i].proc_arr[c].start_flag
        )
      );

      node_path_arr.push_back(proc_node_return + string("/proc_hnd"));
      node_valu_arr.push_back(job_obj.task_arr[i].proc_arr[c].proc_hnd);

      switch(((int)(job_obj.task_arr[i].input_type))) {
        case ((int)TASK_INPUT_TYPE::ID_RANGE):   {
          /** following 2 methods are for pushing new tasks into NEW queue **/
          node_path_arr.push_back(
            zkc_proxy_ptr->get_newtsk_queue_path_prefix()
            + "/" + job_obj.task_arr[i].proc_arr[c].proc_hnd
          );
          node_valu_arr.push_back("");
          break;
        }
        case ((int)TASK_INPUT_TYPE::DYNAMIC_RANGE): {
          if (0 == c) {
            num_to_str_in << job_obj.task_arr[i].proc_arr[c].range_to;
            task_rng_to = num_to_str_in.str();
            num_to_str_in.str("");
            num_to_str_in.clear();
          }
        }
        case ((int)TASK_INPUT_TYPE::RANGE_FILE): {
          /**
           * - once a subtsk done its own portion, it will try to help other
           *   subtsks within the same task, and that's why we have the attr.
           *   cur_proc_hnd_to_process.
           *
           * - before a subtsk be scheduled(throw into the NEW queue) by daemon,
           *   the daemon will 1st try create a seq node using idx of cur. line
           *   to process as its name, the seq # would be the indicator of cur.
           *   processing loop.
           *
           * - the running_procs folder would be accessed by those worker inst.
           *   who actually executes the task, either being deleted or moved to
           *   the failed task queue for future restart.
           *
           * - ${proc_hnd_1}_max_111111_curhnd_${proc_hnd_1}_line_1 => {
           *     max: maximum index of the line in file(0 based)
           *     curhnd: current processing unit, which is the subtask exec.
           *     curln: current index of the line under processing.
           *     nxtln: next index of the line to process.
           *   }
           *
           * - the EX lock(or latching) is used to avoid the problem of a task
           *   being scheduled by multiple threads, which will be usefull when
           *   we needs to scale to multi-master in the future.
           *
           * /prod/yapp/queue/textrfin
           *  +
           *  `- ${task_hnd_0}
           *      +
           *      |- _lock-write-0000000001
           *      |- failed_procs
           *      |   +
           *      |   |- ${phnd_0}_max_111111_curhnd_${phnd_0}_curln_-2_nxtln_0_proccnt_2
           *      |   `- ${phnd_1}_max_111111_curhnd_${phnd_1}_curln_-1_nxtln_1_proccnt_2
           *      |- running_procs
           *      |   +
           *      |   |- ${phnd_0}_max_111111_curhnd_${phnd_0}_curln_-2_nxtln_0_proccnt_2
           *      |   `- ${phnd_1}_max_111111_curhnd_${phnd_1}_curln_-1_nxtln_1_proccnt_2
           *      `- proc_arr
           *          +
           *          |- ${phnd_0}_max_111111_curhnd_${phnd_0}_curln_-2_nxtln_0_proccnt_2
           *          `- ${phnd_1}_max_111111_curhnd_${phnd_1}_curln_-1_nxtln_1_proccnt_2
           * - Note that we set the cur. line # to be negative so that we could avoid
           *   skipping the 1st line while not changing the scheduling logic.
           */
          /** following method is to push new tasks into file_task_queue **/
          string start_pos = StringUtil::convert_int_to_str(
            job_obj.task_arr[i].proc_arr[c].range_from - tot_procs + c
          );
          string next_pos = StringUtil::convert_int_to_str(
            job_obj.task_arr[i].proc_arr[c].range_from + c
          );
          node_path_arr.push_back(
            zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
            job_obj.task_arr[i].task_hnd + "/proc_arr/" +
            job_obj.task_arr[i].proc_arr[c].proc_hnd + MAX_LINE_STR +
            task_rng_to + CUR_HNDL_STR +
            job_obj.task_arr[i].proc_arr[c].proc_hnd + CUR_LINE_STR +
            start_pos + NXT_LINE_STR + next_pos + TOT_PROC_STR + tot_procs_str
          );
          node_valu_arr.push_back("");
          break;
        }
      }
    }
    if (YAPP_MSG_SUCCESS != rc) { break; }

    node_path_arr.push_back(task_node_return + string("/task_hnd"));
    node_valu_arr.push_back(job_obj.task_arr[i].task_hnd);
  }

  if (YAPP_MSG_SUCCESS == rc) {
    rc = zkc_proxy_ptr->batch_create(node_path_arr, node_valu_arr);
    /**
     * - We only issue write op for job id after all writes op. for sub tasks
     *   and processes are sent and succeed. This way, we could gurantee that if
     *   create completes successfully, a job node is capable to be manipulated
     *   by any client if that client session could see its job_id is not null.
     *   Effectively a COMMIT message for this group creation without UNDO.
     * Note:
     * - Since for job initialization, there is only one server thread to handle
     *   everything, data is supposed to be consistent(satisfy all constraint
     *   requirements), the only cause of failure would be the network or nodes
     *   failure, this case, the request will get an error, Yapp will not block
     *   to restart the initialization.
     *
     * - Partial created tree will be either trashed instantly or by a daemon
     *
     * - A Daemon will only try to issue a delete when all cond. satisfied:
     *   1. the job node is not EX lock protected
     *   2. the job node does not have a job handle.
     *
     * - it might happen when the daemon grab the lock(triggered by sigalarm or
     *   wake up from a sleep) such that a job creation might got interrupted,
     *   but nothing bad happen, user could get the hint and submit again.
     */
    if (YAPP_MSG_SUCCESS == rc) {
      string tmp_buf;
      rc = zkc_proxy_ptr->create_node(
        (job_node_return + string("/job_hnd")), job_obj.job_hnd, tmp_buf
      );
    }
  }

  if (YAPP_MSG_SUCCESS != rc) {
    zkc_proxy_ptr->batch_delete(node_path_arr);
    zkc_proxy_ptr->delete_node_recur(job_node_return);
    rc = YAPP_MSG_ERROR_CREATE_JOBNODE;
  } else {
    newly_created_task_set.increment_queue_cnt(proc_cnt);
    zkc_proxy_ptr->release_lock(lock_hnd);
  }
  return rc;
}

bool YappMasterServiceHandler::notify_proc_completion_rpc(
  const string & proc_handle, int ret_val, int term_sig) {
  bool status = true;
  return status;
}

void YappMasterServiceHandler::split_rfile_task(Task & task)
{
  if (!((task.range_from >= 0) && (task.range_to >= task.range_from))){return;}
  if (!(0 == task.proc_arr.size())) { return; }
  if (!(0 < task.proc_cn)) { return; }
  if ((task.range_to - task.range_from + 1) < task.proc_cn) {
    task.proc_cn = task.range_to - task.range_from + 1;
  }
  for (int i = 0; i < task.proc_cn; i++) {
    ProcessControlBlock pcb = YappDomainFactory::create_pcb();
    pcb.range_from = task.range_from;
    pcb.range_to   = task.range_to;
    if (!task.out_pfx.empty()) { pcb.std_out = task.out_pfx; }
    if (!task.err_pfx.empty()) { pcb.std_err = task.err_pfx; }
    task.proc_arr.push_back(pcb);
  }
}

void YappMasterServiceHandler::split_range_task(Task & task)
{
  if (!((task.range_from >= 0) && (task.range_to >= task.range_from))){return;}
  if (!(0 == task.proc_arr.size())) { return; }
  if (!(0 < task.proc_cn)) { return; }
  long long total = task.range_to - task.range_from + 1;
  long long proc_cn = ((task.proc_cn > total) ? total : task.proc_cn);
  long long workload_per_process = total / proc_cn;
  long long work_left_cnt = total - (workload_per_process * proc_cn);
  long long prev = task.range_from;
  long long next = task.range_from + workload_per_process - 1;
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
   *  11: i64             range_to   = TASK_DEFAULT_INPUT_RANG,
   *  12: TASK_ATTR_MASK  task_att   = TASK_DEFAULT_ATTRIBUTES,
   *  13: string          task_hnd   = "",
   *  14: list<ProcessControlBlock> proc_arr, 
   * } 
   */
  while (prev <= task.range_to) {
    if (work_left_cnt > 0) { next++; work_left_cnt--; }
    ProcessControlBlock pcb = YappDomainFactory::create_pcb();
    pcb.range_from = prev;
    pcb.range_to   = (next > task.range_to) ? task.range_to : next;
    if (!task.out_pfx.empty()) { pcb.std_out = task.out_pfx; }
    if (!task.err_pfx.empty()) { pcb.std_err = task.err_pfx; }
    task.proc_arr.push_back(pcb);
    prev = next + 1;
    next = next + workload_per_process;
  }
}

void YappMasterServiceHandler::split_dyrng_task(Task & task)
{
  /**
   * for dynamic range tasks, the [ range_from, range_to, range_step ] recorded
   * @ TASK level represents the raw range input(0 ~ 1M, 1000), while for those
   * proc level [ range_from, range_to ] will hold how many lines in the range
   * file(0 ~ 999), so as to reduce this problem to range file input one.
   */
  if (!((task.range_step > 0) && (task.range_to >= task.range_from))){return;}
  if (!(0 == task.proc_arr.size())) { return; }
  if (!(0 < task.proc_cn)) { return; }

  long long chunk_count =
    (task.range_to - task.range_from + 1) / task.range_step;

  if ((chunk_count * task.range_step) <
      (task.range_to - task.range_from + 1)) { chunk_count += 1; }

  task.proc_cn = chunk_count < task.proc_cn ? chunk_count : task.proc_cn;

  for (int i = 0; i < task.proc_cn; i++) {
    ProcessControlBlock pcb = YappDomainFactory::create_pcb();
    pcb.range_from = 0;
    pcb.range_to   = chunk_count - 1;
    if (!task.out_pfx.empty()) { pcb.std_out = task.out_pfx; }
    if (!task.err_pfx.empty()) { pcb.std_err = task.err_pfx; }
    task.proc_arr.push_back(pcb);
  }
}

void YappMasterServiceHandler::split_task(Task & task)
{
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "-- Enter YappMasterServiceHandler::split_task"
            << " with type: " << task.input_type
            << " " << ((int)task.input_type) << std::endl;
  print_task(task);
#endif
  switch(((int)task.input_type)) {
  case ((int)TASK_INPUT_TYPE::ID_RANGE):      { split_range_task(task); break; }
  case ((int)TASK_INPUT_TYPE::RANGE_FILE):    { split_rfile_task(task); break; }
  case ((int)TASK_INPUT_TYPE::DYNAMIC_RANGE): { split_dyrng_task(task); break; }
  }
}

string YappMasterServiceHandler::get_task_string(const Task & task) {
  ostringstream range_task_str;
  range_task_str << "["
                 << task.range_from << ", "
                 << task.range_to << "], "
                 << task.proc_cn << ", "
                 << task.app_env << ", "
                 << task.app_bin << ", "
                 << task.app_src << ", "
                 << task.arg_str << ", "
                 << task.out_pfx << ", "
                 << task.err_pfx << ", ";
  if (TASK_ATTR_MASK::AUTO_SPLIT & task.task_att) {
    range_task_str << "1" << ", ";
  } else {
    range_task_str << "0" << ", ";
  }
  if (TASK_ATTR_MASK::AUTO_MIGRATE & task.task_att) {
    range_task_str << "1" << ", ";
  } else {
    range_task_str << "0" << ", ";
  }
  return range_task_str.str();
}

string YappMasterServiceHandler::get_sub_task_string(const Task & task, int i) {
  ostringstream range_task_str;
  if (i >= 0 && i < (int)task.proc_arr.size()) {
    range_task_str << "["
                   << task.proc_arr[i].range_from << ", "
                   << task.proc_arr[i].range_to << "], "
                   << 1 << ", "
                   << task.app_env << ", "
                   << task.app_bin << ", "
                   << task.app_src << ", "
                   << task.arg_str << ", "
                   << task.proc_arr[i].std_out << ", "
                   << task.proc_arr[i].std_err << ", ";
    if (TASK_ATTR_MASK::AUTO_SPLIT & task.task_att) {
      range_task_str << "1" << ", ";
    } else {
      range_task_str << "0" << ", ";
    }
    if (TASK_ATTR_MASK::AUTO_MIGRATE & task.task_att) {
      range_task_str << "1" << ", ";
    } else {
      range_task_str << "0" << ", ";
    }
  }
  return range_task_str.str();
}

void YappMasterServiceHandler::print_task(const Task & task, int i) {
  if (i >= 0 && i < (int)task.proc_arr.size()) {
    std::cout << "Sub-Task No." << i << ": ";
    std::cout << YappMasterServiceHandler::get_sub_task_string(task, i)
              << std::endl;
  } else {
    std::cout << YappMasterServiceHandler::get_task_string(task)
              << std::endl;
  }
}

bool YappMasterServiceHandler::get_procs_host_and_pid_info(
  const vector<string> & proc_hndl_arr, vector<string> & host_strs_arr,
                                        vector<int>    & proc_pidt_arr)
{
  bool status = false;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  string proc_hnd_in_job_node, proc_node_path, host_str_for_proc,
         host_ip_str, host_port_str, proc_pid_str, yapp_worker_pid;

  int proc_hndl_arr_size = proc_hndl_arr.size();
  for (int i = 0; i < proc_hndl_arr_size; i++) {
    status = RangeFileTaskDataParser::get_running_proc_hnd_in_job_node(
      proc_hndl_arr[i], proc_hnd_in_job_node
    );
    if (false == status) { break; }
    proc_node_path = zkc_proxy_ptr->get_proc_full_path(proc_hnd_in_job_node);
    rc = zkc_proxy_ptr->get_node_data(
      proc_node_path + "/host_str", host_str_for_proc
    );
    if (YAPP_MSG_SUCCESS != rc) { status = false; break; }
    status = JobDataStrParser::parse_proc_host_str(
      host_str_for_proc, host_ip_str, host_port_str, yapp_worker_pid
    );
    if (false == status) { break; }
    rc = zkc_proxy_ptr->get_node_data(
      proc_node_path + "/host_pid", proc_pid_str
    );
    if (YAPP_MSG_SUCCESS != rc) { status = false; break; }

    host_strs_arr.push_back(host_str_for_proc);
    proc_pidt_arr.push_back(atoi(proc_pid_str.c_str()));
  }
  return status; 
}

bool YappMasterServiceHandler::group_procsinfo_by_host(
  const vector<string> & host_strs_arr,
  const vector<string> & proc_hndl_arr,
  const vector<int>    & proc_pidt_arr,
  map<string, vector<string> > & host_str_to_proc_hndl_map,
  map<string, vector<int> >    & host_str_to_proc_pidt_map,
  map<string, vector<int> >    & host_str_to_proc_idxs_map)
{
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "$$$$ ENTER YappMasterServiceHandler::group_procsinfo_by_host:" << std::endl;
#endif
  bool status = true;
  map<string, vector<string> >::iterator itr;
  string key_str;

  int proc_hndl_arr_size = proc_hndl_arr.size();
  for (int i = 0; i < proc_hndl_arr_size; i++) {
    key_str = host_strs_arr[i];
    itr = host_str_to_proc_hndl_map.find(key_str);
    if (host_str_to_proc_hndl_map.end() == itr) {
      host_str_to_proc_hndl_map[key_str] = vector<string>();
      host_str_to_proc_pidt_map[key_str] = vector<int>();
      host_str_to_proc_idxs_map[key_str] = vector<int>();
    }
    host_str_to_proc_hndl_map[key_str].push_back(proc_hndl_arr[i]);
    host_str_to_proc_pidt_map[key_str].push_back(proc_pidt_arr[i]);
    host_str_to_proc_idxs_map[key_str].push_back(i);
  }

#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << ">>>> FINISH GROUP PROCS BY HOST:" << std::endl;
  for (itr  = host_str_to_proc_hndl_map.begin();
       itr != host_str_to_proc_hndl_map.end(); itr++) {
    std::cerr << "==== HOST: " << itr->first << std::endl;
    for (size_t i = 0; i < itr->second.size(); i++) {
      std::cerr << "++++ PROC: " << itr->second[i] << std::endl;
    }
  }
#endif

  return status;
}

void YappMasterServiceHandler::signal_worker_for_proc_arr(
  const vector<string> & proc_hndls, vector<bool> & ret_arr, int signal)
{
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "$$$$ ENTER YappMasterServiceHandler::signal_worker_for_proc_arr:" << std::endl;
#endif
  ret_arr.resize(proc_hndls.size(), false);
  bool status = true;

  /** 1. get all the proc_hndl_in_job for each proc_hndl & **/
  /**    get all the proc id based on all the proc_hndl_in_job **/
  vector<string> host_strs_arr;
  vector<int>    proc_pids_arr;

  status = get_procs_host_and_pid_info(
    proc_hndls, host_strs_arr, proc_pids_arr
  );

  if (false == status) { return; }

  /** 2. associate $(proc_hndl):$(pid) with host_str(host, port, pid) **/
  /**    essentailly multimap { :192.168.1.1:9527:99 => [ proc_1, proc_2 ] } **/
  map<string, vector<string> > host_str_to_proc_hndl_map;
  map<string, vector<int> >    host_str_to_pidt_arrs_map;
  map<string, vector<int> >    host_str_to_idxs_arrs_map;

  status = group_procsinfo_by_host(
    host_strs_arr, proc_hndls, proc_pids_arr, host_str_to_proc_hndl_map,
    host_str_to_pidt_arrs_map, host_str_to_idxs_arrs_map
  );

  if (false == status) { return; }

  string host_ip_str, host_port_str, yapp_worker_pid;
  vector<bool> ret;
  /** 2. for each set of $(proc_hndl_in_job):$(pid) in host a, pause via RPC **/
  map<string, vector<string> >::iterator itr;
  for (itr  = host_str_to_proc_hndl_map.begin();
       itr != host_str_to_proc_hndl_map.end(); itr++) {
    ret.clear();
    status = JobDataStrParser::parse_proc_host_str(
      itr->first, host_ip_str, host_port_str, yapp_worker_pid
    );
    if (false == status) { continue; }

    shared_ptr<TTransport> socket(
      new TSocket(host_ip_str, atol(host_port_str.c_str()))
    );
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    YappServiceClient worker(protocol);

    try {
      transport->open();
      worker.signal_sub_task_async_rpc(ret,
        host_str_to_pidt_arrs_map.find(itr->first)->second, itr->second, signal
      );
      for (size_t c = 0; c < ret.size(); c++) {
        ret_arr[host_str_to_idxs_arrs_map.find(itr->first)->second[c]] = ret[c];
      }
      transport->close();
    } catch (TException &tx) {
      status = false;
      std::cerr << tx.what() << std::endl;
    }
  }
}

void YappMasterServiceHandler::pause_proc_arr_rpc(vector<bool> & ret_arr,
                                            const vector<string>& proc_hndls)
{
  signal_worker_for_proc_arr(proc_hndls, ret_arr, SIGSTOP);
}

void YappMasterServiceHandler::resume_proc_arr_rpc(vector<bool> & ret_arr,
                                             const vector<string>& proc_hndls)
{
  signal_worker_for_proc_arr(proc_hndls, ret_arr, SIGCONT);
}

void YappMasterServiceHandler::terminate_proc_arr_rpc(vector<bool> & ret_arr,
                                                const vector<string>& proc_hnds)
{
  signal_worker_for_proc_arr(proc_hnds, ret_arr, SIGKILL);
}


void YappMasterServiceHandler::restart_failed_proc_arr_rpc(
  vector<bool> & ret_arr, const vector<string> & fail_proc_hndls)
{
  int tot_size = fail_proc_hndls.size();
  int itr_cnts = tot_size / MAX_BATCH_CREATE_CHUNK;
  int tsk_remn = tot_size - itr_cnts * MAX_BATCH_CREATE_CHUNK;
  if (tsk_remn > 0) { itr_cnts++; }
 
  int prev = 0;
  int next = 0;
  vector<string>::const_iterator proc_hndl_prev_itr = fail_proc_hndls.begin();
  vector<string>::const_iterator proc_hndl_next_itr = fail_proc_hndls.begin();

  for (int c = 0; c < itr_cnts; c++)
  {
    vector<bool> tret_arr;
    next += MAX_BATCH_CREATE_CHUNK;
    next = (next > tot_size) ? tot_size : next;

    proc_hndl_prev_itr = fail_proc_hndls.begin() + prev;
    proc_hndl_next_itr = fail_proc_hndls.begin() + next;

    vector<string> sub_proc_hndl_arr(proc_hndl_prev_itr , proc_hndl_next_itr);

    restart_failed_proc_arr_atomic(tret_arr, sub_proc_hndl_arr);

    ret_arr.insert(ret_arr.end(), tret_arr.begin(), tret_arr.end());
    prev = next;
  } /** end of for **/
}

void YappMasterServiceHandler::restart_failed_proc_arr_atomic(
  vector<bool> & ret_arr, const vector<string>& fproc_hndls)
{
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "@@@@ YappMasterServiceHandler::restart_failed_proc_arr_rpc" << std::endl;
#endif
  ret_arr.resize(fproc_hndls.size(), false);
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return; }

  vector<string> path_to_delete;
  vector<string> path_to_create;
  vector<string> node_to_set;
  vector<string> data_to_set;

  string ftsk_queue_path = zkc_proxy_ptr->get_rfile_task_queue_path_prefix();
  string fail_queue_path = zkc_proxy_ptr->get_failed_task_queue_path_prefix();
  string newt_queue_path = zkc_proxy_ptr->get_newtsk_queue_path_prefix();
 
  string cur_hndl, cur_proc_path;

  string tskhd_in_job, max_line_idx, tskhd_in_run,
         cur_line_idx, nxt_line_idx, tot_proc_cnt;

  bool status = false;
  int hndl_arr_cnt = fproc_hndls.size();

  for (int i = 0; i < hndl_arr_cnt; i++) {
    cur_hndl = fproc_hndls[i];
    path_to_delete.push_back(fail_queue_path + "/" + cur_hndl);
    path_to_create.push_back(newt_queue_path + "/" + cur_hndl);
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> delp: " << path_to_delete.back() << std::endl;
    std::cerr << "==>> newp: " << path_to_create.back() << std::endl;
#endif
    /**
     * when restarting a failed process, it will start from the point it failed
     * with corresponding anchor position logged in the job node(used when the
     * worker node doing the restart). Also note that since tasks from FAILED
     * queue comes from RUNNING queue only, so it won't contain NEW_DELM_STR for
     * marking if current process is being helped.
     */
    if (true == RangeFileTaskDataParser::is_range_file_task_hnd(cur_hndl))
    {
      /** ${phnd_0}_max_111111_curhnd_${phnd_0}_curln_0_nxtln_2_proccnt_2 **/
      status = RangeFileTaskDataParser::parse_rfile_task_data_str(
        cur_hndl, tskhd_in_job, max_line_idx, tskhd_in_run,
                  cur_line_idx, nxt_line_idx, tot_proc_cnt
      );
      if (true == status) {
        path_to_delete.push_back(ftsk_queue_path + "/" +
          zkc_proxy_ptr->get_task_hnd_by_proc_hnd(tskhd_in_job) +
          "/failed_procs/" + fproc_hndls[i]
        );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
        std::cerr << "==>> delp: " << path_to_delete.back() << std::endl;
#endif
        cur_hndl = tskhd_in_job;
      }
    } else {
      status = true;
    }
    cur_proc_path = zkc_proxy_ptr->get_proc_full_path(cur_hndl);
    generate_job_node_initial_value(
      node_to_set, data_to_set, cur_proc_path, cur_hndl
    );
    if (false == status) { break; }
  }

  if (false == status) { return; }

  rc = zkc_proxy_ptr->batch_unlock_delete_and_create_and_set(
    path_to_delete, path_to_create, node_to_set, data_to_set
  );

  if (YAPP_MSG_SUCCESS == rc) {
    ret_arr.clear();
    ret_arr.resize(fproc_hndls.size(), true);
  } else {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> fail to do the batch!" << std::endl;
#endif
  }
}

bool YappMasterServiceHandler::generate_job_node_initial_value(
  vector<string> & node_to_set, vector<string> & data_to_set,
  const string & cur_proc_path, const string & cur_hndl, bool b_reset_anchor)
{
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "@@@@ YappMasterServiceHandler::generate_job_node_initial_value" << std::endl;
#endif
  bool status = true;

  if (cur_proc_path.empty()) { return false; }
  node_to_set.push_back(cur_proc_path + "/cur_status");
  data_to_set.push_back(
    StringUtil::convert_int_to_str(PROC_STATUS_CODE::PROC_STATUS_NEW)
  );

  node_to_set.push_back(cur_proc_path + "/return_val");
  data_to_set.push_back(
    StringUtil::convert_int_to_str(
      g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_RETURN_VAL)
  );

  node_to_set.push_back(cur_proc_path + "/terminated_signal");
  data_to_set.push_back(
    StringUtil::convert_int_to_str(
      g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_TERMIN_SIG)
  );

  node_to_set.push_back(cur_proc_path + "/proc_hnd");
  data_to_set.push_back(cur_hndl);

/*
  node_to_set.push_back(cur_proc_path + "/last_updated_tmstp_sec");
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  data_to_set.push_back(StringUtil::convert_int_to_str(ts.tv_sec));
*/

  if (true == b_reset_anchor) {
    node_to_set.push_back(cur_proc_path + "/cur_anchor");
    data_to_set.push_back(
      g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_ANCHOR_POS
    );
//    data_to_set.push_back(
//      StringUtil::convert_int_to_str(
//        g_yapp_service_constants.PROC_CTRL_BLCK_DEFAULT_ANCHOR_POS)
//    );
    /** setting start flag to fresh, so that worker will not pull from log.*/
    node_to_set.push_back(cur_proc_path + string("/start_flag"));
    data_to_set.push_back(
      StringUtil::convert_int_to_str(
        g_yapp_service_constants.PROC_CTRL_BLCK_FRESH_START)
    );
  }

  return status;
}

bool YappMasterServiceHandler::generate_rftask_hndl_with_initial_value(
  string & job_node_hndl, string & new_hndl, string & cur_hndl,
                                             ZkClusterProxy * zkc_proxy_ptr)
{
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "@@@@ YappMasterServiceHandler::generate_rftask_hndl_with_initial_value"
            << std::endl;
#endif
  bool status = false;
  if (true == RangeFileTaskDataParser::is_range_file_task_hnd(cur_hndl))
  {
    string tskhd_in_job, max_line_idx, tskhd_in_run,
           cur_line_idx, nxt_line_idx, tot_proc_cnt;
    size_t hnd_pos = cur_hndl.find(NEW_DELM_STR);
    if (string::npos != hnd_pos) { cur_hndl = cur_hndl.substr(0, hnd_pos); }
    /** 1. if task is tasking file as input, then needs more processing **/
    /** ${phnd_0}_max_111111_curhnd_${phnd_0}_curln_0_nxtln_2_proccnt_2 **/
    status = RangeFileTaskDataParser::parse_rfile_task_data_str(
      cur_hndl, tskhd_in_job, max_line_idx, tskhd_in_run,
                cur_line_idx, nxt_line_idx, tot_proc_cnt
    );
    if (false == status) { return status; }
    Task subtask_obj;
    status = YappDomainFactory::get_subtask_by_proc_hnd(
      subtask_obj, tskhd_in_job, zkc_proxy_ptr
    );
    if (false == status) { return status; }
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> job node hndl: " << tskhd_in_job << std::endl;
#endif
    /** tskhd_in_job: "job-001_task-001_proc-001" **/
    int cur_proc_idx = atoll(
      tskhd_in_job.substr(tskhd_in_job.rfind("-") + 1).c_str()
    );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> cur_proc_ist: "
              << tskhd_in_job.substr(tskhd_in_job.rfind("-") + 1)
              << std::endl;
    std::cerr << "==>> cur_proc_idx: " << tskhd_in_job << std::endl;
#endif
    string start_pos = StringUtil::convert_int_to_str(
      subtask_obj.proc_arr.back().range_from - subtask_obj.proc_cn + cur_proc_idx
    );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> from: " << subtask_obj.range_from << std::endl;
    std::cerr << "==>> pcnt: " << subtask_obj.proc_cn << std::endl;
    std::cerr << "==>> stat: " << start_pos << std::endl;
#endif
    string next_pos = StringUtil::convert_int_to_str(
      subtask_obj.range_from + cur_proc_idx
    );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> next: " << next_pos << std::endl;
#endif
    string max_pos = StringUtil::convert_int_to_str(
      subtask_obj.proc_arr.front().range_to
    );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> maxp: " << max_pos << std::endl;
#endif
    new_hndl =
      zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
      subtask_obj.task_hnd + "/proc_arr/" +
      subtask_obj.proc_arr.front().proc_hnd +
      MAX_LINE_STR + max_pos + CUR_HNDL_STR +
      subtask_obj.proc_arr.front().proc_hnd +
      CUR_LINE_STR + start_pos + NXT_LINE_STR +
      next_pos + TOT_PROC_STR + tot_proc_cnt;
    job_node_hndl = tskhd_in_job;
    status = true;
  }
  return status;
}

void YappMasterServiceHandler::restart_from_last_anchor_proc_arr_rpc(
  vector<bool> & ret_arr, const vector<string> & term_proc_hndls,
                          const vector<string> & fail_proc_hndls)
{
  vector<bool> term_ret_arr, fail_ret_arr;
  fully_restart_proc_arr_in_batch(term_ret_arr, term_proc_hndls, true, false);
  fully_restart_proc_arr_in_batch(fail_ret_arr, fail_proc_hndls, false, false);
  ret_arr.insert(ret_arr.end(), term_ret_arr.begin(), term_ret_arr.end());
  ret_arr.insert(ret_arr.end(), fail_ret_arr.begin(), fail_ret_arr.end());
}

void YappMasterServiceHandler::fully_restart_proc_arr_rpc(
  vector<bool> & ret_arr, const vector<string> & term_proc_hndls,
                          const vector<string> & fail_proc_hndls)
{
  vector<bool> term_ret_arr, fail_ret_arr;
  fully_restart_proc_arr_in_batch(term_ret_arr, term_proc_hndls, true, true);
  fully_restart_proc_arr_in_batch(fail_ret_arr, fail_proc_hndls, false, true);
  ret_arr.insert(ret_arr.end(), term_ret_arr.begin(), term_ret_arr.end());
  ret_arr.insert(ret_arr.end(), fail_ret_arr.begin(), fail_ret_arr.end());
}

void YappMasterServiceHandler::fully_restart_proc_arr_in_batch(
  vector<bool> & ret_arr, const vector<string>& proc_hndls,
  bool b_restart_terminated, bool b_reset_anchor)
{
  int tot_size = proc_hndls.size();
  int itr_cnts = tot_size / MAX_BATCH_CREATE_CHUNK;
  int tsk_remn = tot_size - itr_cnts * MAX_BATCH_CREATE_CHUNK;
  if (tsk_remn > 0) { itr_cnts++; }
 
  int prev = 0;
  int next = 0;
  vector<string>::const_iterator proc_hndl_prev_itr = proc_hndls.begin();
  vector<string>::const_iterator proc_hndl_next_itr = proc_hndls.begin();

  for (int c = 0; c < itr_cnts; c++)
  {
    vector<bool> tret_arr;
    next += MAX_BATCH_CREATE_CHUNK;
    next = (next > tot_size) ? tot_size : next;

    proc_hndl_prev_itr = proc_hndls.begin() + prev;
    proc_hndl_next_itr = proc_hndls.begin() + next;

    vector<string> sub_proc_hndl_arr(proc_hndl_prev_itr , proc_hndl_next_itr);

    if (true == b_restart_terminated) {
      fully_restart_proc_arr_atomic(
        tret_arr, sub_proc_hndl_arr, vector<string>(), b_reset_anchor, true
      );
    } else {
      fully_restart_proc_arr_atomic(
        tret_arr, vector<string>(), sub_proc_hndl_arr, true, b_reset_anchor
      );
    }
    ret_arr.insert(ret_arr.end(), tret_arr.begin(), tret_arr.end());
    prev = next;
  } // end of for
}

void YappMasterServiceHandler::fully_restart_proc_arr_atomic(
  vector<bool> & ret_arr, const vector<string>& tproc_hndls,
                          const vector<string>& fproc_hndls,
                          bool b_reset_anchor_for_termin_proc,
                          bool b_reset_anchor_for_failed_proc)
{
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "@@@@ YappMasterServiceHandler::fully_restart_proc_arr_rpc" << std::endl;
#endif
  bool status = true;

  ret_arr.resize(tproc_hndls.size() + fproc_hndls.size(), false);
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  if (NULL == zkc_proxy_ptr) { return; }

  vector<string> path_to_delete;
  vector<string> path_to_create;
  vector<string> node_to_set;
  vector<string> data_to_set;

  string ftsk_queue_path = zkc_proxy_ptr->get_rfile_task_queue_path_prefix();
  string term_queue_path = zkc_proxy_ptr->get_terminated_queue_path_prefix();
  string fail_queue_path = zkc_proxy_ptr->get_failed_task_queue_path_prefix();
  string newt_queue_path = zkc_proxy_ptr->get_newtsk_queue_path_prefix();

  string cur_hndl, new_hndl, job_node_hndl, cur_proc_path, task_hndl, full_hndl;

  int hndl_arr_cnt = tproc_hndls.size();
  for (int i = 0; i < hndl_arr_cnt; i++) {
    cur_hndl = tproc_hndls[i];
    job_node_hndl = cur_hndl;
    path_to_delete.push_back(term_queue_path + "/" + cur_hndl);
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> path to del: " << path_to_delete.back() << std::endl;
#endif
    /** when restarting a already finished process, it starts from begining **/
    if (true == RangeFileTaskDataParser::is_range_file_task_hnd(cur_hndl)) {
      status = generate_rftask_hndl_with_initial_value(
        job_node_hndl, new_hndl, cur_hndl, zkc_proxy_ptr
      );
      if (true == status) { path_to_create.push_back(new_hndl); }
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> newp: " << path_to_create.back() << std::endl;
#endif
    } else {
      /** 2. if task takes id range as input, then simply move to new queue **/
      path_to_create.push_back(newt_queue_path + "/" + cur_hndl);
      status = true;
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> newp: " << path_to_create.back() << std::endl;
#endif
    }
    cur_proc_path = zkc_proxy_ptr->get_proc_full_path(job_node_hndl);
    status = generate_job_node_initial_value(
      node_to_set, data_to_set, cur_proc_path, job_node_hndl,
      b_reset_anchor_for_termin_proc
    );
    if (false == status) { break; }
  }

  hndl_arr_cnt = fproc_hndls.size();

  /** get all the full hndl str for every failed proc in each task **/
  map<string, vector<string> > task_hndl_to_full_hndls_map;
  if (hndl_arr_cnt > 0) {
    string tskhd_in_job, max_line_idx, tskhd_in_run,
           cur_line_idx, nxt_line_idx, tot_proc_cnt;
    for (int i = 0; i < hndl_arr_cnt; i++) {
      cur_hndl = fproc_hndls[i];
      if (true == RangeFileTaskDataParser::is_range_file_task_hnd(cur_hndl)) {
        status = RangeFileTaskDataParser::parse_rfile_task_data_str(
          cur_hndl, tskhd_in_job, max_line_idx, tskhd_in_run,
                    cur_line_idx, nxt_line_idx, tot_proc_cnt
        );
        if (false == status) { break; }
        task_hndl = zkc_proxy_ptr->get_task_hnd_by_proc_hnd(tskhd_in_job);
        task_hndl_to_full_hndls_map[task_hndl] = vector<string>();
      }
    }
    if (false == status) { return; }
    map<string, vector<string> >::iterator itr;
    for (itr  = task_hndl_to_full_hndls_map.begin();
         itr != task_hndl_to_full_hndls_map.end(); itr++) {
      rc = zkc_proxy_ptr->get_node_arr(
        ftsk_queue_path + "/" + itr->first + "/proc_arr", itr->second
      );
      if (YAPP_MSG_SUCCESS != rc) { status = false; break; }
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> find full failed hndl set:" << std::endl;
      for (size_t s = 0; s < itr->second.size(); s++) {
        std::cerr << "++++ " << itr->second[s] << std::endl;
      }
#endif
    }
  }
  
  if (false == status) { return; }

  for (int i = 0; i < hndl_arr_cnt; i++) {
    cur_hndl = fproc_hndls[i];
    job_node_hndl = cur_hndl;
    path_to_delete.push_back(fail_queue_path + "/" + cur_hndl);
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> path to del: " << path_to_delete.back() << std::endl;
#endif
    /** when restarting a already finished process, it starts from begining **/
    if (true == RangeFileTaskDataParser::is_range_file_task_hnd(cur_hndl)) {
      status = generate_rftask_hndl_with_initial_value(
        job_node_hndl, new_hndl, cur_hndl, zkc_proxy_ptr
      );

      task_hndl = zkc_proxy_ptr->get_task_hnd_by_proc_hnd(job_node_hndl);
      path_to_delete.push_back(
        ftsk_queue_path + "/" + task_hndl + "/failed_procs/" + fproc_hndls[i]
      );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> delp: " << path_to_delete.back() << std::endl;
#endif
      path_to_delete.push_back(
        ftsk_queue_path + "/" + task_hndl + "/running_procs/" + fproc_hndls[i]
      );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> delp: " << path_to_delete.back() << std::endl;
#endif
      vector<string> & full_hndl_arr = task_hndl_to_full_hndls_map[task_hndl];
      int hndl_cnt = full_hndl_arr.size();
      full_hndl = fproc_hndls[i];
      for (int x = 0; x < hndl_cnt; x++) {
        if (0 == full_hndl_arr[x].find(fproc_hndls[i])) {
          full_hndl = full_hndl_arr[x]; break;
        }
      }
      path_to_delete.push_back(
        ftsk_queue_path + "/" + task_hndl + "/proc_arr/" + full_hndl
      );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> delp: " << path_to_delete.back() << std::endl;
#endif

      if (true == status) { path_to_create.push_back(new_hndl); }
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> newp: " << path_to_create.back() << std::endl;
#endif
    } else {
      /** 2. if task takes id range as input, then simply move to new queue **/
      path_to_create.push_back(newt_queue_path + "/" + cur_hndl);
      status = true;
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> newp: " << path_to_create.back() << std::endl;
#endif
    }
    cur_proc_path = zkc_proxy_ptr->get_proc_full_path(job_node_hndl);
    status = generate_job_node_initial_value(
      node_to_set, data_to_set, cur_proc_path, job_node_hndl,
      b_reset_anchor_for_failed_proc
    );
    if (false == status) { break; }
  }
  if (false == status) { return; }

  rc = zkc_proxy_ptr->batch_unlock_delete_and_create_and_set(
    path_to_delete, path_to_create, node_to_set, data_to_set
  );
  if (YAPP_MSG_SUCCESS == rc) {
    ret_arr.clear();
    ret_arr.resize(tproc_hndls.size() + fproc_hndls.size(), true);
  } else {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> fail to do the batch!" << std::endl;
#endif
  }
}

bool YappMasterServiceHandler::purge_job_tree_rpc(const string & job_handle) {
  bool status = true;
  bool bexist = true;
  string job_node_path = zkc_proxy_ptr->get_job_full_path(job_handle);
  string lock_hnd;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;

  if (NULL == zkc_proxy_ptr) { return false; }
  rc = zkc_proxy_ptr->node_exists(job_node_path);
  if (YAPP_MSG_INVALID_NONODE == rc) { bexist = false; }
  else if (YAPP_MSG_INVALID == rc) { return false; }

  /** If the job nodes exists, then 1st lock, then delete **/
  if (true == bexist) {
    /** 2-1, obtain the job_node like '/test/foo/yapp/jobs/job-00001' **/
    rc = zkc_proxy_ptr->try_acquire_write_lock(job_node_path, lock_hnd);
    if (YAPP_MSG_SUCCESS == rc) {
      if (true == YappDomainFactory::is_job_all_terminated(job_handle, zkc_proxy_ptr)) {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
        std::cerr << "==>> going to purge job node:" << job_node_path << std::endl;
        string tree_str;
        zkc_proxy_ptr->print_node_recur(tree_str, job_node_path);
        std::cerr << tree_str;
#endif
        rc = zkc_proxy_ptr->purge_job_info(job_handle);
        if (YAPP_MSG_SUCCESS == rc) { status = true; }
      } else {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
        std::cerr << "==>> job not terminated:" << job_node_path << std::endl;
#endif
        status = false;
      }
      zkc_proxy_ptr->release_lock(lock_hnd);
    } else {
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cerr << "==>> Fail to Grab the Lock Before Purging." << job_node_path << std::endl;
#endif
      status = false;
    }
  } else {
  /** If the job nodes does not exists, then clean them from the queue **/
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cerr << "==>> Job Nodes Not Existed, Clean Up the Queue." << std::endl;
#endif
    rc = zkc_proxy_ptr->purge_job_info(job_handle);
    if (YAPP_MSG_SUCCESS == rc) { status = true; } else { status = false; }
  }
  return status;
}

void YappMasterServiceHandler::print_job_tree_rpc(string & tree_str,
                                            const string & job_handle) {
  if (NULL != zkc_proxy_ptr) {
    zkc_proxy_ptr->print_node_recur(
      tree_str, zkc_proxy_ptr->get_job_full_path(job_handle), true
    );
  }
}

void YappMasterServiceHandler::print_task_tree_rpc(string & tree_str,
                                             const string & task_handle) {
  if (NULL != zkc_proxy_ptr) {
    zkc_proxy_ptr->print_node_recur(
      tree_str, zkc_proxy_ptr->get_task_full_path(task_handle), true
    );
  }
}

void YappMasterServiceHandler::print_proc_tree_rpc(string & tree_str,
                                             const string & proc_handle) {
  if (NULL != zkc_proxy_ptr) {
    zkc_proxy_ptr->print_node_recur(
      tree_str, zkc_proxy_ptr->get_proc_full_path(proc_handle), true
    );
  }
}

void YappMasterServiceHandler::print_queue_stat(string & tree_str,
                                          const string & hndl_str) {
  string tp_hndl_str = hndl_str;
  StringUtil::trim_string(tp_hndl_str);
  if (NULL == zkc_proxy_ptr) { return; }
  zkc_proxy_ptr->print_queue_stat(tree_str, hndl_str);
}

void YappMasterServiceHandler::print_failed_queue_stat(string & tree_str,
                                                 const string & hndl_str) {
  string tp_hndl_str = hndl_str;
  StringUtil::trim_string(tp_hndl_str);
  if (NULL == zkc_proxy_ptr) { return; }
  zkc_proxy_ptr->print_queue_stat(tree_str, hndl_str, true);
}

YAPP_MSG_CODE YappMasterServiceHandler::get_worker_hosts(
  vector<YappServerInfo> & yapp_worker_arr)
{
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cerr << "++ Begin To Locate Worker Nodes" << std::endl;
#endif
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  vector<string> child_nodes_arr;
  if (NULL != zkc_proxy_ptr) {
    string election_path = zkc_proxy_ptr->get_election_path();
    rc = zkc_proxy_ptr->get_node_arr(election_path, child_nodes_arr);
    int yapp_nodes_cnt = child_nodes_arr.size();
    if (YAPP_MSG_SUCCESS == rc && 1 < yapp_nodes_cnt) {
      for (int i = 1; i < yapp_nodes_cnt; i++) {
        string worker_path = election_path + string("/") + child_nodes_arr[i];
        string worker_dat = "";
        string host_str, port_str, pidn_str;
        rc = zkc_proxy_ptr->get_node_data(worker_path, worker_dat);
        if (YAPP_MSG_SUCCESS != rc) { break; }
        rc = ZkClusterProxy::parse_election_node_data_str(
          worker_dat, host_str, port_str, pidn_str
        );
        if (YAPP_MSG_SUCCESS != rc) { break; }
        yapp_worker_arr.push_back(
          YappServerInfo(host_str, atoi(port_str.c_str()))
        );
      }
    }
  }
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  for (size_t i = 0; i < yapp_worker_arr.size(); i++) {
    std::cerr << "Worker No. " << i
              << " Host: " << yapp_worker_arr[i].host_str
              << " Port: " << yapp_worker_arr[i].port_num << std::endl;
  }
#endif
  return rc;
}
