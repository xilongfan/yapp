#include <signal.h>
#include <algorithm>
#include <sys/types.h>
#include <unistd.h>

#include "test_yapp_client_using_file_input.h"

using namespace yapp::client::test_file_input;

CPPUNIT_TEST_SUITE_REGISTRATION(YappClientUsingFileInputTest);

/**
 *   yapp -h $host -p $port
 *        --autosplit --automigration
 *        --proc_num=3       # default process number to start
 *        --env='RAILS_ENV=production'
 *        --app='ruby ${PROG_PATH}/update_ra.rb'
 *        --range='0 59'
 *        --args='-test -verbose'
 *        --stdout=${log_dir}/update_ra.stdout
 *        --stderr=${log_dir}/update_ra.stderr
 */
void YappClientUsingFileInputTest::setUp(void){
  set_test_env();
  load_test_cases();
}

void YappClientUsingFileInputTest::load_test_cases(void) {
  /**
   * 1. load testing cases for test_parse_arugments
   */
  int test_case_cnt = sizeof(YAPP_CLIENT_PARAM_ARR_SET) / sizeof(char **);
  yapp_params_list_arr_ptr = new vector<string>[test_case_cnt];
  wrong_yapp_params_list_arr_ptr_0 = new vector<string>[test_case_cnt];
  wrong_yapp_params_list_arr_ptr_1 = new vector<string>[test_case_cnt];
  wrong_yapp_params_list_arr_ptr_2 = new vector<string>[test_case_cnt];

  for (int i = 0; i < test_case_cnt; i++) {
    for (int c = 0; c < YAPP_CLIENT_PARAM_ARR_SIZE_SET[i]; c++) {
      yapp_params_list_arr_ptr[i].push_back(YAPP_CLIENT_PARAM_ARR_SET[i][c]);
    }
    yapp_client_arr.push_back(
      YappClient(yapp_params_list_arr_ptr[i], true, true)
    );
  }
  for (int i = 0; i < test_case_cnt; i++) {
    for (int c = 1; c < YAPP_CLIENT_PARAM_ARR_SIZE_SET[i]; c++) {
      wrong_yapp_params_list_arr_ptr_0[i].push_back(
        YAPP_CLIENT_PARAM_ARR_SET[i][c]
      );
    }
    wrong_yapp_params_list_arr_ptr_0[i].push_back("LJSLKD JFLUSDG JUCKDFP21-");
    wrong_yapp_client_arr_0.push_back(
      YappClient(wrong_yapp_params_list_arr_ptr_0[i], true, true)
    );
  }
  for (int i = 0; i < test_case_cnt; i++) {
    for (int c = 0; c < YAPP_CLIENT_PARAM_ARR_SIZE_SET[i]; c++) {
      wrong_yapp_params_list_arr_ptr_1[i].push_back(
        YAPP_CLIENT_PARAM_ARR_SET[i][c]
      );
    }
    wrong_yapp_params_list_arr_ptr_1[i].push_back("LJSLKD JFLUSDG JUCKDFP21-");
    wrong_yapp_client_arr_1.push_back(
      YappClient(wrong_yapp_params_list_arr_ptr_1[i], true, true)
    );
  }
  for (int i = 0; i < test_case_cnt; i++) {
    for (int c = 1; c < YAPP_CLIENT_PARAM_ARR_SIZE_SET[i]; c++) {
      wrong_yapp_params_list_arr_ptr_2[i].push_back(
        YAPP_CLIENT_PARAM_ARR_SET[i][c]
      );
    }
    wrong_yapp_client_arr_2.push_back(
      YappClient(wrong_yapp_params_list_arr_ptr_2[i], true, true)
    );
  }

  /**
   * 2. prepare the yapp master server object for those upcoming tests.
   */
  for (int c = 0; c < NUM_OF_NODES; c++) {
    for (int i = 0; i < YAPP_MASTER_PARAM_CNT; i++) {
      yapp_worker_param_arr[c].push_back(string(YAPP_MASTER_PARAM_ARR[c][i]));
      yapp_worker_param_arr_bck[c].push_back(string(YAPP_MASTER_PARAM_ARR_BCK[c][i]));
    }
  }
  /*
  for (int i = 0; i < NUM_OF_NODES; i++) {
    yapp_worker_arr[i] = new YappWorker(yapp_worker_param_arr[i], true, true);
    CPPUNIT_ASSERT(NULL != yapp_worker_arr[i]);
    CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS, yapp_worker_arr[i]->parse_arguments());
  }
  */
}

void YappClientUsingFileInputTest::set_test_env(void) {
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str())
  );
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->delete_node_recur(TEST_ROOT)
  );
  vector<string> path_arr;
  vector<string> data_arr;
  for (size_t i = 0; i < sizeof(TEST_ENV) / sizeof(char *); i++) {
    path_arr.push_back(TEST_ENV[i]);
    data_arr.push_back("");
  }
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS, zk_ptr->batch_create(path_arr, data_arr)
  );
  delete zk_ptr;
}

void YappClientUsingFileInputTest::tearDown(void){
  delete[] yapp_params_list_arr_ptr;
  delete[] wrong_yapp_params_list_arr_ptr_0;
  delete[] wrong_yapp_params_list_arr_ptr_1;
  delete[] wrong_yapp_params_list_arr_ptr_2;
}

void YappClientUsingFileInputTest::test_parse_arugments(void){
  for (size_t i = 0; i < yapp_client_arr.size(); i++) {

#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "Arguments List ";
    for (int c = 0; c < YAPP_CLIENT_PARAM_ARR_SIZE_SET[i]; c++) {
      std::cerr << yapp_params_list_arr_ptr[i][c] << " ";
    }
    std::cerr << std::endl;
#endif

    CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS,yapp_client_arr[i].parse_arguments());
  }

  for (size_t i = 0; i < yapp_client_arr.size(); i++) {
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "Arguments List";
    for (size_t c = 0; c < wrong_yapp_params_list_arr_ptr_0[i].size(); c++) {
      std::cerr << wrong_yapp_params_list_arr_ptr_0[i][c] << " ";
    }
    std::cerr << std::endl;
#endif

    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_INVALID_ARGS, wrong_yapp_client_arr_0[i].parse_arguments()
    );
  }

  for (size_t i = 0; i < yapp_client_arr.size(); i++) {
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "Arguments List";
    for (size_t c = 0; c < wrong_yapp_params_list_arr_ptr_1[i].size(); c++) {
      std::cerr << wrong_yapp_params_list_arr_ptr_1[i][c] << " ";
    }
    std::cerr << std::endl;
#endif

    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_INVALID_ARGS, wrong_yapp_client_arr_1[i].parse_arguments()
    );
  }

  for (size_t i = 0; i < yapp_client_arr.size(); i++) {
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "Arguments List";
    for (size_t c = 0; c < wrong_yapp_params_list_arr_ptr_2[i].size(); c++) {
      std::cerr << wrong_yapp_params_list_arr_ptr_2[i][c] << " ";
    }
    std::cerr << std::endl;
#endif

    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_INVALID_ARGS, wrong_yapp_client_arr_2[i].parse_arguments()
    );
  }
}

void YappClientUsingFileInputTest::test_run(void) {

  /** 1. fire the master service as a seperate thread. **/
  start_yapp_service();

  /** 2. run each client & log the job_handle for sanity check. **/
  for (size_t i = 0; i < yapp_client_arr.size(); i++) {
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "==>> Running Yapp Client No." << i << std::endl;
#endif
    // CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS, yapp_client_arr[i].run());
    yapp_client_arr[i].run();
    int proc_cnt = yapp_client_arr[i].job_obj.task_arr.front().proc_arr.size();
    for (int c = 0; c < proc_cnt; c++) {
      YAPP_CLIENT_ID_TO_JOB_HND_MAPPING.push_back(
        yapp_client_arr[i].job_obj.task_arr.front().proc_arr[c].proc_hnd
      );
    }
  }

#ifdef DEBUG_YAPP_CLIENT
  std::cerr << "==>> Finish Logging all the job nodes:" << std::endl;
  for (size_t i = 0; i < YAPP_CLIENT_ID_TO_JOB_HND_MAPPING.size(); i++) {
    std::cerr << "No." << i << ": " << YAPP_CLIENT_ID_TO_JOB_HND_MAPPING[i]
              << std::endl;
  }
  std::cerr.flush();
#endif

  /** 4. stop all yapp service and restart to test the queue's durability **/
  sleep(20);

#ifdef DEBUG_YAPP_CLIENT
  std::cerr << "-- Start Trashing the Yapp Server: " << std::endl;
#endif
  stop_yapp_service();
  sleep(90);
#ifdef DEBUG_YAPP_CLIENT
  std::cerr << "-- Finish Trashing the Yapp Server: " << std::endl;
#endif

#ifdef DEBUG_YAPP_CLIENT
  std::cerr << "-- Restarts the Yapp Server: " << std::endl;
#endif
  start_yapp_service();

  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str())
  );

  /** 5. periodically querying the queue status & check if all jobs done. */
  bool b_test_fin = false;
  while (false == b_test_fin) {
    sleep(10);
    b_test_fin = check_progress(zk_ptr);
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << ">>>> CHECK TEST PROGRESS: " << b_test_fin
              << " AND PRINT QUEUE STAT." << std::endl;
#endif
    print_queue_stat(zk_ptr, string(""));
  }

  delete zk_ptr;

  /** 6. trash the master service. **/
  stop_yapp_service();

  /** 7. Do the Sanity Checking **/
  sanity_check();
}

bool YappClientUsingFileInputTest::check_progress(ZkClusterProxy * zkc_proxy_ptr) {
  bool status = true;
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  vector<string> rf_task_to_schedule;

  if (NULL == zkc_proxy_ptr) { return false; }

  rc = zkc_proxy_ptr->get_node_arr(
    zkc_proxy_ptr->get_rfile_task_queue_path_prefix(), rf_task_to_schedule
  );
  if (YAPP_MSG_SUCCESS != rc) { return false; }

  int tsk_cnt = rf_task_to_schedule.size();

#ifdef DEBUG_YAPP_CLIENT
  std::cerr << ">>>> ENTER YappClientUsingFileInputTest::check_progress" << std::endl;
#endif

  for (int i = 0; i < tsk_cnt; i++) {
    /** 1. try to acquire the ex lock for the task set to schedule **/
    bool b_empty_proc_arr = false;
    bool b_empty_runn_arr = false;


    YappSubtaskQueue rf_failed_task_queue;
    rf_failed_task_queue.set_queue_path(
      zkc_proxy_ptr->get_rfile_task_queue_path_prefix() + "/" +
      rf_task_to_schedule[i] + "/failed_procs"
    );
    vector<string> rf_failed_subtsk_arr;
    rc = rf_failed_task_queue.get_task_queue(rf_failed_subtsk_arr, zkc_proxy_ptr);

    vector<string> tp_chld_nodes_arr;
    vector<string> chld_nodes_arr;
    rc = zkc_proxy_ptr->get_node_arr(
      zkc_proxy_ptr->get_rfile_task_queue_path_prefix()
        + "/" + rf_task_to_schedule[i] + "/running_procs",
      tp_chld_nodes_arr
    );

    bool matched = false;
    int  task_size = tp_chld_nodes_arr.size();
    int  failed_cnt= rf_failed_subtsk_arr.size();  

    for (int c = 0; c < task_size; c++) {
      matched = false;
      for (int i = 0; i < failed_cnt; i++) {
        if (rf_failed_subtsk_arr[i] == tp_chld_nodes_arr[c]){
#ifdef DEBUG_YAPP_CLIENT
          std::cerr << "---- FOUND CHILD NODE IN FAIL_ARR: " << tp_chld_nodes_arr[c] << std::endl;
#endif
          matched = true;
          break;
        }
      }
      if (false == matched) {
        chld_nodes_arr.push_back(tp_chld_nodes_arr[c]);
      }
    }

#ifdef DEBUG_YAPP_CLIENT
    if (0 < chld_nodes_arr.size()) {
      for (size_t c = 0; c < chld_nodes_arr.size(); c++) {
        std::cerr << "++++ FOUND CHILD NODE IN RUNN_ARR: " << chld_nodes_arr[c] << std::endl;
      }
    } else {
      std::cerr << "++++ NO CHILD NODES FOUND IN RUNN_ARR!" << std::endl;
    }
#endif
    if (YAPP_MSG_SUCCESS != rc) { status = false; break; } 
    if (0 == chld_nodes_arr.size()) { b_empty_runn_arr = true; }

    chld_nodes_arr.clear();
    tp_chld_nodes_arr.clear();
    rc = zkc_proxy_ptr->get_node_arr(
      zkc_proxy_ptr->get_rfile_task_queue_path_prefix()
        + "/" + rf_task_to_schedule[i] + "/proc_arr",
      tp_chld_nodes_arr 
    );

    task_size = tp_chld_nodes_arr.size();
    for (int c = 0; c < task_size; c++) {
      matched = false;
      for (int i = 0; i < failed_cnt; i++) {
        if (0 == tp_chld_nodes_arr[c].find(rf_failed_subtsk_arr[i])){
#ifdef DEBUG_YAPP_CLIENT
          std::cerr << "---- FOUND CHILD NODE IN FAIL_ARR: " << tp_chld_nodes_arr[c] << std::endl;
#endif
          matched = true;
          break;
        }
      }
      if (false == matched) {
        chld_nodes_arr.push_back(tp_chld_nodes_arr[c]);
      }
    }

#ifdef DEBUG_YAPP_CLIENT
    if (0 < chld_nodes_arr.size()) {
      for (size_t c = 0; c < chld_nodes_arr.size(); c++) {
        std::cerr << "++++ FOUND CHILD NODE IN PROC_ARR: " << chld_nodes_arr[c] << std::endl;
      }
    } else {
      std::cerr << "++++ NO CHILD NODES FOUND IN PROC_ARR!" << std::endl;
    }
#endif
    if (YAPP_MSG_SUCCESS != rc) { status = false; break; } 
    if (0 == chld_nodes_arr.size()) { b_empty_proc_arr = true; }

    if (!b_empty_proc_arr || !b_empty_runn_arr) { status = false; break; }
  }

  return status;
}

void YappClientUsingFileInputTest::sanity_check(void)
{
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str())
  );
  int total_client = sizeof(YAPP_CLIENT_JOBCNT_ARR) / sizeof(int);
  int total_proc_cnt = 0;
  for (int i = 0; i < total_client; i++) {
    total_proc_cnt += YAPP_CLIENT_JOBCNT_ARR[i];
  }

  /** 1. calling sync to make sure all wirtes get applied before query **/
  zk_ptr->sync(zk_ptr->get_terminated_queue_path_prefix());

  vector<string> term_proc_hnd_arr;
  CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS,
    zk_ptr->get_node_arr(
      zk_ptr->get_terminated_queue_path_prefix(), term_proc_hnd_arr
    )
  );
  CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS,
    zk_ptr->get_node_arr(
      zk_ptr->get_failed_task_queue_path_prefix(), term_proc_hnd_arr
    )
  );
  
#ifdef DEBUG_YAPP_CLIENT
  std::cerr << ">>>> TERMINATED TASKS:" << std::endl;
  for (size_t x = 0; x < term_proc_hnd_arr.size(); x++) {
    std::cerr << term_proc_hnd_arr[x] << std::endl;
  }
#endif

  CPPUNIT_ASSERT_EQUAL((int)(term_proc_hnd_arr.size()), total_proc_cnt); 

  /**
   * 2. Check if there is any tasks lefted in the queue. The test is supposed
   *    to wait for a sufficient long time so that all jobs can be terminated.
   */
  vector<string> tmp_arr;

  CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS,
    zk_ptr->get_node_arr(zk_ptr->get_newtsk_queue_path_prefix(), tmp_arr)
  );
  CPPUNIT_ASSERT_EQUAL(0, (int)(tmp_arr.size()));
  CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS,
    zk_ptr->get_node_arr(zk_ptr->get_ready_queue_path_prefix(), tmp_arr)
  );
  CPPUNIT_ASSERT_EQUAL(0, (int)(tmp_arr.size()));

  CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS,
    zk_ptr->get_node_arr(zk_ptr->get_running_queue_path_prefix(), tmp_arr)
  );
#ifdef DEBUG_YAPP_CLIENT
  std::cerr << ">>>> RUNNING TASKS:" << std::endl;
  for (size_t x = 0; x < tmp_arr.size(); x++) {
    std::cerr << tmp_arr[x] << std::endl;
  }
#endif
  CPPUNIT_ASSERT_EQUAL(0, (int)(tmp_arr.size()));

  CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS,
     zk_ptr->get_node_arr(zk_ptr->get_paused_queue_path_prefix(), tmp_arr)
  );
  CPPUNIT_ASSERT_EQUAL(0, (int)(tmp_arr.size()));
 
  /** 2. check if each terminated processes got reflected in actual job node **/
  vector<Task> subtask_arr;
  for (size_t x = 0; x < term_proc_hnd_arr.size(); x++) {
    term_proc_hnd_arr[x] = term_proc_hnd_arr[x].substr(
      0, term_proc_hnd_arr[x].find(MAX_LINE_STR)
    );
  }
  std::sort(term_proc_hnd_arr.begin(), term_proc_hnd_arr.end());
#ifdef DEBUG_YAPP_CLIENT
  std::cerr << ">>>> TERMINATED TASKS:" << std::endl;
  for (size_t x = 0; x < term_proc_hnd_arr.size(); x++) {
    std::cerr << term_proc_hnd_arr[x] << std::endl;
  }
#endif
  YappDomainFactory::get_subtasks_by_proc_hnds(
    subtask_arr, term_proc_hnd_arr, zk_ptr, false
  );
  CPPUNIT_ASSERT_EQUAL(
    (int)(subtask_arr.size()), total_proc_cnt
  );
  int job_idx = 0;
  for (int i = 0; i < total_client; i++) {
    vector<int> cur_idx_arr;
    vector<int> nxt_idx_arr;
    vector<int> tot_cnt_arr;
    vector<int> max_idx_arr;
    for (int c = 0;  c < YAPP_CLIENT_JOBCNT_ARR[i]; c++) {
      /** 2.1 all sub-task should be marked as terminated. **/
      CPPUNIT_ASSERT_EQUAL(
        (int)subtask_arr[job_idx].proc_arr.front().cur_status, 4
      );
      /** 2.2 all sub-task should have the right return value. **/
      CPPUNIT_ASSERT_EQUAL(subtask_arr[job_idx].proc_arr.front().return_val,
                           YAPP_CLIENT_RETVAL_ARR[i]
      );
      /** 2.3 all sub-task should have the right return signal. **/
      CPPUNIT_ASSERT_EQUAL(
        (int)subtask_arr[job_idx].proc_arr.front().terminated_signal, 2147483647
      );
      /** 2.4 all sub-task should have the right process handle. **/
#ifdef DEBUG_YAPP_CLIENT
      std::cerr << "==>> Going to verify subtask No." << job_idx << ": "
                << YAPP_CLIENT_ID_TO_JOB_HND_MAPPING[job_idx]
                << std::endl;
#endif
      string tskhd_in_job, max_line_idx, tskhd_in_run,
             cur_line_idx, nxt_line_idx, tot_proc_cnt;

      RangeFileTaskDataParser::parse_rfile_task_data_str(
        subtask_arr[job_idx].proc_arr.front().proc_hnd,
        tskhd_in_job, max_line_idx, tskhd_in_run,
        cur_line_idx, nxt_line_idx, tot_proc_cnt
      );

      if (0 == c) { hndl_for_tasks_to_show.push_back(tskhd_in_job); }

      std::cerr << ">>>> " << YAPP_CLIENT_ID_TO_JOB_HND_MAPPING[job_idx] << std::endl;
      std::cerr << ">>>> " << subtask_arr[job_idx].proc_arr.front().proc_hnd << std::endl;
      /** 2.4.1, check if all subtasks been logged correctly **/
      CPPUNIT_ASSERT_EQUAL(
        tskhd_in_run, YAPP_CLIENT_ID_TO_JOB_HND_MAPPING[job_idx]
      );
      cur_idx_arr.push_back(atoll(cur_line_idx.c_str()));
      nxt_idx_arr.push_back(atoll(nxt_line_idx.c_str()));
      tot_cnt_arr.push_back(atoll(tot_proc_cnt.c_str()));
      max_idx_arr.push_back(atoll(max_line_idx.c_str()));
      /** 2.4.2, check if all subtasks been fully completed. **/
      CPPUNIT_ASSERT_EQUAL(
        cur_idx_arr.back() + tot_cnt_arr.back(), nxt_idx_arr.back()
      );
      if (2 < tot_cnt_arr.size()) {
        CPPUNIT_ASSERT(
          (cur_idx_arr.back() == cur_idx_arr[cur_idx_arr.size() - 2] + 1) ||
          (cur_idx_arr.back() == cur_idx_arr[cur_idx_arr.size() - 2] - tot_cnt_arr.back() + 1)
        );
      }
      if (2 < tot_cnt_arr.size()) {
        CPPUNIT_ASSERT_EQUAL(
          tot_cnt_arr.back(), tot_cnt_arr[tot_cnt_arr.size() - 2]
        );
      }
      if (2 < max_idx_arr.size()) {
        CPPUNIT_ASSERT_EQUAL(
          max_idx_arr.back(), max_idx_arr[max_idx_arr.size() - 2]
        );
      }
      job_idx++;
    }
  }
  print_queue_stat(zk_ptr);
  delete zk_ptr;
}

void YappClientUsingFileInputTest::print_queue_stat(ZkClusterProxy * zk_ptr) {
  int hndl_cnt = hndl_for_tasks_to_show.size();
  string job_hndl, tsk_hndl;
  print_queue_stat(zk_ptr, string(""));
  for (int i = 0; i < hndl_cnt; i++) {
    job_hndl = hndl_for_tasks_to_show[i].substr(
      0, hndl_for_tasks_to_show[i].find("_task-")
    );
    tsk_hndl = hndl_for_tasks_to_show[i].substr(
      0, hndl_for_tasks_to_show[i].find("_proc-")
    );
    print_queue_stat(zk_ptr, job_hndl);
    print_queue_stat(zk_ptr, tsk_hndl);
    print_queue_stat(zk_ptr, hndl_for_tasks_to_show[i]);
  }
}

void YappClientUsingFileInputTest::print_queue_stat(ZkClusterProxy * zk_ptr,
                                                    const string & hndl_str)
{
  string ret_str;
  zk_ptr->print_queue_stat(ret_str, hndl_str);
  std::cerr << ">>>> PRINT INFO FOR TASK WITH HNDLE: " << hndl_str << std::endl;
  std::cerr << ret_str;
}

void YappClientUsingFileInputTest::start_yapp_service(void) {
#ifdef DEBUG_YAPP_CLIENT
  std::cerr << std::endl;
#endif
  for (int i = 0; i < NUM_OF_NODES; i++) {
    yapp_worker_proc_id_arr[i] = fork();
    if (0 == yapp_worker_proc_id_arr[i]) {
      YappWorker * yapp_worker_ptr = new YappWorker(yapp_worker_param_arr_bck[i], true, true);
      CPPUNIT_ASSERT(NULL != yapp_worker_ptr);
      CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS, yapp_worker_ptr->parse_arguments());
      thread_yapp_service(yapp_worker_ptr);
      delete yapp_worker_ptr;
      tearDown();
      exit(0);
    } else {
      continue;
    }
/*
    CPPUNIT_ASSERT_EQUAL(
      0, pthread_create(
           &yapp_worker_proc_id_arr[i], NULL,
           thread_yapp_service, yapp_worker_arr[i]
         )
    );
*/
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "-- Finished Setting Up the Yapp Server: "
              << yapp_worker_proc_id_arr[i] << std::endl;
    std::cerr.flush();
#endif
  }
  sleep(3);
}

void * YappClientUsingFileInputTest::thread_yapp_service(void * yapp_worker_ptr) {
  YAPP_MSG_CODE rc = ((YappWorker *)yapp_worker_ptr)->run();
  if (YAPP_MSG_SUCCESS != rc) {
    std::cerr << YAPP_MSG_ENTRY[0 - rc] << std::endl;
  }
  return NULL;
}

void YappClientUsingFileInputTest::stop_yapp_service(void) {
//  void * thrd_info_ptr = NULL;
  for (int i = 0; i < NUM_OF_NODES; i++) {
    kill(yapp_worker_proc_id_arr[i], SIGTERM);
//    pthread_cancel(yapp_worker_proc_id_arr[i]);
  }
  bool b_verbose = true;
  for (int i = 0; i < NUM_OF_NODES; i++) {
    int status;
    int task_sig;
    pid_t child_pid = yapp_worker_proc_id_arr[i];
    int task_ret;
    do {
      int w = waitpid(child_pid, &status, WUNTRACED | WCONTINUED);
      if (w == -1) {
        std::cerr << "-- Error happend when waiting for child process: "
                  << child_pid
                  << " with parent process: "
                  << getpid() << std::endl;
        break;
      }
      if (WIFEXITED(status)) {
        task_ret = WEXITSTATUS(status);
        if (true == b_verbose) {
          std::cout << "-- child process: " << child_pid
                    << " terminated with returned value of "
                    << task_ret << std::endl;
        } 
      } else if (WIFSIGNALED(status)) {
        if (true == b_verbose) {
          task_sig = WTERMSIG(status);
          std::cout << "-- child process: " << child_pid
                    << " got killed by signal "
                    <<  task_sig << std::endl;
        }
      } else if (WIFSTOPPED(status)) {
        if (true == b_verbose) {
          std::cout << "-- child process: " << child_pid
                    << " stopped by signal "
                    << WSTOPSIG(status) << std::endl;
        }
      } else if (WIFCONTINUED(status)) {
        if (true == b_verbose) {
          std::cout << "-- child process: " << child_pid
                    << " resumed by SIGCONT" << std::endl;
        }
      }
    } while (!WIFEXITED(status) && !WIFSIGNALED(status));
//    pthread_join(yapp_worker_proc_id_arr[i], &thrd_info_ptr);
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "-- Finished Trashing the Yapp Server: "
              << yapp_worker_proc_id_arr[i] << std::endl;
#endif
  }
}
