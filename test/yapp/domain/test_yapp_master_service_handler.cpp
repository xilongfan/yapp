#include <config.h>
#include <unistd.h>
#include <pthread.h>

#include "yapp_util.h"
#include "../conf/test_yapp_conf.h"
#include "test_yapp_master_service_handler.h"

using namespace yapp::domain;
using namespace yapp::conf;
using namespace yapp::util;

CPPUNIT_TEST_SUITE_REGISTRATION(YappMasterServiceHandlerTest);

void YappMasterServiceHandlerTest::setUp(void) {
  out_pfx = "/var/tmp/update_ra.stdout";
  err_pfx = "/var/tmp/update_ra.stderr";
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
  zkc_proxy_ptr = new ZkClusterProxy(true);
  zkc_proxy_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  master_srv_hnd = new YappMasterServiceHandler(
    zkc_proxy_ptr, true, true
  );
}

void YappMasterServiceHandlerTest::tearDown(void) {
  delete master_srv_hnd;
  delete zkc_proxy_ptr;
}

void YappMasterServiceHandlerTest::test_initiate_new_job(void) {
  Task range_task_arr[] = {
    YappDomainFactory::create_task(
      0, 99, 4, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 99, 2, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 0, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 2, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 2, 3, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 2, 3, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
  };
  int task_cnt = 6;
  vector<Task> task_arr;
  for (int i = 0; i < task_cnt; i++) {
    task_arr.push_back(range_task_arr[i]);
  }
  Job job_obj = YappDomainFactory::create_job(
    "xfan@spokeo.com", time(0), task_arr
  );
  Job job_ret;
  master_srv_hnd->submit_job_rpc(job_ret, job_obj);
  master_srv_hnd->purge_job_tree_rpc(job_ret.job_hnd);
}

/**
 *   yapp -h $host -p $port
 *        --autosplit --automigration
 *        --proc_num=10       # default process number to start
 *        --env='RAILS_ENV=production'
 *        --app='ruby", "${PROG_PATH}/update_ra.rb'
 *        --range='0 100'
 *        --args='-test -verbose'
 *        --stdout=${log_dir}/update_ra.stdout
 *        --stderr=${log_dir}/update_ra.stderr
 */ 
void YappMasterServiceHandlerTest::test_split_task(void) {
  Task range_task_arr[] = {
    YappDomainFactory::create_task(
      0, 99, 4, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 99, 2, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 0, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 2, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 2, 3, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
    YappDomainFactory::create_task(
      0, 2, 3, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
      "-test -verbose", out_pfx, err_pfx, true, true, "", ""
    ),
  };
  int child_task_arr_len_set[] = {
    4, 2, 1, 1, 3, 3
  };
  Task child_task_arr_set[][4] = {
    {
      YappDomainFactory::create_task(
        0, 24, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
      YappDomainFactory::create_task(
        25, 49, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
      YappDomainFactory::create_task(
        50, 74, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
      YappDomainFactory::create_task(
        75, 99, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
    },
    {
      YappDomainFactory::create_task(
        0, 49, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
      YappDomainFactory::create_task(
        50, 99, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
    },
    {
      YappDomainFactory::create_task(
        0, 0, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
    },
    {
      YappDomainFactory::create_task(
        0, 2, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
    },
    {
      YappDomainFactory::create_task(
        0, 0, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
      YappDomainFactory::create_task(
        1, 1, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
      YappDomainFactory::create_task(
        2, 2, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
    },
    {
      YappDomainFactory::create_task(
        0, 0, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
      YappDomainFactory::create_task(
        1, 1, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
      YappDomainFactory::create_task(
        2, 2, 1, "RAILS_ENV=production", "ruby", "${PROG_PATH}/update_ra.rb",
        "-test -verbose", out_pfx, err_pfx, true, true, "", ""
      ),
    },
  };
  vector<Task> result_arr[sizeof(range_task_arr) / sizeof(Task)];
  for (size_t i = 0; i < sizeof(range_task_arr) / sizeof(Task); i++) {
    result_arr[i] = vector<Task>(
      child_task_arr_set[i], child_task_arr_set[i] + child_task_arr_len_set[i]
    );
  }
  for (size_t i = 0; i < sizeof(range_task_arr) / sizeof(Task); i++) {
    master_srv_hnd->split_task(range_task_arr[i]);
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cout << "==>> Task No. " << i << " input_type: "
              << range_task_arr[i].input_type << std::endl;
#endif
    CPPUNIT_ASSERT_EQUAL(
      range_task_arr[i].proc_arr.size(), result_arr[i].size()
    );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
    std::cout << "==>> Compare testing case No. " << i << std::endl;
#endif
    for (size_t c = 0; c < result_arr[i].size(); c++) {
      CPPUNIT_ASSERT_EQUAL(
        YappMasterServiceHandler::get_task_string(result_arr[i][c]),
        YappMasterServiceHandler::get_sub_task_string(range_task_arr[i], c)
      );
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
      std::cout << ">>>> ";
      YappMasterServiceHandler::print_task(result_arr[i][c]);
      std::cout << "<<<< ";
      YappMasterServiceHandler::print_task(range_task_arr[i], c);
#endif
    }
  }
#ifdef DEBUG_YAPP_MASTER_SERVICE_HANDLER
  std::cout << std::endl;
  std::cout << "Input Testing Case Count: "
            << sizeof(range_task_arr) / sizeof(Task)
            << std::endl;
  std::cout << "Output Testing Results #: "
            << sizeof(child_task_arr_set) / (sizeof(Task) * 4)
            << std::endl;
  for (size_t i = 0; i < sizeof(range_task_arr) / sizeof(Task); i++) {
    std::cout << "Size of Output Testing Vector No." << i << ": "
              << result_arr[i].size()
              << std::endl;
  }
#endif
  
}
