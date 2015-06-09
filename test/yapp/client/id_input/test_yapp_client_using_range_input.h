#ifndef TEST_YAPP_CLIENT_USING_RANGE_INPUT_H_
#define TEST_YAPP_CLIENT_USING_RANGE_INPUT_H_

#include <string>
#include <cstring>
#include <unistd.h>
#include <pthread.h>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include "yapp_client.h"
#include "../domain/yapp_domain_factory.h"
#include "../base/yapp_base.h"
#include "../worker/yapp_worker.h"
#include "../../conf/test_yapp_conf.h"
#include "../domain/gen-cpp/yapp_service_constants.h"

namespace yapp {
namespace client {
namespace test_range_input {

using namespace yapp::base;
using namespace yapp::conf;
using namespace yapp::worker;
using namespace yapp::domain;

using std::ifstream;
using std::string;
using std::ios;
using std::vector;

/**
Usage When Run Yapp in Client Mode:
yapp [--mode=client] [--verbose]

     --zk_conn_str=192.168.1.101:2181 | --zk_cfg_file='/etc/yapp.cfg'
     [--autosplit=(true|false)] [--automigrate=(true|false)] [--proc_num=${num}<16 def.>]
     [--app_env='RAILS_ENV=production']
     --app_bin='/usr/bin/ruby /update_ra.rb' --range='0 59' [--arg_str='-test -verbose']
     --stdout='/tmp/update_ra.stdout' --stderr='/tmp/update_ra.stdout'

Note: [] --> optional
**/

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_CONN_STR[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=3",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra.rb",
  "--range=0 1",
  "--arg_str=-test -verbose",
  "--stdout=./update_ra.stdout",
  "--stderr=./update_ra.stderr",
}; 
const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_AUTO[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
//  "--autosplit=true",
//  "--automigration=true",
  "--proc_num=3",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra.rb",
  "--range=0 59",
  "--arg_str=-test -verbose",
  "--stdout=./update_ra.stdout",
  "--stderr=./update_ra.stderr",
}; 
const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
//  "--proc_num=3",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_exception.rb",
  "--range=0 59",
  "--arg_str=-test -verbose",
  "--stdout=./update_ra_exception.stdout",
  "--stderr=./update_ra_exception.stderr",
}; 
const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
//  "--autosplit=true",
//  "--automigration=true",
//  "--proc_num=3",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_exception_div_zero.rb",
  "--range=0 59",
  "--arg_str=-test -verbose",
  "--stdout=./update_ra_exception_div_zero.stdout",
  "--stderr=./update_ra_exception_div_zero.stderr",
}; 
const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO_ENV[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
//  "--autosplit=true",
//  "--automigration=true",
//  "--proc_num=3",
//  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_exception_nil.rb",
  "--range=0 59",
  "--arg_str=-test -verbose",
  "--stdout=./update_ra_exception_nil.stdout",
  "--stderr=./update_ra_exception_nil.stderr",
}; 
const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO_ENV_ARG[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
//  "--autosplit=true",
//  "--automigration=true",
//  "--proc_num=3",
//  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_exception_require.rb",
  "--range=0 59",
//  "--arg_str=-test -verbose",
  "--stdout=./update_ra_exception_require.stdout",
  "--stderr=./update_ra_exception_require.stderr",
}; 

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_CFG_FILE[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=3",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_exception_syntax.rb",
  "--range=0 59",
  "--arg_str=-test -verbose",
  "--stdout=./update_ra_exception_syntax.stdout",
  "--stderr=./update_ra_exception_syntax.stderr",
}; 

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITHOUT_IO_OPT[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=3",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra.rb",
  "--range=0 59",
  "--arg_str=-test -verbose",
}; 

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_DEF_IO_OPT[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=3",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra.rb",
  "--range=0 59",
  "--arg_str=-test -verbose",
  "--stdout=/dev/null",
  "--stderr=/dev/null",
}; 

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_PARTIAL_IO[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=3",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra.rb",
  "--range=0 59",
  "--arg_str=-test -verbose",
  "--stdout=./update_ra.stdout",
  "stderr=/dev/null",
}; 

const static char * const YAPP_MASTER_PARAM_ARR[][3] = {
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=5",
    "--port=9527",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=5",
    "--port=9528",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=5",
    "--port=9529",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
   "--thrd_pool_size=10",
    "--port=9530",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=5",
    "--port=9531",
  },
};

const static char * const YAPP_MASTER_PARAM_ARR_BCK[][3] = {
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=5",
    "--port=9532",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=5",
    "--port=9533",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=5",
    "--port=9534",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
   "--thrd_pool_size=10",
    "--port=9535",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=5",
    "--port=9536",
  },
};


const static int YAPP_MASTER_PARAM_CNT = 3;

const static char * const * const YAPP_CLIENT_PARAM_ARR_SET[] = {
  &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_CONN_STR[0],

  &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_AUTO[0],
  &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM[0],
  &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO[0],
  &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO_ENV[0],
  &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO_ENV_ARG[0],
 
  &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_CFG_FILE[0],

  &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITHOUT_IO_OPT[0],
  &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_DEF_IO_OPT[0],
  &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_PARTIAL_IO[0],
}; 


const static int YAPP_CLIENT_PARAM_ARR_SIZE_SET[] = {
  11, 9, 10, 8, 7, 6, 11, 9, 11, 11
};
/*
  "--stdout=./update_ra.stdout", "--stderr=./update_ra.stderr",
  "--stdout=./update_ra.stdout", "--stderr=./update_ra.stderr",
  "--stdout=./update_ra_exception.stdout", "--stderr=./update_ra_exception.stderr",
  "--stdout=./update_ra_exception_div_zero.stdout", "--stderr=./update_ra_exception_div_zero.stderr",
  "--stdout=./update_ra_exception_nil.stdout", "--stderr=./update_ra_exception_nil.stderr",
  "--stdout=./update_ra_exception_require.stdout", "--stderr=./update_ra_exception_require.stderr",
  "--stdout=./update_ra_exception_syntax.stdout", "--stderr=./update_ra_exception_syntax.stderr",
  "--app_src=./update_ra.rb",
  "--app_src=./update_ra.rb",
  "--app_src=./update_ra.rb",
*/

const static int YAPP_CLIENT_RETVAL_ARR[] = {
  0,  0, 1,  1, 1, 1, 1,  0, 0,  0
};

const static int YAPP_CLIENT_JOBCNT_ARR[] = {
  2,  3, 16,  16, 16, 16, 3,  3, 3,  3
};

vector<string> YAPP_CLIENT_ID_TO_JOB_HND_MAPPING;


const static int NUM_OF_NODES = 5;

class YappClientUsingRangeInputTest : public CPPUNIT_NS::TestFixture {
  CPPUNIT_TEST_SUITE(YappClientUsingRangeInputTest);
  CPPUNIT_TEST(test_parse_arugments);
  CPPUNIT_TEST(test_run);
  CPPUNIT_TEST_SUITE_END();
  public:
    void setUp(void);
    void tearDown(void);
  protected:
    void test_parse_arugments(void);
    void test_run();
  protected:
    /**
     * blocks until the yapp master thread finshed running election & online
     */
    void start_yapp_service(void);
    static void * thread_yapp_service(void * yapp_client_test_ptr);
    /**
     * blocks until the yapp master thread finshed.
     */
    void stop_yapp_service(void);
    void sanity_check(void);

  private:
    void set_test_env(void);
    void load_test_cases(void);

    vector<string> * yapp_params_list_arr_ptr;
    vector<YappClient> yapp_client_arr;

    vector<string> * wrong_yapp_params_list_arr_ptr_0;
    vector<YappClient> wrong_yapp_client_arr_0;

    vector<string> * wrong_yapp_params_list_arr_ptr_1;
    vector<YappClient> wrong_yapp_client_arr_1;

    vector<string> * wrong_yapp_params_list_arr_ptr_2;
    vector<YappClient> wrong_yapp_client_arr_2;

    vector<string> yapp_worker_param_arr[NUM_OF_NODES];
    vector<string> yapp_worker_param_arr_bck[NUM_OF_NODES];
    // YappWorker * yapp_worker_arr[NUM_OF_NODES];
    pid_t yapp_worker_proc_id_arr[NUM_OF_NODES];
};

}
}
}

#endif
