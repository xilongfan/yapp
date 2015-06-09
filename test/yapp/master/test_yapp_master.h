#ifndef TEST_YAPP_MASTER_H_
#define TEST_YAPP_MASTER_H_

#include <string>
#include <cstring>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include "yapp_master.h"
#include "../conf/test_yapp_conf.h"

namespace yapp {
namespace master {

using namespace yapp::conf;

// ./yapp --verbose  --zk_conn_str=192.168.1.1:2818 --autosplit=true --automigrate=true --proc_num=16 --env=RAILS_ENV=production --app=ruby /update_ra.rb --range=0 59 --args=-test -verbose --stdout=/tmp/update_ra.stdout --stderr=/tmp/update_ra.stdout
// ./yapp --mode=master --verbose  --zkconnstr=192.168.1.1:2818 --thrd_pool_size=100 --port=9999

const static char * const YAPP_MASTER_PARAM_ARR_WITH_CONN_STR[] = {
  "--zk_conn_str=192.168.1.1:2818",
  "--thrd_pool_size=100",
  "--port=9999",
}; 

const static char * const YAPP_MASTER_PARAM_ARR_WITH_CONN_STR_NO_POOLSIZE[] = {
  "--zk_conn_str=192.168.1.1:2818",
  "--port=9999",
}; 

const static char * const YAPP_MASTER_PARAM_ARR_WITH_CONN_STR_NO_PORT[] = {
  "--zk_conn_str=192.168.1.1:2818",
  "--thrd_pool_size=100",
}; 

const static char * const YAPP_MASTER_PARAM_ARR_WITH_CONN_STR_NO_PORT_POOLSIZE[] = {
  "--zk_conn_str=192.168.1.1:2818",
}; 

const static char * const YAPP_MASTER_PARAM_ARR_WITH_CFG_FILE[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--thrd_pool_size=100",
  "--port=9999",
}; 

const static char * const YAPP_MASTER_PARAM_ARR_WITH_CFG_FILE_NO_POOLSIZE[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--port=9999",
}; 

const static char * const YAPP_MASTER_PARAM_ARR_WITH_CFG_FILE_NO_PORT[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--thrd_pool_size=100",
}; 

const static char * const YAPP_MASTER_PARAM_ARR_WITH_CFG_FILE_NO_PORT_POOLSIZE[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
}; 

/** all this input should be able to be correctly parsed. **/
const static char * const * const YAPP_MASTER_PARAM_ARR_SET[] = {
  &YAPP_MASTER_PARAM_ARR_WITH_CONN_STR[0],
  &YAPP_MASTER_PARAM_ARR_WITH_CONN_STR_NO_POOLSIZE[0],
  &YAPP_MASTER_PARAM_ARR_WITH_CONN_STR_NO_PORT[0],
  &YAPP_MASTER_PARAM_ARR_WITH_CONN_STR_NO_PORT_POOLSIZE[0],

  &YAPP_MASTER_PARAM_ARR_WITH_CFG_FILE[0],
  &YAPP_MASTER_PARAM_ARR_WITH_CFG_FILE_NO_POOLSIZE[0],
  &YAPP_MASTER_PARAM_ARR_WITH_CFG_FILE_NO_PORT[0],
  &YAPP_MASTER_PARAM_ARR_WITH_CFG_FILE_NO_PORT_POOLSIZE[0],
};

const static int YAPP_MASTER_PARAM_ARR_SIZE_SET[] = {
  3, 2, 2, 1, 3, 2, 2, 1
};

class YappMasterTest : public CPPUNIT_NS::TestFixture {
  CPPUNIT_TEST_SUITE(YappMasterTest);
  CPPUNIT_TEST(test_parse_arguments);
  CPPUNIT_TEST_SUITE_END();
  public:
    void setUp(void);
    void tearDown(void);
  protected:
    void test_parse_arguments(void);
  private:
    vector<string> * yapp_params_list_arr_ptr;
    vector<YappMaster> yapp_master_arr;

    vector<string> * wrong_yapp_params_list_arr_ptr_0;
    vector<YappMaster> wrong_yapp_master_arr_0;

    vector<string> * wrong_yapp_params_list_arr_ptr_1;
    vector<YappMaster> wrong_yapp_master_arr_1;

    vector<string> * wrong_yapp_params_list_arr_ptr_2;
    vector<YappMaster> wrong_yapp_master_arr_2;
};

}
}

#endif
