#ifndef TEST_YAPP_ADMIN_H_
#define TEST_YAPP_ADMIN_H_

#include <string>
#include <cstring>
#include <unistd.h>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include "yapp_admin.h"
#include "../client/yapp_client.h"
#include "../domain/yapp_domain_factory.h"
#include "../base/yapp_base.h"
#include "../worker/yapp_worker.h"
#include "../domain/gen-cpp/yapp_service_constants.h"
#include "../conf/test_yapp_conf.h"

namespace yapp {
namespace admin {

using namespace yapp::base;
using namespace yapp::conf;
using namespace yapp::worker;
using namespace yapp::client;
using namespace yapp::domain;

using std::ifstream;
using std::string;
using std::ios;
using std::vector;

/**
Usage When Run Yapp in Client Mode:
yapp [--mode=client] [--verbose]

     --zk_conn_str=192.168.1.1:2181 | --zk_cfg_file='/etc/yapp.cfg'
     [--autosplit=(true|false)] [--automigrate=(true|false)] [--proc_num=${num}<16 def.>]
     [--app_env='RAILS_ENV=production']
     --app_bin='/usr/bin/ruby /update_ra.rb' --range_file='0 59' [--arg_str='-test -verbose']
     --stdout='/tmp/update_ra.stdout' --stderr='/tmp/update_ra.stdout'

Note: [] --> optional
**/

const static char * const YAPP_ADMIN_PARAM_ARR_QUSTAT[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--qstat",
};
const static char * const YAPP_ADMIN_PARAM_ARR_PRINT[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--print",
};
const static char * const YAPP_ADMIN_PARAM_ARR_PAUSE[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--pause",
};
const static char * const YAPP_ADMIN_PARAM_ARR_RESUME[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--resume",
};

const static char * const YAPP_ADMIN_PARAM_ARR_KILL[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--kill",
};
const static char * const YAPP_ADMIN_PARAM_ARR_RESTART[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--restart",
};
const static char * const YAPP_ADMIN_PARAM_ARR_PURGE[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--purge",
};

const static int YAPP_ADMIN_PARAM_ARR_SIZE_SET[] = {
  2, 2, // 2, 2, 2, 2, 2,
};

const static char * const * const YAPP_ADMIN_PARAM_ARR_SET[] = {
  &YAPP_ADMIN_PARAM_ARR_QUSTAT[0],
  &YAPP_ADMIN_PARAM_ARR_PRINT[0],
  /*
  &YAPP_ADMIN_PARAM_ARR_PAUSE[0],
  &YAPP_ADMIN_PARAM_ARR_RESUME[0],
  &YAPP_ADMIN_PARAM_ARR_KILL[0],
  &YAPP_ADMIN_PARAM_ARR_RESTART[0],
  &YAPP_ADMIN_PARAM_ARR_PURGE[0],
  */
}; 

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_CONN_STR[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=32",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=3 -test -verbose",
  "--stdout=./update_ra_by_zip.stdout",
  "--stderr=./update_ra_by_zip.stderr",
}; 

const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_AUTO[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
//  "--autosplit=true",
//  "--automigration=true",
  "--proc_num=64",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=4 -test -verbose",
  "--stdout=./update_ra_by_zip.stdout",
  "--stderr=./update_ra_by_zip.stderr",
}; 

const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=256",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=16 -test -verbose",
  "--stdout=./update_ra_by_zip.stdout",
  "--stderr=./update_ra_by_zip.stderr",
}; 
const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
//  "--autosplit=true",
//  "--automigration=true",
  "--proc_num=128",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=16 -test -verbose",
  "--stdout=./update_ra_by_zip.stdout",
  "--stderr=./update_ra_by_zip.stderr",
}; 
const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO_ENV[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
//  "--autosplit=true",
//  "--automigration=true",
  "--proc_num=192",
//  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=16 -test -verbose",
  "--stdout=./update_ra_by_zip.stdout",
  "--stderr=./update_ra_by_zip.stderr",
}; 

const static char * const YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO_ENV_ARG[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
//  "--autosplit=true",
//  "--automigration=true",
  "--proc_num=64",
//  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=20 -test -verbose",
  "--stdout=./update_ra_by_zip.stdout",
  "--stderr=./update_ra_by_zip.stderr",
}; 

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_CFG_FILE[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=96",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=5 -test -verbose",
  "--stdout=./update_ra_by_zip.stdout",
  "--stderr=./update_ra_by_zip.stderr",
}; 

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITHOUT_IO_OPT[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=160",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=2 -test -verbose",
}; 

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_DEF_IO_OPT[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=32",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=7 -test -verbose",
  "--stdout=/dev/null",
  "--stderr=/dev/null",
}; 

const static char * const YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_PARTIAL_IO[] = {
  "--zk_cfg_file=./test_cfg_util_load_cfg.input",
  "--autosplit=true",
  "--automigration=true",
  "--proc_num=128",
  "--app_env=RAILS_ENV=production",
  "--app_bin=/usr/bin/ruby",
  "--app_src=./update_ra_by_zip.rb",
  "--range_file=./zip.info",
  "--arg_str=9 -test -verbose",
  "--stdout=./update_ra_by_zip.stdout",
  "--stderr=/dev/null",
}; 

const static char * const YAPP_MASTER_PARAM_ARR[][3] = {
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=10",
    "--port=9527",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=10",
    "--port=9528",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=10",
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
    "--thrd_pool_size=15",
    "--port=9532",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=25",
    "--port=9533",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=15",
    "--port=9534",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
   "--thrd_pool_size=20",
    "--port=9535",
  },
  {
    "--zk_cfg_file=./test_cfg_util_load_cfg.input",
    "--thrd_pool_size=25",
    "--port=9536",
  },
};


const static int YAPP_MASTER_PARAM_CNT = 3;

const static char * const * const YAPP_CLIENT_PARAM_ARR_SET[] = {
  // &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_CONN_STR[0],
  // &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_AUTO[0],

  &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM[0],
  /*
  &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO[0],
  &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO_ENV[0],
  &YAPP_CLIENT_PARAM_WITH_CONN_STR_NO_PROCNUM_AUTO_ENV_ARG[0],
 
  &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_CFG_FILE[0],

  &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITHOUT_IO_OPT[0],
  &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_DEF_IO_OPT[0],
  &YAPP_CLIENT_RANGE_TASK_PARAM_ARR_WITH_PARTIAL_IO[0],
  */
}; 


const static int YAPP_CLIENT_PARAM_ARR_SIZE_SET[] = {
  // 11, 9,
  11,
  // 9, 8, 8, 11, 9, 11, 11
};
/*
  "--stdout=./update_ra.stdout", "--stderr=./update_ra.stderr",
  "--stdout=./update_ra.stdout", "--stderr=./update_ra.stderr",
  "--stdout=./update_ra_exception.stdout", "--stderr=./update_ra_exception.stderr",
  "--stdout=./update_ra_exception_div_zero.stdout", "--stderr=./update_ra_exception_div_zero.stderr",
  "--stdout=./update_ra_exception_nil.stdout", "--stderr=./update_ra_exception_nil.stderr",
  "--stdout=./update_ra_exception_require.stdout", "--stderr=./update_ra_exception_require.stderr",
  "--stdout=./update_ra_exception_syntax.stdout", "--stderr=./update_ra_exception_syntax.stderr",
  "--app_src=./update_ra_by_zip.rb",
  "--app_src=./update_ra_by_zip.rb",
  "--app_src=./update_ra_by_zip.rb",
*/

const static int YAPP_CLIENT_RETVAL_ARR[] = {
  // 0, 0,
  0,
  // 0, 0, 0, 0, 0, 0, 0
};

const static int YAPP_CLIENT_JOBCNT_ARR[] = {
  // 32,64,
  256,
  // 128, 192, 64, 96, 160, 32, 128
};

vector<string> YAPP_CLIENT_ID_TO_JOB_HND_MAPPING;


const static int NUM_OF_NODES = 5;

class YappAdminTest : public CPPUNIT_NS::TestFixture {
  CPPUNIT_TEST_SUITE(YappAdminTest);
  CPPUNIT_TEST(test_parse_arguments);
  CPPUNIT_TEST(test_run);
  CPPUNIT_TEST_SUITE_END();
  public:
    void setUp(void);
    void tearDown(void);
  protected:
    void test_parse_arguments(void);
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
    void load_all_proc_hndl_for_admin_test(void);

    void set_test_env(void);
    void load_test_cases(void);
    void print_queue_stat(ZkClusterProxy * zk_ptr);
    void print_queue_stat(ZkClusterProxy * zk_ptr, const string & hndl_str);

    bool check_progress(ZkClusterProxy * zk_ptr);

    vector<string> * yapp_params_list_arr_ptr;
    vector<YappClient> yapp_client_arr;
    vector<string> hndl_for_tasks_to_show;

    vector<string> * yapp_admin_params_list_arr_ptr;
    vector<YappAdmin> yapp_admin_arr;
    vector<vector<string> > yapp_admin_actual_cases; 
    vector<string> proc_hndls_arr;
    vector<string> task_hndls_arr;
    vector<string> jobs_hndls_arr;

    vector<string> yapp_worker_param_arr[NUM_OF_NODES];
    vector<string> yapp_worker_param_arr_bck[NUM_OF_NODES];
    pid_t yapp_worker_proc_id_arr[NUM_OF_NODES];
};

}
}

#endif
