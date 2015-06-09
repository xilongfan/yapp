/**
 * Desc:
 * - This file would contain the interface of a YAPP client running instance,
 *   which would provide a command line access for the users to submit their
 *   jobs and handles the communication between master node for deploying the
 *   tasks and running them in parallel across diff. slave nodes. 
 */

#ifndef YAPP_CLIENT_H_
#define YAPP_CLIENT_H_

#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <ctime>

#include "../base/yapp_base.h"
#include "../util/yapp_util.h"
#include "../domain/yapp_master_service_handler.h"
#include "../domain/yapp_domain_factory.h"

namespace yapp {
namespace client {

using std::string;
using std::vector;

using namespace yapp::base;
using namespace yapp::util;
using namespace yapp::domain;

const static int YAPP_CLIENT_INVALID = -1;
const static int YAPP_CLIENT_DEF_PROC_NUM = 16;

const static char * const RANGE_NUM_DELIM = " ";
const static char * const YAPP_CLIENT_OPTION_ENTRY[] = {
  "--zk_conn_str=",   // '192.168.1.1:2818'
  "--zk_cfg_file=",   // './test_cfg_util_load_cfg.input'
  "--autosplit=",     // true
  "--automigrate=",   // true
  "--proc_num=",      // 3"
  "--app_env=",       // 'RAILS_ENV=production'
  "--app_bin=",       // '/usr/bin/ruby'
  "--app_src=",       // '${PROG_PATH}/update_ra.rb'
  "--range=",         // '0 59"
  "--arg_str=",       // '-test -verbose'
  "--stdout=",        // /home/foo/log/update_ra.stdout"
  "--stderr=",        // /home/foo/log/update_ra.stderr"
  "--owner=",         // "xfan@spokeo.com"
  "--range_file=",    // '/var/tmp/range.info'
  "--working_dir=",   // '/var/www/test.foo.com/current'
  "--anchor_prfx=",   // 'YAPP_CUR_ID_POS='

  "--range_step=",    // 10, by setting this, user can skip range file.
};

enum YAPP_CLIENT_OPTION_ENTRY_MAPPING {
  YAPP_CLIENT_OPT_INVAL = -1,

  YAPP_CLIENT_OPT_ZK_CONN_STR = 0,
  YAPP_CLIENT_OPT_ZK_CFG_FILE = 1,
  YAPP_CLIENT_OPT_AUTO_SPLIT = 2,
  YAPP_CLIENT_OPT_AUTO_MIGRATE = 3,
  YAPP_CLIENT_OPT_PROC_NUMBER = 4,
  YAPP_CLIENT_OPT_APP_ENV = 5,
  YAPP_CLIENT_OPT_APP_BIN = 6,
  YAPP_CLIENT_OPT_APP_SRC = 7,

  YAPP_CLIENT_OPT_RANGE_INPUT = 8,
  YAPP_CLIENT_OPT_APP_ARGS = 9,
  YAPP_CLIENT_OPT_STDOUT = 10,
  YAPP_CLIENT_OPT_STDERR = 11,

  YAPP_CLIENT_OPT_JOB_OWNER = 12,

  YAPP_CLIENT_OPT_RANGE_FILE_INPUT = 13,

  YAPP_CLIENT_OPT_WORKING_DIR = 14,

  YAPP_CLIENT_OPT_ANCHOR_PRFX = 15,

  YAPP_CLIENT_OPT_RANGE_STEP = 16,

  YAPP_CLIENT_OPT_COUNT
};

enum YAPP_CLIENT_TASK_TYPE {
  YAPP_CLIENT_TASK_TYPE_INVALID = -1,

  YAPP_CLIENT_RANGE_TASK = 0,

  YAPP_CLIENT_RANGE_FILE_TASK = 1,

  YAPP_CLIENT_DYNAMIC_RANGE_TASK = 2,
  
  YAPP_CLIENT_TASK_COUNT
};

/**
 * - Some Jobs Can Be Easily Splitted by Its Range.
 * yapp --zk_conn_str='192.168.1.1:2818'
 *      { or --zk_cfg_file='./test_cfg_util_load_cfg.input' }
 *      --autosplit=true
 *      --automigration=true
 *      --proc_num=3
 *      --env='RAILS_ENV=production'
 *      --app='ruby /update_ra.rb'
 *      --range='0 59'
 *      --args='-test -verbose'
 *      --stdout='/home/foo/log/update_ra.stdout'
 *      --stderr='/home/foo/log/update_ra.stderr'
 *
 * - Some Jobs Cannot be Simply Splitted Since Special Configure Parameters Based
 *   on Pre-Computed Data are Needed, like ReverseAddressNeighborhoods Table. For
 *   Load Balancing, Sparse area across a large section of geocode_bucket range
 *   would normally require a coarser split, while area with dense population will
 *   need a finer split granularity.
 *
 *   Eg:
 *   # building command for area with sparse population.
 *   RAILS_ENV=production \
 *     ruby ${src_dir}/build_allnew_reverse_address_neighborhoods.rb \
 *          1557000001 1662500000 5275000 \
 *          > ${log_dir}/build_temp_ra_nieghbor_log_proc_1.stdout.${timestmp} \
 *          2>${log_dir}/build_temp_ra_nieghbor_log_proc_1.stderr.${timestmp} &
 *    
 *   # building command for area with sparse population.
 *   RAILS_ENV=production \
 *     ruby ${src_dir}/build_allnew_reverse_address_neighborhoods.rb \
 *          1662500001 1669500000 350000 \
 *          > ${log_dir}/build_temp_ra_nieghbor_log_proc_2.stdout.${timestmp} \
 *          2>${log_dir}/build_temp_ra_nieghbor_log_proc_2.stderr.${timestmp} &
 * 
 *   Our Strategy is to preserve all those configuration provided by the developer,
 *   and not do the auto splitting for each process. Instead of running it directly
 *   providing the range, developers could put all those configuration parameters
 *   in a text file and treat each minimum processing unit(a line for ASCII file or
 *   a fixed chunk of bytes for binary file) as an input. ALSO HERE IS WHERE OUR
 *   DATA-FLOW ARCHITECTURE COMES.
 * 
 *   Eg:
 *   # build_ra_nei.range
 *   1557000001 1662500000 5275000
 *   1662500001 1669500000 350000
 *   ...
 * 
 *   # submit the job and configuration using the file.
 *   yapp -h $host -p $port
 *        --autosplit --automigration --file
 *        --proc_num=64      # default process number to start
 *        --env='RAILS_ENV=production'
 *        --app='ruby ${PROG_PATH}/build_allnew_reverse_address_neighborhoods.rb'
 *        --input_file='${FILE_PATH}/build_ra_nei.range'
 *        --ftype=asc        # splitted by line
 *        --foffset=0        # start to read from line 0(if it is 0-based)
 *        --fmin_unit_size=1 # min consecutive lines for a single process to read
 *        --stdout=${log_dir}/build_temp_ra_nieghbor_log_proc.stdout
 *        --stderr=${log_dir}/build_temp_ra_nieghbor_log_proc.stderr
 *        --args='-test -verbose'
 *        --max_res_usage=45 # the max percent of resource to be granted
 * 
 *   Then what if my program needs to read a fixed chunk of a binary file?
 * 
 *   Eg.
 *   # submit the job and configuration using the file.
 *   yapp -h $host -p $port
 *        --autosplit --automigration --file
 *        --proc_num=64      # default process number to start
 *        --env='RAILS_ENV=production'
 *        --app='ruby ${PROG_PATH}/build_allnew_reverse_address_neighborhoods.rb'
 *        --input_file='${FILE_PATH}/build_ra_nei.range'
 *        --ftype=bin        # splitted by chunk
 *        --foffset=0        # start to read from byte 0(if it is 0-based)
 *        --fmin_unit_size=1 # consecutive bytes for a single process to read
 *        --stdout=${log_dir}/build_temp_ra_nieghbor_log_proc.stdout
 *        --stderr=${log_dir}/build_temp_ra_nieghbor_log_proc.stderr
 *        --args='-test -verbose'
 *        --max_res_usage=45 # the max percent of resource to be granted
 */
class YappClient : public YappBase {
public:
  YappClient(vector<string> args, bool b_verb, bool b_test = false);
  virtual ~YappClient();
  virtual YAPP_MSG_CODE run();
  virtual YAPP_MSG_CODE parse_arguments();
  virtual void usage();

  YAPP_MSG_CODE init_zk_cluster_conn_by_str();

  void print_task();

  Job job_obj;

private:
  ZkClusterProxy * zkc_proxy_ptr;

  string zk_conn_str;
  bool   b_auto_split;
  bool   b_auto_migrate;
  int    proc_num;
  string app_env;
  string app_bin;
  string app_src;
  string job_owner;
  string working_dir;
  string anchor_prfx;

  long long int range_from;
  long long int range_to;
  long long int range_step;
  string range_file;

  string app_args;
  string fstdout;
  string fstderr;

  YAPP_CLIENT_TASK_TYPE task_type;

  ConfigureUtil cfg_obj;
};

}
}
#endif // YAPP_CLIENT_H_
