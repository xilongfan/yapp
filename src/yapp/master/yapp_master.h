/**
 * Desc:
 * - This files contains the interface of YAPP master instance, which will act
 *   as a coordinator for scheduling & monitoring processes running across diff.
 *   slave nodes, note that there actually will not be too much communication
 *   between master & slave direclty given that using zookeeper cluster as the
 *   centralized service for exchanging messages.
 *
 * - The YappMaster process(running on master node as a daemon) do nothing but
 *   listens at a port specified by user(or a 'well-known' port --> 9527) for
 *   accepting the job (a sequence of well-specified tasks to run in parallel
 *   mode) submission from any YappClient instance.
 *
 * - Upon receiving the job submission, the YappMaster would do the following:
 *   1. splitting each task in the job into finer pieces(id range or file I/O)
 *   2. log down the job info. into ZooKeeper Cluster(as WHL) by creating nodes
 *   3. based on the emphemeral nodes presented in the election path, notify the
 *      corresponding worker nodes to check out available sub-tasks.
 *   4. perform global synchronization (per job) using barriers for the ordering
 *      specified in job description, monitoring the running of all sub-tasks.
 *   
 * - Eg. rebuild_reverse_address.yapp
 *       job_begin
 *         1. rebuild table reverse_tmp_addresses { sub-task 0, 1, ... n }
 *         barrier_block;
 *         2. rebuild reverse_tmp_address_street_datas { sub-task 0, 1, ... n }
 *         3. rebuild reverse_tmp_address_neighborhoods { sub-task 0, 1, ... n }
 *         barrier_block;
 *         4. swapping current table with tmp table.
 *       job_end
 *
 * - Server would be per job Threaded, using either blocking or non-blocking I/O
 *   and implemented based on a robust RPC framework provided by Apache Thrift.
 */

#ifndef YAPP_MASTER_H_
#define YAPP_MASTER_H_
#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include <boost/shared_ptr.hpp>

#include "../util/yapp_util.h"
#include "../base/yapp_base.h"
#include "../domain/yapp_service_handler.h"

namespace yapp {
namespace master {

using std::string;
using std::vector;
using boost::shared_ptr;

using namespace yapp::base;
using namespace yapp::util;
using namespace yapp::domain;

const static char * const YAPP_MASTER_OPTION_ENTRY[] = {
  "--port=", "--zk_conn_str=", "--thrd_pool_size=", "--zk_cfg_file=",
};

enum YAPP_MASTER_OPTION_ENTRY_MAPPING {
  YAPP_MASTER_OPT_PORT = 0,       YAPP_MASTER_OPT_CONN_STR,
  YAPP_MASTER_OPT_THRD_POOL_SIZE, YAPP_MASTER_OPT_ZKCFG, 
  YAPP_MASTER_OPT_COUNT
};

class YappMaster : public YappBase {
public:
  YappMaster(vector<string> args, const ConfigureUtil & cobj, bool b_test = false,
             int shmid = -1, int shmsz = 0);
  YappMaster(vector<string> args, bool b_verb, bool b_test = false,
             int shmid = -1, int shmsz = 0);

  virtual ~YappMaster();
  virtual YAPP_MSG_CODE run();
  /**
   * Desc:
   * - This method would parse the arguments list directly come from the command
   *   line, also note that they should be all lower-cased during the previous
   *   pass by the app portal Yapp::parse_args.
   * Return:
   * - YAPP_MSG_CODE rc: YAPP_MSG_SUCCESS if everything is ok.
   */
  virtual YAPP_MSG_CODE parse_arguments();
  /**
   * Desc:
   * - This method will print usage info. every time there is a error in parsing
   */
  virtual void usage();

  string get_zk_conn_str() { return cfg_obj.get_zk_cluster_conn_str(); }

  void set_cfg_obj(const ConfigureUtil & cobj) { cfg_obj = cobj; }
protected:
  ConfigureUtil cfg_obj;
  int shrd_mem_id;
  int shrd_mem_sz;
};

}
}
#endif // YAPP_MASTER_H_
