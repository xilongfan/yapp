/**
 * Desc:
 * - This files contains the interface of YAPP worker instance, which will act
 *   as a slave for actually running those unassigned tasks logged on zookeeper.
 *
 */

#ifndef YAPP_WORKER_H_
#define YAPP_WORKER_H_
#include <iostream>
#include <vector>
#include <string>

#include "../base/yapp_base.h"
#include "../util/yapp_util.h"
#include "../domain/yapp_service_handler.h"

namespace yapp {
namespace worker {

using std::string;
using std::vector;
using boost::shared_ptr;

using namespace yapp::base;
using namespace yapp::util;
using namespace yapp::domain;

const static char * const YAPP_WORKER_OPTION_ENTRY[] = {
  "--port=", "--zk_conn_str=", "--thrd_pool_size=", "--zk_cfg_file=",
};

enum YAPP_WORKER_OPTION_ENTRY_MAPPING {
  YAPP_WORKER_OPT_PORT = 0,       YAPP_WORKER_OPT_CONN_STR,
  YAPP_WORKER_OPT_THRD_POOL_SIZE, YAPP_WORKER_OPT_ZKCFG, 
  YAPP_WORKER_OPT_COUNT
};

class YappWorker : public YappBase {
public:
  YappWorker(vector<string> args, const ConfigureUtil & cobj, bool b_test = false,
             int shmid = -1, int shmsz = 0);
  YappWorker(vector<string> args, bool b_verb, bool b_test = false,
             int shmid = -1, int shmsz = 0);

  virtual ~YappWorker() {}
  virtual YAPP_MSG_CODE run();
  virtual YAPP_MSG_CODE parse_arguments();
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
#endif // YAPP_WORKER_H_
