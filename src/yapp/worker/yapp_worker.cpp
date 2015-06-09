#include "./yapp_worker.h"

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/server/TThreadPoolServer.h>

using namespace apache::thrift;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

using namespace yapp::worker;

YappWorker::YappWorker(
  vector<string> args, const ConfigureUtil & cobj, bool b_test, int shmid, int shmsz) :
    YappBase(args, cobj.b_verbose, b_test)
{
  cfg_obj = cobj;
  shrd_mem_id = shmid;
  shrd_mem_sz = shmsz;
}

YappWorker::YappWorker(vector<string> args, bool b_verb, bool b_test, int shmid, int shmsz):
   YappBase(args, b_verb, b_test)
{
  shrd_mem_id = shmid;
  shrd_mem_sz = shmsz;
}

void YappWorker::usage() {
  std::cerr << "Usage When Run Yapp in Master or Worker<def> Mode:"
            << std::endl
            << std::endl
            << "yappd [--zk_cfg_file='/etc/yapp.cfg']"
            << std::endl
            << "      [--mode=[master|worker<def>]] [--zk_conn_str=192.168.1.1:2181]"
            << std::endl
            << "      [--port=9527{def}] [--thrd_pool_size=16{def}]"
            << std::endl;
}

YAPP_MSG_CODE YappWorker::parse_arguments()
{
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID_ARGS;

  int argc = arg_arr.size();
  bool b_arg_valid = true;

  size_t kv_delim_pos = -1;
  string key_str;
  string val_str;
  for (int i = 0; i < argc; i++) {
#ifdef DEBUG_YAPP_WORKER
    std::cerr << arg_arr[i] << std::endl;
#endif
    if (false == b_arg_valid) { break; }
    kv_delim_pos = arg_arr[i].find(KEY_VALUE_DELIM, 0);
#ifdef DEBUG_YAPP_WORKER
    std::cerr << "delim_pos" << ":" << kv_delim_pos << std::endl;
#endif
    if (string::npos == kv_delim_pos) {
      b_arg_valid = false;
      break;
    }
    key_str = arg_arr[i].substr(0, kv_delim_pos + 1);
    val_str = arg_arr[i].substr(kv_delim_pos + 1);
    if (key_str.empty() || val_str.empty()) {
      b_arg_valid = false;
      break;
    }
#ifdef DEBUG_YAPP_WORKER
    std::cerr << key_str << ":" << val_str << std::endl;
#endif
    for (size_t c = 0; c < sizeof(YAPP_WORKER_OPTION_ENTRY) /
                           sizeof(char *); c++) {
      if (false == b_arg_valid) { break; }
      if (key_str != YAPP_WORKER_OPTION_ENTRY[c]) {
        continue;
      }
      switch ((YAPP_WORKER_OPTION_ENTRY_MAPPING)c) {
        case YAPP_WORKER_OPT_PORT: {
          if (true == StringUtil::is_integer(val_str)) {
            cfg_obj.port_num = atoi(val_str.c_str());
            break;
          }
        }
        case YAPP_WORKER_OPT_CONN_STR: {
          cfg_obj.zk_conn_str = val_str;
          break;
        }
        case YAPP_WORKER_OPT_THRD_POOL_SIZE: {
          if (true == StringUtil::is_integer(val_str)) {
            cfg_obj.thrd_pool_size = atoi(val_str.c_str());
            break;
          }
        }
        case YAPP_WORKER_OPT_ZKCFG: {
          if (true == cfg_obj.load_zk_cluster_cfg(val_str)) {
            break;
          }
        }
        default: {
#ifdef DEBUG_YAPP_WORKER
          std::cerr << "invalid argument: " << key_str << ":" << val_str
                    << std::endl;
#endif
          b_arg_valid = false;
          break;
        }
      } /* switch ((YAPP_WORKER_OPTION_ENTRY_MAPPING)c) */
    } /* for (size_t c = 0; c < sizeof(YAPP_WORKER_OPTION_ENTRY) */
  } /* for (int i = 0; i < argc; i++) */
#ifdef DEBUG_YAPP_WORKER
  std::cerr << "b_arg_valid: " << b_arg_valid << std::endl;
  std::cerr << "cfg_obj.port_num: " << cfg_obj.port_num << std::endl;
  std::cerr << "cfg_obj.thrd_pool_size: " << cfg_obj.thrd_pool_size << std::endl;
  std::cerr << "cfg_obj.zk_conn_str: " << cfg_obj.zk_conn_str << std::endl;
#endif
  if (true == b_arg_valid && cfg_obj.port_num > 0 && cfg_obj.thrd_pool_size > 0
                          && false == cfg_obj.zk_conn_str.empty()) {
    rc = YAPP_MSG_SUCCESS;
  }
  return rc;
};


YAPP_MSG_CODE YappWorker::run() {
  /* 1. parse the commond line arg array defined in YappBase. */
  YAPP_MSG_CODE rc = parse_arguments();
  if (YAPP_MSG_SUCCESS != rc) {
    usage();
    return rc;
  }
  if (true == b_verbose) {
    std::cout << "-- running yapp at worker mode" << std::endl;
    print_arg_list();
  }

  /**
   * +-------------------------------------------+
   * | Server (threaded on per job basis)        |
   * +-------------------------------------------+
   * | Processor (thrift generate the interface) |
   * +-------------------------------------------+
   * | Protocol (binary)                         |
   * +-------------------------------------------+
   * | Transport (TCP)                           |
   * +-------------------------------------------+
   */

  /* 2-1, choose the protocol. */
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  YappServiceHandler * yapp_srv_hnd_ptr = new YappServiceHandler(
    cfg_obj, false, b_verbose, b_testing, shrd_mem_id, shrd_mem_sz
  );
  if (NULL == yapp_srv_hnd_ptr) {
    return YAPP_MSG_ERROR_ALLOCATING_YAPP_SERV;
  }
  if (true != yapp_srv_hnd_ptr->join_yapp_nodes_group() ||
      true != yapp_srv_hnd_ptr->start_yapp_event_handler()) {
    return YAPP_MSG_ERROR_JOIN_YAPP_GROUP;
  }
  /* 2-2, build the processor based on the selected handler. */
  shared_ptr<YappServiceHandler> yapp_worker_srv_hdler(yapp_srv_hnd_ptr);
  shared_ptr<TProcessor> yapp_worker_processor(
    new YappServiceProcessor(yapp_worker_srv_hdler)
  );
  /* 2-3, choose thread manager & set thead factory */
  shared_ptr<ThreadManager> yapp_worker_thrd_manager =
      ThreadManager::newSimpleThreadManager(cfg_obj.thrd_pool_size);
  shared_ptr<PosixThreadFactory> yapp_worker_thrd_factory =
      shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
  yapp_worker_thrd_manager->threadFactory(yapp_worker_thrd_factory);
  yapp_worker_thrd_manager->start();

  /* 2-4, initialize & start the server in non-blocking mode. */
  shared_ptr<TServerTransport> yapp_worker_srv_trans(
      new TServerSocket(cfg_obj.port_num)
  );
  shared_ptr<TTransportFactory> yapp_worker_trans_fac(
      new TBufferedTransportFactory()
  );
  TThreadPoolServer yapp_worker_server(
    yapp_worker_processor, yapp_worker_srv_trans,   yapp_worker_trans_fac,
    protocol_factory,      yapp_worker_thrd_manager
  );

  if (true == b_verbose) {
    std::cout << "Starting Yapp Worker Daemon on Port "
              << cfg_obj.port_num
              << " with the initial threading pool size "
              << cfg_obj.thrd_pool_size
              << std::endl;
  }
  yapp_worker_server.serve();
  if (true == b_verbose) {
    std::cout << "Finish Running Yapp Worker Daemon on Port "
              << cfg_obj.port_num
              << " with the initial threading pool size "
              << cfg_obj.thrd_pool_size
              << std::endl;
  }
  return YAPP_MSG_SUCCESS;
}
