#include "./yapp.h"
#include "./util/yapp_util.h"
#include "./client/yapp_client.h"

using namespace yapp;
using namespace yapp::base;
using namespace yapp::util;
using yapp::client::YappClient;

YAPP_MSG_CODE YappClientApp::parse_args(int argc, char * argv[]) {
  YAPP_MSG_CODE rc = YAPP_MSG_INVALID;
  string param_str;
  if (argc > 0) {
    for (int i = 0; i < argc; i++) {
      param_str = string(argv[i]);
      if (param_str == YAPP_VERBOSE_FLAG) {
        b_verbose = true;
      } else {
        arg_arr.push_back(argv[i]);
      }
    }
  }
  return rc;
}

YAPP_MSG_CODE YappClientApp::run(int argc, char * argv[]) {
  argv++; argc--;
  parse_args(argc, argv);
  yapp_obj_ptr = new YappClient(arg_arr, b_verbose);
  return yapp_obj_ptr->run();
}

int main(int argc, char * argv[]) {
  IPC_CONTEX::init_ipc_contex();
  YappClientApp yapp_obj;
  return yapp_obj.run(argc, argv);
}
