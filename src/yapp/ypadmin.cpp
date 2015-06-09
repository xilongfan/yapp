#include "./ypadmin.h"
#include "./util/yapp_util.h"
#include "./admin/yapp_admin.h"

using namespace yapp;
using namespace yapp::base;
using namespace yapp::util;
using yapp::admin::YappAdmin;

YAPP_MSG_CODE YappAdminApp::parse_args(int argc, char * argv[]) {
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

YAPP_MSG_CODE YappAdminApp::run(int argc, char * argv[]) {
  argv++; argc--;
  parse_args(argc, argv);
  yapp_obj_ptr = new YappAdmin(arg_arr, b_verbose);
  return yapp_obj_ptr->run();
}

int main(int argc, char * argv[]) {
  IPC_CONTEX::init_ipc_contex();
  YappAdminApp yapp_obj;
  return yapp_obj.run(argc, argv);
}
