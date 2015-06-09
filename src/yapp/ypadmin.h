/**
 * Desc:
 * - This file contains the centralized main interface for yapp admin util.
 *
 * Dependencies:
 * - ./util
 * - ./domain
 * - ./admin
 *
 * Last Updated By: 06-AUG-2013, Xilong Fan
 */

#ifndef YPADMIN_H_
#define YPADMIN_H_

#include <iostream>
#include <vector>
#include <string>
#include <algorithm>

#include <zookeeper/zookeeper.h>

#include "./base/yapp_base.h"

namespace yapp {

using std::string;
using std::vector;
using namespace yapp::base;
  
class YappAdminApp : public YappApp {
public:
  YappAdminApp() : YappApp() {
    yapp_mode_code = YAPP_MODE_ADMIN;
  }
  virtual ~YappAdminApp() {}
  virtual YAPP_MSG_CODE run(int argc, char * argv[]);
  virtual YAPP_MSG_CODE parse_args(int argc, char * argv[]);
};

}
#endif
