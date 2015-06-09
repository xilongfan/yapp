/**
 * Desc:
 * - This file contains the centralized main interface for yapp client.
 *
 * Dependencies:
 * - ./util
 * - ./domain
 * - ./client
 *
 * Last Updated By: 06-AUG-2013, Xilong Fan
 */

#ifndef YAPP_H_
#define YAPP_H_

#include <iostream>
#include <vector>
#include <string>
#include <algorithm>

#include <zookeeper/zookeeper.h>

#include "./base/yapp_base.h"

#ifdef DEBUG_YAPP
  #define PR(X) std::cout << #X << " = " << X << std::endl;
  #define PRS(X) std::cout << #X << std::endl;
  #define PRL() std::cout << std::endl;
#endif


namespace yapp {

using std::string;
using std::vector;
using namespace yapp::base;
  
class YappClientApp : public YappApp {
public:
  YappClientApp() : YappApp() {
    yapp_mode_code = YAPP_MODE_CLIENT;
  }
  virtual ~YappClientApp() {}

  virtual YAPP_MSG_CODE run(int argc, char * argv[]);
  virtual YAPP_MSG_CODE parse_args(int argc, char * argv[]);
};

}
#endif
