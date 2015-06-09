#ifndef TEST_YAPP_CONF_H_
#define TEST_YAPP_CONF_H_
#include <vector>
#include <string>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace yapp {
namespace conf {

using std::vector;
using std::string;

const static string TEST_ROOT = "/test";

const static char * const TEST_ENV[] = {
  "/test",
  "/test/yapp",
  "/test/yapp/conf",
  "/test/yapp/conf/nodes",
  "/test/yapp/jobs",
  "/test/yapp/queue",
  "/test/yapp/queue/textrfin",
  "/test/yapp/queue/new",
  "/test/yapp/queue/ready",
  "/test/yapp/queue/running",
  "/test/yapp/queue/paused",
  "/test/yapp/queue/terminated",
  "/test/yapp/queue/failed",
};

const static char * const NEW_NODE_DIR_PATH = "/test/yapp/conf/nodes";
const static char * const NEW_NODE_DIR = "/test/yapp/conf/nodes/";
const static char * const NEW_NODE_PATH_ENTRY[] = {
  "aldkf", "2134", "235nvjha__@#Q$"
};

/**
 * Files listed here are input data.
 */
const static char * const INPUTS_FILE_ENTRY[] = {
  "./test_cfg_util_load_cfg.input",
};
/**
 * Files listed here are the corresponding data set as correct outcome.
 */
const static char * const OUTPUT_FILE_ENTRY[] = {
  "./test_cfg_util_load_cfg.output",
};
enum TEST_MAPPING_CODE {
  TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX = 0,
};

const static int NUM_OF_CLIENTS = 3;

}
}
}

#endif
