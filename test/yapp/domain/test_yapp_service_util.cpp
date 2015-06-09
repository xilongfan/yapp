#include <config.h>

#include "../conf/test_yapp_conf.h"
#include "test_yapp_service_util.h"

using namespace yapp::domain;

CPPUNIT_TEST_SUITE_REGISTRATION(TaskHandleUtilTest);
CPPUNIT_TEST_SUITE_REGISTRATION(JobDataStrParserTest);

void TaskHandleUtilTest::setUp(void) { }

void TaskHandleUtilTest::tearDown(void) { }

void TaskHandleUtilTest::test_get_task_handle_type(void)
{
  for (size_t i = 0; i < sizeof(ERROR_TASK_HNDL_STR_ARR)/sizeof(char *); i++) {
#ifdef DEBUG_YAPP_SERVICE_UTIL
    std::cerr << "==>> GOING TO VERIFY: "
              << ERROR_TASK_HNDL_STR_ARR[i]
              << std::endl;
#endif
    CPPUNIT_ASSERT_EQUAL(
      ERROR_TASK_HNDL_STR_ARR_RET,
      TaskHandleUtil::get_task_handle_type(string(ERROR_TASK_HNDL_STR_ARR[i]))
    );
  }
  for (size_t i = 0; i < sizeof(CORRECT_TSK_HNDL_STR_ARR)/sizeof(char *); i++) {
    CPPUNIT_ASSERT_EQUAL(
      CORRECT_TSK_HNDL_STR_ARR_RET[i],
      TaskHandleUtil::get_task_handle_type(string(CORRECT_TSK_HNDL_STR_ARR[i]))
    );
  }
}

void JobDataStrParserTest::setUp(void) { }

void JobDataStrParserTest::tearDown(void) { }

void JobDataStrParserTest::test_parse_proc_host_str(void)
{
  string host_str, host_ip, host_prt, yapp_pid;
 
  for (size_t i = 0; i < sizeof(INVALID_HOST_STR_ARR)/sizeof(char *); i++) {
#ifdef DEBUG_YAPP_SERVICE_UTIL
    std::cerr << "==>> VERIFY: " << INVALID_HOST_STR_ARR[i] << std::endl;
#endif
    host_str = INVALID_HOST_STR_ARR[i];
    CPPUNIT_ASSERT_EQUAL(false,
      JobDataStrParser::parse_proc_host_str(
        host_str, host_ip, host_prt, yapp_pid
      )
    );
  }
  for (size_t i = 0; i < sizeof(VALID_HOST_STR_ARR)/sizeof(char *); i++) {
#ifdef DEBUG_YAPP_SERVICE_UTIL
    std::cerr << "==>> VERIFY: " << VALID_HOST_STR_ARR[i] << std::endl;
#endif
    host_str = VALID_HOST_STR_ARR[i];
    CPPUNIT_ASSERT_EQUAL(true,
      JobDataStrParser::parse_proc_host_str(
        host_str, host_ip, host_prt, yapp_pid
      )
    );
    CPPUNIT_ASSERT_EQUAL(host_ip, string(VALID_IP_ARR[i]));
    CPPUNIT_ASSERT_EQUAL(host_prt, string(VALID_PORT_ARR[i]));
    CPPUNIT_ASSERT_EQUAL(yapp_pid, string(VALID_PID_ARR[i]));
  }
}
