#ifndef TEST_YAPP_SERVICE_UTIL_H_
#define TEST_YAPP_SERVICE_UTIL_H_

#include <string>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include "yapp_service_util.h"

namespace yapp {
namespace domain {

using std::string;

const static char * ERROR_TASK_HNDL_STR_ARR[] = {
  "kljdflkajd fkhaskdfjhyapoiru2	98q7u0	289u54oi	35j",
  "",
  "    ",
  "\n\t",
  "job-00000234ff_task-93254283_proc-4534 436436", /* incorrect digit seq. */
  "job-_task-999999_proc-545",                     /* missing digit seq. */
  "job-0000_task-_proc-5435",                      /* missing digit seq. */
  "job-0000_task-5555_proc-",                      /* missing digit seq. */
  "job-0000_proc-9999_task-5555",                  /* incorrect layer order. */
  "job-0999_proc-5555",                            /* missing some layer. */
  "task-555555_proc-44",                           /* missing some layer. */
  "proc-5443536",                                  /* missing some layer. */
  "proc-54353_task-555555",                        /* missing some layer. */
  "task-55555_job-2222_proc-5435",                 /* incorrect layer order. */
  "proc-54353_task-555555_job-543636",             /* incorrect layer order. */
};

const static int ERROR_TASK_HNDL_STR_ARR_RET =
  TaskHandleUtil::TASK_HANDLE_TYPE_INVALID;

const static char * CORRECT_TSK_HNDL_STR_ARR[] = {
  "job-0000_task-000_proc-00",                     /* testing diff seq. len */
  "job-000_task-00_proc-0",                        /* testing boundary cond. */
  "job-0_task-00_proc-000",                        /* testing boundary cond. */
  "job-00_task-0_proc-000",                        /* testing boundary cond. */
  "job-0_task-0_proc-0",                           /* testing boundary cond. */
  "job-0000_task-0001_proc-0001",                  /* normal case for proc hnd*/
  "job-0000_task-0001",                            /* normal case for tsk hnd */
  "job-0000",                                      /* normal case for job hnd */
  " job-0000_task-000_proc-00",                    /* testing diff seq. len */
  "job-000_task-00_proc-0 ",                       /* testing boundary cond. */
  " job-0_task-00_proc-000 ",                      /* testing boundary cond. */
  "\tjob-00_task-0_proc-000\n",                    /* testing boundary cond. */
  "\t job-0_task-0_proc-0 ",                       /* testing boundary cond. */
  "job-0000_task-0001_proc-0001\n",                /* normal case for proc hnd*/
  "job-0000_task-0001 \t",                         /* normal case for tsk hnd */
  "job-0000\r\n",                                  /* normal case for job hnd */
};


const static int CORRECT_TSK_HNDL_STR_ARR_RET[] = {
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_TASK,
  TaskHandleUtil::TASK_HANDLE_TYPE_JOBS,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_PROC,
  TaskHandleUtil::TASK_HANDLE_TYPE_TASK,
  TaskHandleUtil::TASK_HANDLE_TYPE_JOBS,
};

const static char * INVALID_HOST_STR_ARR[] = {
  "/test/yapp/conf/nodes/yapp-0000000006:192.168.1.1:9535",
  "/test/yapp/conf/nodes/yapp-0000000006:192.168.1.1:9535:",
  ":192.168.1.1:9:1",
  "::9:1",
  ":10.1.1.2::",
};

const static char * VALID_HOST_STR_ARR[] = {
  "/test/yapp/conf/nodes/yapp-0000000006:192.168.1.1:9535:10029",
  "/test/yapp/conf/nodes/yapp-0000000006:192.168.1.2:9535:1",
  "/test/yapp/conf/nodes/yapp-0000000006:192.168.1.3:9:1",
  "/test/yapp/conf/nodes/yapp-0000000006:192.168.1.4:9:1",
  "6:10.1.1.1:9:1",
};

const static char * VALID_IP_ARR[] = {
  "192.168.1.1",
  "192.168.1.2",
  "192.168.1.3",
  "192.168.1.4",
  "10.1.1.1",
};

const static char * VALID_PORT_ARR[] = {
  "9535",
  "9535",
  "9",
  "9",
  "9",
};

const static char * VALID_PID_ARR[] = {
  "10029",
  "1",
  "1",
  "1",
  "1",
};

class TaskHandleUtilTest : public CPPUNIT_NS::TestFixture {
  CPPUNIT_TEST_SUITE(TaskHandleUtilTest);
  CPPUNIT_TEST(test_get_task_handle_type);
  CPPUNIT_TEST_SUITE_END();
  public:
    void setUp(void);
    void tearDown(void);
  protected:
    void test_get_task_handle_type(void);
};

class JobDataStrParserTest : public CPPUNIT_NS::TestFixture {
  CPPUNIT_TEST_SUITE(JobDataStrParserTest);
  CPPUNIT_TEST(test_parse_proc_host_str);
  CPPUNIT_TEST_SUITE_END();
  public:
    void setUp(void);
    void tearDown(void);
  protected:
    void test_parse_proc_host_str(void);
};

}
}

#endif
