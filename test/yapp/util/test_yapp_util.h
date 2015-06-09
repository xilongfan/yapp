#ifndef TEST_YAPP_UTIL_H_
#define TEST_YAPP_UTIL_H_

#include <string>
#include <cstring>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include "yapp_util.h"
#include "../conf/test_yapp_conf.h"

namespace yapp {
namespace util {

using namespace yapp::conf;

const static string TEST_TREE_ROOT = "/test/yapp";
const static char * TEST_PATH_ARR[] = {
  "/test/yapp/001/001",
  "/test/yapp/001/002",
  "/test/yapp/001/003",

  "/test/yapp/002/001",
  "/test/yapp/002/002/001/001/001",
  "/test/yapp/002/002/001/002",
  "/test/yapp/002/002/002",
  "/test/yapp/002/002/003",
  "/test/yapp/002/002/004",
  "/test/yapp/002/002/004/001",
  "/test/yapp/002/002/004/001/001",
  "/test/yapp/002/002/004/001/001/001",
  "/test/yapp/002/002/004/001/001/001/001",
  "/test/yapp/002/002/004/001/001/001/001/001",

  "/test/yapp/003/001",
  "/test/yapp/003/002",
  "/test/yapp/003/003",

  "/test/yapp/004/001",
  "/test/yapp/004/002",
};

class ZkClusterProxyTest : public CPPUNIT_NS::TestFixture {
  CPPUNIT_TEST_SUITE(ZkClusterProxyTest);

  CPPUNIT_TEST(test_init_zk_conn);

  CPPUNIT_TEST(test_create_and_del_node);
  CPPUNIT_TEST(test_create_election_node);
  CPPUNIT_TEST(test_get_node_data);
  CPPUNIT_TEST(test_get_node_arr);
  CPPUNIT_TEST(test_create_and_delete_node_recur);
  CPPUNIT_TEST(test_print_node_recur);
  CPPUNIT_TEST(test_run_master_election);
  CPPUNIT_TEST(test_get_yapp_master_addr);
  CPPUNIT_TEST(test_print_queue_stat);

  // CPPUNIT_TEST(test_try_acquire_read_lock);
  CPPUNIT_TEST(test_try_acquire_write_lock);
  CPPUNIT_TEST(test_release_lock);

  CPPUNIT_TEST_SUITE_END ();
  public:
    void setUp (void);
    void tearDown (void);
  protected:
    static void * thread_test_run_master_election(void * zkc_proxy_ptr);

    void test_init_zk_conn(void);
    void test_create_and_del_node(void);
    void test_create_election_node(void);
    void test_get_node_data(void);
    void test_get_node_arr(void);
    void test_run_master_election(void);
    void test_get_yapp_master_addr(void);
    void test_create_and_delete_node_recur(void);
    void test_print_node_recur(void);
    void test_print_queue_stat(void);

    void test_try_acquire_read_lock(void);
    void test_try_acquire_write_lock(void);
    void test_release_lock(void);

  private:
    void set_test_env(void);
    ConfigureUtil cfg_util;
};

class ConfigureUtilTest : public CPPUNIT_NS::TestFixture
{
  CPPUNIT_TEST_SUITE(ConfigureUtilTest);
  CPPUNIT_TEST(test_load_zk_cluster_cfg);
  CPPUNIT_TEST(test_get_zk_cluster_conn_str);
  CPPUNIT_TEST_SUITE_END();
  public:
    void setUp(void);
    void tearDown(void);
  protected:
    void test_load_zk_cluster_cfg(void);
    void test_get_zk_cluster_conn_str(void);
};

class StringUtilTest : public CPPUNIT_NS::TestFixture
{
  CPPUNIT_TEST_SUITE(StringUtilTest);
  CPPUNIT_TEST(test_parse_full_zk_node_path);
  CPPUNIT_TEST_SUITE_END();
  public:
    void setUp(void);
    void tearDown(void);
    void test_parse_full_zk_node_path(void);
  private:
    vector<string> valid_ret_dir_arrs[3];
    vector<string> valid_ret_node_name_arr;
    vector<string> valid_path_str_arr;
    vector<string> inval_path_str_arr;
};

}
}

#endif
