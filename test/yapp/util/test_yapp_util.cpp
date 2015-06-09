#include <config.h>
#include <unistd.h>
#include <pthread.h>
#include <ctime>

#include "test_yapp_util.h"

using namespace yapp::util;

using std::ifstream;
using std::string;
using std::ios;

CPPUNIT_TEST_SUITE_REGISTRATION(ZkClusterProxyTest);
CPPUNIT_TEST_SUITE_REGISTRATION(ConfigureUtilTest);
CPPUNIT_TEST_SUITE_REGISTRATION(StringUtilTest);

void StringUtilTest::test_parse_full_zk_node_path(void) {
  for (size_t i = 0; i < valid_path_str_arr.size(); i++) {
    string node_name;
    vector<string> dir_arr;
    CPPUNIT_ASSERT_EQUAL(true,
      StringUtil::parse_full_zk_node_path(valid_path_str_arr[i],
                                          dir_arr,
                                          node_name)
    );
    CPPUNIT_ASSERT_EQUAL(node_name, valid_ret_node_name_arr[i]);
    CPPUNIT_ASSERT_EQUAL(valid_ret_dir_arrs[i].size(), dir_arr.size());
    for (size_t c = 0; c < dir_arr.size(); c++) {
      CPPUNIT_ASSERT_EQUAL(valid_ret_dir_arrs[i][c], dir_arr[c]);
    }
  }
  for (size_t i = 0; i < inval_path_str_arr.size(); i++) {
    string node_name;
    vector<string> dir_arr;
    CPPUNIT_ASSERT_EQUAL(false,
      StringUtil::parse_full_zk_node_path(inval_path_str_arr[i],
                                          dir_arr,
                                          node_name)
    );
    CPPUNIT_ASSERT(node_name.empty());
    CPPUNIT_ASSERT(dir_arr.empty());
  }
}

void StringUtilTest::setUp(void) {
  valid_path_str_arr.push_back("/home/xfan/projects/yapp");
  valid_ret_dir_arrs[0].push_back("home");
  valid_ret_dir_arrs[0].push_back("xfan");
  valid_ret_dir_arrs[0].push_back("projects");
  valid_ret_node_name_arr.push_back("yapp");

  valid_path_str_arr.push_back("/home");
  valid_ret_node_name_arr.push_back("home");

  valid_path_str_arr.push_back("/");
  valid_ret_node_name_arr.push_back("");

  const char * inval_cases[] = {
    "/zookeeper", "/home.h", "/home/xfan/pro\\/jects/yapp", "test", ".",
    "..", "./", "../", "./test", "../test", "src//test.h", "/src/test.h",
    "/src/./test", "/src../test", "/src/./test/dasdf", "/src/../test/lsadjf",
  };
  for (unsigned i = 0; i < sizeof(inval_cases)/sizeof(char *); i++) {
    inval_path_str_arr.push_back(string(inval_cases[i]));
  }; 
}

void StringUtilTest::tearDown(void) {

}

void * ZkClusterProxyTest::thread_test_run_master_election(void * zkc_proxy_ptr)
{
#ifdef DEBUG_YAPP_UTIL
  std::cerr << "Thread " << pthread_self() << " Created!" << std::endl;
  std::cerr.flush();
#endif
  CPPUNIT_ASSERT(NULL != zkc_proxy_ptr);
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    ((ZkClusterProxy *)zkc_proxy_ptr)->run_master_election()
  );
  return NULL;
}

void ZkClusterProxyTest::set_test_env(void) {
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str())
  );

  YAPP_MSG_CODE rc = zk_ptr->node_exists(TEST_ROOT);

  CPPUNIT_ASSERT(YAPP_MSG_INVALID != rc);

  if (YAPP_MSG_SUCCESS == rc) {
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS, zk_ptr->delete_node_recur(TEST_ROOT)
    );
  }

  vector<string> path_arr;
  vector<string> data_arr;
  for (size_t i = 0; i < sizeof(TEST_ENV) / sizeof(char *); i++) {
    path_arr.push_back(TEST_ENV[i]);
    data_arr.push_back("");
  }
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS, zk_ptr->batch_create(path_arr, data_arr)
  );
  delete zk_ptr;
}

void ZkClusterProxyTest::setUp(void) {
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
  set_test_env();
}

void ZkClusterProxyTest::tearDown(void) {}

void ZkClusterProxyTest::test_init_zk_conn(void) {
  /**
   * loading the configuration file for testing.
   */
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str())
  );
  delete zk_ptr;
}

void ZkClusterProxyTest::test_create_election_node(void) {
  /**
   * loading the configuration file for testing.
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
   */
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  string path, dat_str;
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS, zk_ptr->create_election_node(path, dat_str)
  );
#ifdef DEBUG_YAPP_UTIL
  std::cerr << path << std::endl;
  std::cerr << dat_str << std::endl;
#endif
  delete zk_ptr;
}

void ZkClusterProxyTest::test_get_node_arr(void) {
  /**
   * loading the configuration file for testing.
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
   */
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  string path;
  for (size_t i = 0; i < sizeof(NEW_NODE_PATH_ENTRY) / sizeof(char *); i++) {
    zk_ptr->del_node(string(NEW_NODE_DIR) + string(NEW_NODE_PATH_ENTRY[i]));
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zk_ptr->create_node(
        string(NEW_NODE_DIR) + string(NEW_NODE_PATH_ENTRY[i]),
        string(NEW_NODE_PATH_ENTRY[i]), path
      )
    );
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "Node " << path << " Created with Value "
              << NEW_NODE_PATH_ENTRY[i] << std::endl;
#endif
  }
  vector<string> node_arr;
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->get_node_arr(string(NEW_NODE_DIR_PATH), node_arr)
  );
  CPPUNIT_ASSERT_EQUAL(
    sizeof(NEW_NODE_PATH_ENTRY) / sizeof(char *), node_arr.size()
  );
  for (size_t i = 0; i < node_arr.size(); i++) {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "Child Node: " << node_arr[i] << std::endl;
#endif
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zk_ptr->del_node(string(NEW_NODE_DIR) + node_arr[i])
    );
  }
  delete zk_ptr;
 
}

void ZkClusterProxyTest::test_get_node_data(void) {
   /**
   * loading the configuration file for testing.
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
   */
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  string path;
  for (size_t i = 0; i < sizeof(NEW_NODE_PATH_ENTRY) / sizeof(char *); i++) {
    zk_ptr->del_node(string(NEW_NODE_DIR) + string(NEW_NODE_PATH_ENTRY[i]));
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zk_ptr->create_node(
        string(NEW_NODE_DIR) + string(NEW_NODE_PATH_ENTRY[i]),
        string(NEW_NODE_PATH_ENTRY[i]), path
      )
    );
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "Node " << path << " Created with Value "
              << NEW_NODE_PATH_ENTRY[i] << std::endl;
#endif
  }
  string node_dat;
  struct Stat node_stat;
  for (size_t i = 0; i < sizeof(NEW_NODE_PATH_ENTRY) / sizeof(char *); i++) {
    memset(&node_stat, 0, sizeof(node_stat));
    zk_ptr->get_node_data(
      string(NEW_NODE_DIR) + string(NEW_NODE_PATH_ENTRY[i]), node_dat,&node_stat
    );
    CPPUNIT_ASSERT_EQUAL(0, node_stat.version);
    CPPUNIT_ASSERT_EQUAL(string(NEW_NODE_PATH_ENTRY[i]), node_dat);
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zk_ptr->del_node(string(NEW_NODE_DIR) + string(NEW_NODE_PATH_ENTRY[i]))
    );
  }
  delete zk_ptr;
}

void ZkClusterProxyTest::test_create_and_del_node(void) {
  /**
   * loading the configuration file for testing.
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
   */
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  string path;
  for (size_t i = 0; i < sizeof(NEW_NODE_PATH_ENTRY) / sizeof(char *); i++) {
    zk_ptr->del_node(string(NEW_NODE_DIR) + string(NEW_NODE_PATH_ENTRY[i]));
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zk_ptr->create_node(
        string(NEW_NODE_DIR) + string(NEW_NODE_PATH_ENTRY[i]),
        string("test"), path
      )
    );
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "Node " << path << " Created with Value "
              << "test" << std::endl;
#endif
  }
  for (size_t i = 0; i < sizeof(NEW_NODE_PATH_ENTRY) / sizeof(char *); i++) {
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zk_ptr->del_node(string(NEW_NODE_DIR) + string(NEW_NODE_PATH_ENTRY[i]))
    );
  }
  delete zk_ptr;
}

void ZkClusterProxyTest::test_run_master_election(void) {
  /**
   * loading the configuration file for testing.
  ConfigureUtil cfg_util;
  string cfg_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(cfg_file));
   */

  /**
   * variables declaration & initialization.
   */
  pthread_t client_thread_arr[NUM_OF_CLIENTS];
  void * client_thread_ret_arr[NUM_OF_CLIENTS] = { NULL };
  ZkClusterProxy * zkc_proxy_ptr_arr[NUM_OF_CLIENTS] = { NULL };
  string conn_str = cfg_util.get_zk_cluster_conn_str();

  memset(client_thread_arr, 0, sizeof(client_thread_arr));
  for (int i = 0; i < NUM_OF_CLIENTS; i++) { 
    zkc_proxy_ptr_arr[i] = new ZkClusterProxy(true);
    CPPUNIT_ASSERT(NULL != zkc_proxy_ptr_arr[i]);
    zkc_proxy_ptr_arr[i]->init_zk_conn(conn_str);
  }

  /**
   * start running testing threads.
   */
  for (int i = 0; i < NUM_OF_CLIENTS; i++) {
    pthread_create(&client_thread_arr[i], NULL,
                   thread_test_run_master_election,
                   zkc_proxy_ptr_arr[i]
    );
  }
  for (int i = 0; i < NUM_OF_CLIENTS; i++) {
    pthread_join(client_thread_arr[i], &client_thread_ret_arr[i]);
    // zkc_proxy_ptr_arr[i]->run_master_election();
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "Thread " << client_thread_arr[i] << " Finished!" << std::endl;
    std::cerr.flush();
#endif
  }
  
  /**
   * Sanity check.
   */
  int master_cnt = 0, worker_cnt = 0;
  string ret_host, ret_port, ret_pcid;

  for (int i = 0; i < NUM_OF_CLIENTS; i++) {
    /** nulled intentionally */
    CPPUNIT_ASSERT(NULL == client_thread_ret_arr[i]);
    CPPUNIT_ASSERT((YAPP_MODE_INVALID !=zkc_proxy_ptr_arr[i]->get_yapp_mode())&&
                   (YAPP_MODE_CLIENT  !=zkc_proxy_ptr_arr[i]->get_yapp_mode())
    );
    CPPUNIT_ASSERT((YAPP_MODE_WORKER == zkc_proxy_ptr_arr[i]->get_yapp_mode())||
                   (YAPP_MODE_MASTER == zkc_proxy_ptr_arr[i]->get_yapp_mode())
    );
    if (YAPP_MODE_MASTER ==zkc_proxy_ptr_arr[i]->get_yapp_mode()){master_cnt++;}
    if (YAPP_MODE_WORKER ==zkc_proxy_ptr_arr[i]->get_yapp_mode()){worker_cnt++;}

    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zkc_proxy_ptr_arr[i]->get_yapp_master_addr(ret_host, ret_port, ret_pcid)
    );
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "##########################################################\n";
    std::cerr << ret_host << ":" << ret_port << ":" << ret_pcid << std::endl;
    std::cerr << "##########################################################\n";
#endif
    CPPUNIT_ASSERT_EQUAL(zkc_proxy_ptr_arr[i]->get_master_host_str(), ret_host);
    CPPUNIT_ASSERT_EQUAL(zkc_proxy_ptr_arr[i]->get_master_port_str(), ret_port);
    CPPUNIT_ASSERT_EQUAL(zkc_proxy_ptr_arr[i]->get_master_proc_pid(), ret_pcid);
  }
  for (int i = 0; i < NUM_OF_CLIENTS; i++) {
    delete zkc_proxy_ptr_arr[i];
  }
  CPPUNIT_ASSERT(1 == master_cnt);
}

void ZkClusterProxyTest::test_get_yapp_master_addr(void) {
  string ret_host, ret_port, ret_pcid;
  ZkClusterProxy * zkc_proxy_ptr = new ZkClusterProxy(true);
  zkc_proxy_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  if (YAPP_MSG_SUCCESS == zkc_proxy_ptr->get_yapp_master_addr(
                            ret_host, ret_port, ret_pcid)
  ) {
#ifdef DEBUG_YAPP_UTIL
    std::cerr << "##########################################################\n";
    std::cerr << ret_host << ":" << ret_port << ":" << ret_pcid << std::endl;
    std::cerr << "##########################################################\n";
#endif
  }
  delete zkc_proxy_ptr;
}


void ZkClusterProxyTest::test_create_and_delete_node_recur(void) {
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  string ret_path;

  for (int c = 8000; c < 8002; c++) {
    string sufx = StringUtil::convert_int_to_str(c);
    for (size_t i = 0; i < sizeof(TEST_PATH_ARR) / sizeof(char *); i++) {
      CPPUNIT_ASSERT_EQUAL(
        YAPP_MSG_SUCCESS,
        zk_ptr->create_node_recur(string(TEST_PATH_ARR[i]) + sufx, string(""), ret_path)
      );
      CPPUNIT_ASSERT_EQUAL(ret_path, string(TEST_PATH_ARR[i]) + sufx);
    }
  }

  string tree_str;
  zk_ptr->print_node_recur(tree_str, TEST_TREE_ROOT);
  std::cerr << tree_str;
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->delete_node_recur(TEST_TREE_ROOT)
  );
  delete zk_ptr;
}

void ZkClusterProxyTest::test_print_node_recur(void) {
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  string ret_path;
  for (size_t i = 0; i < sizeof(TEST_PATH_ARR) / sizeof(char *); i++) {
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zk_ptr->create_node_recur(string(TEST_PATH_ARR[i]), string(""), ret_path)
    );
    CPPUNIT_ASSERT_EQUAL(ret_path, string(TEST_PATH_ARR[i]));
  }
  string tree_str;
  zk_ptr->print_node_recur(tree_str, TEST_TREE_ROOT);
  std::cerr << tree_str;
  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->delete_node_recur(TEST_TREE_ROOT)
  );
  delete zk_ptr;
}

void ZkClusterProxyTest::test_print_queue_stat(void) {
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  string ret_str;
  CPPUNIT_ASSERT_EQUAL(
    zk_ptr->print_queue_stat(ret_str, string("")), YAPP_MSG_SUCCESS
  );
  std::cerr << ">>>>>>> void ZkClusterProxyTest::test_print_queue_stat(void)"
            << std::endl;
  std::cerr << ret_str;
  delete zk_ptr;
}

void ZkClusterProxyTest::test_try_acquire_read_lock(void) {
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  string ret_path;
  string tree_str;

  zk_ptr->delete_node_recur(TEST_TREE_ROOT);

  for (size_t i = 0; i < sizeof(TEST_PATH_ARR) / sizeof(char *); i++) {
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zk_ptr->create_node_recur(string(TEST_PATH_ARR[i]), string(""), ret_path)
    );
    CPPUNIT_ASSERT_EQUAL(ret_path, string(TEST_PATH_ARR[i]));
  }

  string str_hnd[10];
  string str_ex_hnd[10];
  for (int i = 0; i < 10; i++) {
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS, zk_ptr->try_acquire_read_lock(TEST_TREE_ROOT,str_hnd[i])
    );
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_INVALID_FAILED_GRAB_WLOCK,
      zk_ptr->try_acquire_write_lock(TEST_TREE_ROOT, str_ex_hnd[i])
    );
  }

#ifdef DEBUG_YAPP_UTIL
  std::cerr << "Finished Acquiring Locks!" << std::endl;
  zk_ptr->print_node_recur(tree_str, TEST_TREE_ROOT);
  std::cerr << tree_str;
#endif

  for (int i = 0; i < 10; i++) {
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS, zk_ptr->release_lock(str_hnd[i])
    );
  }

#ifdef DEBUG_YAPP_UTIL
  std::cerr << "Finished Releasing Locks!" << std::endl;
  zk_ptr->print_node_recur(tree_str, TEST_TREE_ROOT);
  std::cerr << tree_str;
#endif

  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->delete_node_recur(TEST_TREE_ROOT)
  );
  delete zk_ptr;
}

void ZkClusterProxyTest::test_try_acquire_write_lock(void) {
  ZkClusterProxy * zk_ptr = new ZkClusterProxy(true);
  zk_ptr->init_zk_conn(cfg_util.get_zk_cluster_conn_str());
  string ret_path;

  string tree_str;

  zk_ptr->delete_node_recur(TEST_TREE_ROOT);

  for (size_t i = 0; i < sizeof(TEST_PATH_ARR) / sizeof(char *); i++) {
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_SUCCESS,
      zk_ptr->create_node_recur(string(TEST_PATH_ARR[i]), string(""), ret_path)
    );
    CPPUNIT_ASSERT_EQUAL(ret_path, string(TEST_PATH_ARR[i]));
  }

  string str_hnd[10];
  string str_hnd_sh[10];
  for (int i = 0; i < 10; i++) {
    if (0 == i) {
      CPPUNIT_ASSERT_EQUAL(
        YAPP_MSG_SUCCESS,
        zk_ptr->try_acquire_write_lock(TEST_TREE_ROOT,str_hnd[i])
      );
    } else {
      CPPUNIT_ASSERT_EQUAL(
        YAPP_MSG_INVALID_FAILED_GRAB_WLOCK,
        zk_ptr->try_acquire_write_lock(TEST_TREE_ROOT,str_hnd[i])
      );
    }
    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_INVALID_FAILED_GRAB_RLOCK,
      zk_ptr->try_acquire_read_lock(TEST_TREE_ROOT,str_hnd_sh[i])
    );
  }
#ifdef DEBUG_YAPP_UTIL
  std::cerr << "Finished Acquiring Locks!" << std::endl;
  zk_ptr->print_node_recur(tree_str, TEST_TREE_ROOT);
  std::cerr << tree_str;
#endif

  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS, zk_ptr->release_lock(str_hnd[0])
  );

#ifdef DEBUG_YAPP_UTIL
  std::cerr << "Finished Releasing Locks!" << std::endl;
  zk_ptr->print_node_recur(tree_str, TEST_TREE_ROOT);
  std::cerr << tree_str;
#endif

  CPPUNIT_ASSERT_EQUAL(
    YAPP_MSG_SUCCESS,
    zk_ptr->delete_node_recur(TEST_TREE_ROOT)
  );
  delete zk_ptr;
}

void ZkClusterProxyTest::test_release_lock(void) {

}

void ConfigureUtilTest::setUp(void) { }

void ConfigureUtilTest::tearDown (void){}

void ConfigureUtilTest::test_load_zk_cluster_cfg(void) {
  /**
   * read the whole outcome as binary into the memory.
   */
  string out_file(OUTPUT_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  ifstream fin(out_file.c_str(), ios::in | ios::binary);
  CPPUNIT_ASSERT(true == fin.is_open());
  fin.seekg(0, fin.end);
  int file_size_in_byte = ((int)fin.tellg()) - 1;
  fin.seekg(0, fin.beg);
  char * buffer = new char[file_size_in_byte];
  fin.read(buffer, file_size_in_byte);
  fin.close();

  ConfigureUtil cfg_util;
  string in_file(INPUTS_FILE_ENTRY[TEST_LOAD_ZK_CLUSTER_CFG_FILE_IDX]);
  CPPUNIT_ASSERT(true == cfg_util.load_zk_cluster_cfg(in_file));
  CPPUNIT_ASSERT_EQUAL(0,
    std::memcmp(buffer,
                cfg_util.get_zk_cluster_conn_str().c_str(),
                file_size_in_byte)
  );
  delete[] buffer;
}

void ConfigureUtilTest::test_get_zk_cluster_conn_str(void) {
  ConfigureUtilTest::test_load_zk_cluster_cfg();
}
