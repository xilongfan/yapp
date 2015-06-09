#include <config.h>
#include <unistd.h>
#include <pthread.h>

#include "test_yapp_master.h"

using namespace yapp::master;

using std::ifstream;
using std::string;
using std::ios;

CPPUNIT_TEST_SUITE_REGISTRATION(YappMasterTest);

void YappMasterTest::setUp(void){
  int test_case_cnt = sizeof(YAPP_MASTER_PARAM_ARR_SET) / sizeof(char **);
  yapp_params_list_arr_ptr = new vector<string>[test_case_cnt];
  wrong_yapp_params_list_arr_ptr_0 = new vector<string>[test_case_cnt];
  wrong_yapp_params_list_arr_ptr_1 = new vector<string>[test_case_cnt];
  wrong_yapp_params_list_arr_ptr_2 = new vector<string>[test_case_cnt];
  for (int i = 0; i < test_case_cnt; i++) {
    for (int c = 0; c < YAPP_MASTER_PARAM_ARR_SIZE_SET[i]; c++) {
      yapp_params_list_arr_ptr[i].push_back(YAPP_MASTER_PARAM_ARR_SET[i][c]);
    }
    yapp_master_arr.push_back(
      YappMaster(yapp_params_list_arr_ptr[i], true, true)
    );
  }
  for (int i = 0; i < test_case_cnt; i++) {
    for (int c = 1; c < YAPP_MASTER_PARAM_ARR_SIZE_SET[i]; c++) {
      wrong_yapp_params_list_arr_ptr_0[i].push_back(
        YAPP_MASTER_PARAM_ARR_SET[i][c]
      );
    }
    wrong_yapp_params_list_arr_ptr_0[i].push_back("LJSLKD JFLUSDG JUCKDFP21-");
    wrong_yapp_master_arr_0.push_back(
      YappMaster(wrong_yapp_params_list_arr_ptr_0[i], true, true)
    );
  }
  for (int i = 0; i < test_case_cnt; i++) {
    for (int c = 0; c < YAPP_MASTER_PARAM_ARR_SIZE_SET[i]; c++) {
      wrong_yapp_params_list_arr_ptr_1[i].push_back(
        YAPP_MASTER_PARAM_ARR_SET[i][c]
      );
    }
    wrong_yapp_params_list_arr_ptr_1[i].push_back("LJSLKD JFLUSDG JUCKDFP21-");
    wrong_yapp_master_arr_1.push_back(
      YappMaster(wrong_yapp_params_list_arr_ptr_1[i], true, true)
    );
  }
  for (int i = 0; i < test_case_cnt; i++) {
    for (int c = 1; c < YAPP_MASTER_PARAM_ARR_SIZE_SET[i]; c++) {
      wrong_yapp_params_list_arr_ptr_2[i].push_back(
        YAPP_MASTER_PARAM_ARR_SET[i][c]
      );
    }
    wrong_yapp_master_arr_2.push_back(
      YappMaster(wrong_yapp_params_list_arr_ptr_2[i], true, true)
    );
  }
}

void YappMasterTest::tearDown(void){
  delete[] yapp_params_list_arr_ptr;
  delete[] wrong_yapp_params_list_arr_ptr_0;
  delete[] wrong_yapp_params_list_arr_ptr_1;
  delete[] wrong_yapp_params_list_arr_ptr_2;
}

void YappMasterTest::test_parse_arguments(void){
  for (size_t i = 0; i < yapp_master_arr.size(); i++) {

#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "Arguments List ";
    for (int c = 0; c < YAPP_MASTER_PARAM_ARR_SIZE_SET[i]; c++) {
      std::cerr << yapp_params_list_arr_ptr[i][c] << " ";
    }
    std::cerr << std::endl;
#endif

    CPPUNIT_ASSERT_EQUAL(YAPP_MSG_SUCCESS,yapp_master_arr[i].parse_arguments());
  }

  for (size_t i = 0; i < yapp_master_arr.size(); i++) {
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "Arguments List";
    for (size_t c = 0; c < wrong_yapp_params_list_arr_ptr_0[i].size(); c++) {
      std::cerr << wrong_yapp_params_list_arr_ptr_0[i][c] << " ";
    }
    std::cerr << std::endl;
#endif

    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_INVALID_ARGS, wrong_yapp_master_arr_0[i].parse_arguments()
    );
  }

  for (size_t i = 0; i < yapp_master_arr.size(); i++) {
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "Arguments List";
    for (size_t c = 0; c < wrong_yapp_params_list_arr_ptr_1[i].size(); c++) {
      std::cerr << wrong_yapp_params_list_arr_ptr_1[i][c] << " ";
    }
    std::cerr << std::endl;
#endif

    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_INVALID_ARGS, wrong_yapp_master_arr_1[i].parse_arguments()
    );
  }

  for (size_t i = 0; i < yapp_master_arr.size(); i++) {
#ifdef DEBUG_YAPP_CLIENT
    std::cerr << "Arguments List";
    for (size_t c = 0; c < wrong_yapp_params_list_arr_ptr_2[i].size(); c++) {
      std::cerr << wrong_yapp_params_list_arr_ptr_2[i][c] << " ";
    }
    std::cerr << std::endl;
#endif

    CPPUNIT_ASSERT_EQUAL(
      YAPP_MSG_INVALID_ARGS, wrong_yapp_master_arr_2[i].parse_arguments()
    );
  }
}
