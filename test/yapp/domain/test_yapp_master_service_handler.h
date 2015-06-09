#ifndef TEST_YAPP_MASTER_SERVICE_HANDLER_H_
#define TEST_YAPP_MASTER_SERVICE_HANDLER_H_

#include <string>
#include <cstring>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include "yapp_master_service_handler.h"
#include "yapp_domain_factory.h"

namespace yapp {
namespace domain {

class YappMasterServiceHandlerTest : public CPPUNIT_NS::TestFixture {
  CPPUNIT_TEST_SUITE(YappMasterServiceHandlerTest );
  CPPUNIT_TEST(test_split_task);
  CPPUNIT_TEST(test_initiate_new_job);
  CPPUNIT_TEST_SUITE_END ();
  public:
    void setUp (void);
    void tearDown (void);
  protected:
    void test_split_task(void);
    void test_initiate_new_job(void);
  private:
    string out_pfx;
    string err_pfx;
    YappMasterServiceHandler * master_srv_hnd;
    ZkClusterProxy * zkc_proxy_ptr;
};

}
}

#endif
