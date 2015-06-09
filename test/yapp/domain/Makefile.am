TESTS=test_yapp_domain
check_PROGRAMS=test_yapp_domain
test_yapp_domain_SOURCES=test_yapp_master_service_handler.h test_yapp_master_service_handler.cpp test_yapp_service_util.h test_yapp_service_util.cpp test_yapp_domain_driver.cpp
test_yapp_domain_CXXFLAGS=$(AM_CXXFLAGS) -I$(top_srcdir)/src/yapp/domain -I$(top_srcdir)/src/yapp/util
test_yapp_domain_LDADD=-lcppunit $(top_builddir)/src/yapp/domain/libyapp_service.la $(top_builddir)/src/yapp/util/libyapp_util.la
