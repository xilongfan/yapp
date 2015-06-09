TESTS=test_yapp_client_idinput
check_PROGRAMS=test_yapp_client_idinput
test_yapp_client_idinput_SOURCES=test_yapp_client_using_range_input.h test_yapp_client_using_range_input.cpp test_yapp_client_driver.cpp
test_yapp_client_idinput_CXXFLAGS=$(AM_CXXFLAGS) -I$(top_srcdir)/src/yapp/client -I$(top_srcdir)/src/yapp/master -I$(top_srcdir)/src/yapp/worker -I$(top_srcdir)/src/yapp/domain
test_yapp_client_idinput_LDADD=-lzookeeper_mt -lcppunit $(top_builddir)/src/yapp/client/libyapp_client.la $(top_builddir)/src/yapp/master/libyapp_master.la $(top_builddir)/src/yapp/worker/libyapp_worker.la $(top_builddir)/src/yapp/domain/libyapp_service.la
