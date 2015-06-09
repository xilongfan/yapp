TESTS=test_yapp_master
check_PROGRAMS=test_yapp_master
test_yapp_master_SOURCES=test_yapp_master.h test_yapp_master.cpp test_yapp_master_driver.cpp
test_yapp_master_CXXFLAGS=$(AM_CXXFLAGS) -I$(top_srcdir)/src/yapp/master
test_yapp_master_LDADD=-lzookeeper_mt -lcppunit $(top_builddir)/src/yapp/master/libyapp_master.la
