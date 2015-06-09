TESTS=test_yapp_util
check_PROGRAMS=test_yapp_util
test_yapp_util_SOURCES=test_yapp_util.h test_yapp_util.cpp test_yapp_util_driver.cpp
test_yapp_util_CXXFLAGS=$(AM_CXXFLAGS) -I$(top_srcdir)/src/yapp/util
test_yapp_util_LDADD=-lzookeeper_mt -lcppunit $(top_builddir)/src/yapp/util/libyapp_util.la
