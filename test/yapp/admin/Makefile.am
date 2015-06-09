TESTS=test_yapp_admin
check_PROGRAMS=test_yapp_admin
test_yapp_admin_SOURCES=test_yapp_admin.h test_yapp_admin.cpp test_yapp_admin_driver.cpp
test_yapp_admin_CXXFLAGS=$(AM_CXXFLAGS) -I$(top_srcdir)/src/yapp/admin -I$(top_srcdir)/src/yapp/client -I$(top_srcdir)/src/yapp/master -I$(top_srcdir)/src/yapp/worker -I$(top_srcdir)/src/yapp/domain
test_yapp_admin_LDADD=-lzookeeper_mt -lcppunit $(top_builddir)/src/yapp/admin/libyapp_admin.la $(top_builddir)/src/yapp/client/libyapp_client.la $(top_builddir)/src/yapp/master/libyapp_master.la $(top_builddir)/src/yapp/worker/libyapp_worker.la $(top_builddir)/src/yapp/domain/libyapp_service.la
#cat ./*.stdout.* | grep zip: | sed s/'^line.*zip: '//g | sort | uniq > ./a.cmp
#cat ./zip.info | sort | uniq > ./a.ans
#diff ./a.cmp ./a.ans
#rm ./a.cmp ./a.ans
