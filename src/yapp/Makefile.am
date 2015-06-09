SUBDIRS=base util domain worker client master admin
bin_PROGRAMS=yapp ypadmin yappd
yapp_SOURCES=yapp.cpp yapp.h
yapp_LDADD=./domain/libyapp_service.la ./base/libyapp_base.la ./util/libyapp_util.la ./client/libyapp_client.la -lzookeeper_mt -lthrift
ypadmin_SOURCES=ypadmin.cpp ypadmin.h
ypadmin_LDADD=./domain/libyapp_service.la ./base/libyapp_base.la ./util/libyapp_util.la ./admin/libyapp_admin.la -lzookeeper_mt -lthrift
yappd_SOURCES=yappd.cpp yappd.h
yappd_LDADD=./domain/libyapp_service.la ./base/libyapp_base.la ./domain/libyapp_service.la ./util/libyapp_util.la ./worker/libyapp_worker.la ./master/libyapp_master.la -lzookeeper_mt -lthrift
