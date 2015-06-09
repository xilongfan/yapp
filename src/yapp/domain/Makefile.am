.NOTPARALLEL:
noinst_LTLIBRARIES=libyapp_service_gen.la libyapp_service.la

nodist_libyapp_service_gen_la_SOURCES=\
	gen-cpp/YappMasterService.cpp \
	gen-cpp/YappMasterService.h \
	gen-cpp/YappWorkerService.cpp \
	gen-cpp/YappWorkerService.h \
	gen-cpp/YappService.cpp \
	gen-cpp/YappService.h \
	gen-cpp/yapp_service_types.cpp \
	gen-cpp/yapp_service_types.h \
	gen-cpp/yapp_service_constants.cpp \
	gen-cpp/yapp_service_constants.h

libyapp_service_gen_la_LIBADD=\
	-lthrift \
	../util/libyapp_util.la

libyapp_service_la_LIBADD=\
	../util/libyapp_util.la \
	libyapp_service_gen.la \
	-lthrift

libyapp_service_la_SOURCES=\
	yapp_service_handler.h \
	yapp_service_handler.cpp \
	yapp_service_util.h \
	yapp_service_util.cpp \
	yapp_master_service_handler.h \
	yapp_master_service_handler.cpp \
	yapp_worker_service_handler.h \
	yapp_worker_service_handler.cpp \
	yapp_domain_factory.cpp \
	yapp_domain_factory.h

#
# Common thrift code generation rules
#
THRIFT=thrift

gen-cpp gen-cpp/YappMasterService.cpp gen-cpp/YappWorkerService.cpp gen-cpp/YappService.cpp gen-cpp/yapp_service_types.cpp : $(top_srcdir)/thrift/yapp_service.thrift
	$(THRIFT) --gen cpp -r $<

INCLUDES=\
	-Igen-cpp

clean-local:
	$(RM) -r gen-cpp

EXTRA_DIST=\
	gen-cpp \
	yapp_service_util.h \
	yapp_service_util.cpp \
	yapp_service_handler.h \
	yapp_service_handler.cpp \
	yapp_master_service_handler.h \
	yapp_master_service_handler.cpp \
	yapp_worker_service_handler.h \
	yapp_worker_service_handler.cpp \
	yapp_domain_factory.h \
	yapp_domain_factory.cpp
