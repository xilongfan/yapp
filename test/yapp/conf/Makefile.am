dist_sysconf_DATA=test_cfg_util_load_cfg.input test_cfg_util_load_cfg.output test_yapp_conf.h update_ra_exception_div_zero.rb  update_ra_exception_nil.rb  update_ra_exception.rb  update_ra_exception_require.rb  update_ra_exception_syntax.rb  update_ra.rb update_ra_by_zip.rb zip.info sample_fencing_script.sh

install-exec-hook:
	rm -rf $(sysconfdir)/test_cfg_util_load_cfg.input
	rm -rf $(sysconfdir)/test_cfg_util_load_cfg.output
	rm -rf $(sysconfdir)/test_yapp_conf.h
	rm -rf $(sysconfdir)/update_ra_exception_div_zero.rb
	rm -rf $(sysconfdir)/update_ra_exception_nil.rb
	rm -rf $(sysconfdir)/update_ra_exception.rb
	rm -rf $(sysconfdir)/update_ra_exception_require.rb
	rm -rf $(sysconfdir)/update_ra_exception_syntax.rb
	rm -rf $(sysconfdir)/update_ra.rb
	rm -rf $(sysconfdir)/update_ra_by_zip.rb
	rm -rf $(sysconfdir)/zip.info
	rm -rf $(sysconfdir)/sample_fencing_script.sh

check-local:
	rm -rf $(srcdir)/../admin/update_ra*std* >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.rb $(top_builddir)/test/yapp/admin/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.info $(top_builddir)/test/yapp/admin/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.input $(srcdir)/*.output $(top_builddir)/test/yapp/admin/ >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/id_input/update_ra*std* >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.rb $(top_builddir)/test/yapp/client/id_input/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.info $(top_builddir)/test/yapp/client/id_input/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.input $(srcdir)/*.output $(top_builddir)/test/yapp/client/id_input/ >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/file_input/update_ra*std* >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.rb $(top_builddir)/test/yapp/client/file_input/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.info $(top_builddir)/test/yapp/client/file_input/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.input $(srcdir)/*.output $(top_builddir)/test/yapp/client/file_input/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.input $(srcdir)/*.output $(top_builddir)/test/yapp/util/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.input $(srcdir)/*.output $(top_builddir)/test/yapp/master/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.input $(srcdir)/*.output $(top_builddir)/test/yapp/domain/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.sh $(top_builddir)/test/yapp/client/id_input/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.sh $(top_builddir)/test/yapp/client/file_input/ >/dev/null 2>&1 || true
	cp -rp $(srcdir)/*.sh $(top_builddir)/test/yapp/admin/ >/dev/null 2>&1 || true

clean-local:
	rm -rf $(srcdir)/../admin/*.rb >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../admin/*.info >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/id_input/*.rb >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/id_input/*.info >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/id_input/update_ra*std* >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/file_input/*.rb >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/file_input/*.info >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/file_input/update_ra*std* >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/id_input/*.sh >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../client/file_input/*.sh >/dev/null 2>&1 || true
	rm -rf $(srcdir)/../admin/*.sh >/dev/null 2>&1 || true

distclean-local:
	rm -rf $(top_builddir)/test/yapp/admin/*.rb >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/admin/*.info >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/admin/update_ra*std* >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/admin/*.input >/dev/null 2>&1  || true
	rm -rf $(top_builddir)/test/yapp/admin/*.output >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/id_input/*.rb >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/id_input/*.info >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/id_input/update_ra*std* >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/id_input/*.input >/dev/null 2>&1  || true
	rm -rf $(top_builddir)/test/yapp/client/id_input/*.output >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/file_input/*.rb >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/file_input/*.info >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/file_input/update_ra*std* >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/file_input/*.input >/dev/null 2>&1  || true
	rm -rf $(top_builddir)/test/yapp/client/file_input/*.output >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/util/*.input >/dev/null 2>&1  || true
	rm -rf $(top_builddir)/test/yapp/util/*.output >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/master/*.input >/dev/null 2>&1  || true
	rm -rf $(top_builddir)/test/yapp/master/*.output >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/domain/*.input >/dev/null 2>&1  || true
	rm -rf $(top_builddir)/test/yapp/domain/*.output >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/id_input/*.sh >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/client/file_input/*.sh >/dev/null 2>&1 || true
	rm -rf $(top_builddir)/test/yapp/admin/*.sh >/dev/null 2>&1 || true
