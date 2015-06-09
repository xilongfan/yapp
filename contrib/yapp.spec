Name:          yapp
Group:         Development
Summary:       YAPP(Yet Another Parallel Processing), A Data Processing Automation Framework
License:       GPLv3
Version:       0.9.6
Release:       0
URL:           https://github.com/Spokeo/yapp
Packager:      Xilong Fan, <xfan@spokeo.com>
Vendor:        Spokeo, Inc
Source0:       %{name}-%{version}.tar.gz

BuildRequires: libtool >= 2.2
BuildRequires: thrift >= 0.9.0
BuildRequires: thrift-lib-cpp-devel >= 0.9.0
BuildRequires: zookeeper-lib >= 3.4.5

Requires:      thrift-lib-cpp >= 0.9.0
Requires:      zookeeper-lib >= 3.4.5

BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
Basically, what we need would be a fault-tolerant, automated batching system to
manage all backend processes across different script server. It supports auto
splitting to fully utilize the hardware, process migration among different nodes
for automatic failover and dynamic load balancing.


%files
%defattr(-,yapp,yapp)
%{_bindir}/*
%config(noreplace) %{_sysconfdir}/yapp.cfg
%{_initrddir}/yappd
%{_localstatedir}/log/yappd
%{_localstatedir}/run/yappd
%{_localstatedir}/tmp/yappd

%attr(755, yapp, yapp) %{_initrddir}/yappd
%attr(755, yapp, yapp) %{_localstatedir}/log/yappd
%attr(755, yapp, yapp) %{_localstatedir}/run/yappd
%attr(755, yapp, yapp) %{_localstatedir}/tmp/yappd

%clean
rm -rf %{buildroot}

%preun
if [ "$1" = "0" ] || [ "$1" = "1" ]; then
  /sbin/service yappd stop
fi
exit 0

%prep
%setup -q -n %{name}-%{version}

%build
%configure
make

%check

%makeinstall
mkdir -p ${RPM_BUILD_ROOT}%{_initrddir}
mkdir -p ${RPM_BUILD_ROOT}%{_localstatedir}/log/yappd
mkdir -p ${RPM_BUILD_ROOT}%{_localstatedir}/run/yappd
mkdir -p ${RPM_BUILD_ROOT}%{_localstatedir}/tmp/yappd
mv ${RPM_BUILD_ROOT}%{_sysconfdir}/yappd ${RPM_BUILD_ROOT}%{_initrddir}/yappd

%changelog
* Fri Mar 13 2015 Xilong Fan <xfan@spokeo.com> 0.9.6
- Bug fix version for previous 0.9.5
