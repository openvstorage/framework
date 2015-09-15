Name: openvstorage-core
Version:
Release:	1%{?dist}
Summary: openvStorage core
License: Apache License, Version 2.0
URL: www.openvstorage.org
Source0: https://github.com/openvstorage/openvstorage/archive/master.zip

BuildArch: amd64
Requires: python >= 2.7.2, python-pip >= 1.4.1-2, rabbitmq-server >= 3.2.4, python-memcache >= 1.47-2,
 memcached >= 1.4.7, volumedriver-server >= 3.6.0, arakoon >= 1.8, alba >=0.5, lsscsi >= 0.27-2,
 libvirt0 >= 1.1.1, python-libvirt >= 1.1.1, python-dev >= 2.7.5, python-pyinotify, sudo,
 libev4 >= 1:4.11-1, python-boto, nfs-kernel-server, python-suds-jurko, python-datadiff,
 ipython, gcc, devscripts, openssh-server, python-paramiko, python-rpyc, python-librabbitmq >= 1.5.2,
 python-pysnmp4, python-kombu >= 3.0.7, python-celery >= 3.1.6, python-pika,
 python-six, python-protobuf, python-pyudev, sshpass, avahi-utils >= 0.6.31, openvpn, ntp,
 logstash >= 1.4.0-1-c82dc09, virtinst >= 0.600.4

%description
Core components for the Open vStorage product

%prep
%autosetup

%build
%configure
make %{?_smp_mflags}

%install
%make_install

%files
%doc

%changelog