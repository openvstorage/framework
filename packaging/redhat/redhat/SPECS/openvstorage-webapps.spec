Name: openvstorage-webapps
Version:
Release:	1%{?dist}
Summary: openvStorage Web Applications
License: Apache License, Version 2.0
URL: www.openvstorage.org
Source0: https://github.com/openvstorage/openvstorage/archive/master.zip

BuildArch: amd64
Requires: openvstorage-core = ${binary:Version}, python-django >= 1.5.1-2, nginx >= 1.2.6,
 python-djangorestframework >= 2.3.9, gunicorn >= 0.15.0-1, python-gevent >= 0.13.0-1build2, python-markdown >= 2.3.1-1

%description
Web components for the Open vStorage product

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