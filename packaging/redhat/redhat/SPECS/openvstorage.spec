Name: openvstorage
Version:
Release:	1%{?dist}
Summary: openvStorage
License: Apache License, Version 2.0
URL: www.openvstorage.org
Source0: https://github.com/openvstorage/openvstorage/archive/master.zip

BuildArch: amd64
Requires: openvstorage-core = ${binary:Version}, openvstorage-webapps = ${binary:Version}

%description
Open vStorage umbrella package

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