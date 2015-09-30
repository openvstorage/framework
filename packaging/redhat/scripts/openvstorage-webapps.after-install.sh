#!/usr/bin/env bash
pip install djangorestframework>=2.3.12

mkdir -p /etc/nginx/sites-enabled

python /opt/OpenvStorage/scripts/install/openvstorage-webapps.postinst.py "$Version" "$@"

cp /etc/nginx/sites-enabled/* /etc/nginx/conf.d/
chmod 777 /etc/nginx/conf.d/*
service nginx restart
