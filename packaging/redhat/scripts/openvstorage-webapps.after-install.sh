#!/usr/bin/env bash
pip install djangorestframework >= 2.3.9

python /opt/OpenvStorage/scripts/install/openvstorage-webapps.postinst.py "__NEW_VERSION__" "$@"
