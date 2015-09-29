#!/usr/bin/env bash
pip install rpyc
pip install pika
pip install datadiff
pip install celery >= 3.0.19
pip install librabbitmq >= 1.5.2


python /opt/OpenvStorage/scripts/install/openvstorage-core.postinst.py "__NEW_VERSION__" "$@"
