# Download base image ubuntu 16.04
FROM ubuntu:16.04

# Repo can be passed as build argument but defaults to fwk-develop
ARG repo=fwk-develop

# Update Ubuntu Software repository
RUN echo "deb http://apt.openvstorage.com ${repo} main" | tee -a /etc/apt/sources.list \
    && printf 'Package: *\nPin: origin apt.openvstorage.com\nPin-Priority: 1000\n\nPackage: *\nPin: origin apt.openvstorage.com\nPin-Priority: 1000\n' > /etc/apt/preferences \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get update \
    && apt-get install -y --force-yes \
        rsyslog \
        sudo \
        openssl \
    && apt-get install -y --force-yes \
        openvstorage \
        openvstorage-extensions

# Prepare to accept new code
RUN rm -rf /opt/OpenvStorage \
    && chmod 777 /opt \
    && chmod -R 777 /var/log/ovs \
    && chmod 777 /run

# Move over the Travis cloned code base
ADD . /opt/OpenvStorage

# Further tweaks to run our tests
RUN cp /opt/OpenvStorage/scripts/system/ovs /usr/bin/ \
    && chmod 777 /usr/bin/ovs \
    && export PYTHONPATH=$PYTHONPATH:/usr/local/lib/python2.7/dist-packages:/usr/lib/python2.7/dist-packages \
    && cd /opt/OpenvStorage/webapps/api; export PYTHONPATH=/opt/OpenvStorage:$PYTHONPATH; python manage.py syncdb --noinput \
    && echo '{"configuration_store":"arakoon"}' > /opt/OpenvStorage/config/framework.json
