## ETCD

#### Framework
All framework keys stated below are relative to ```/ovs/framework```. E.g. ```cluster_id``` will be ```/ovs/framework/cluster_id```.

##### Clusterwide key-values
```
arakoon_clusters = {"ovsdb": "$framework_arakoon_cluster_name",
                    "voldrv": "$storagedriver_arakoon_cluster_name"}
cluster_id = "$cluster_id"
external_etcd = "$external_etcd"
install_time = "$epoch"
memcache = {"endpoints": ["$endpoint_1", "$endpoint2"],
            "metadata": {"internal": True|False}}
messagequeue = {"endpoints": ["$endpoint_3", "$endpoint_4"],
                "protocol": "amqp",
                "user": "ovs",
                "password": "$unencrypted_password",
                "queues": {"storagedriver": "volumerouter"},
                "metadata": {"internal": True|False}}
paths = {"cfgdir": "/opt/OpenvStorage/config",
         "basedir": "/opt/OpenvStorage",
         "ovsdb": "/opt/OpenvStorage/db"}
plugins/installed = {"backends": ["$plugin_a"],
                     "generic": ["$plugin_b", "$plugin_c"]},
plugins/$plugin_a/config = {"nsm": {"safety": 3,
                                    "maxload": 75}}
storagedriver = {"mds_safety": 2,
                 "mds_tlogs": 100,
                 "mds_maxload": 75}
stores = {"persistent": "pyrakoon",
          "volatile": "memcache"}
support = {"enablesupport": True|False,
           "enabled": True|False,
           "interval": 60}
webapps = {"html_endpoint": "/",
           "oauth2": {"mode": "local|remote",
                      "authorize_uri": "$autorize_url_for_remote",
                      "client_id": "$client_id_for_remote",
                      "client_secret": "$client_secret_for_remote",
                      "scope": "$scope_for_remote",
                      "token_uri": "$token_uri_for_remote"}}
```
The ```/ovs/framework/webapps/oauth2``` mode can be either ```local``` or ```remote```. When using ```remote```, certain extra keys on how to reach the remote authentication platform should be given.

* authorize_uri: If a user is not logged in, he will be redirected to this page for authenitcation
* client_id: OVS client identification towards remote oauth2 platform
* client_secret: OVS client password for authenitcation to remote oauth2 platform
* scope: Requested scope for OVS users
* token_uri: URI where OVS can request the token


##### Cluster node specific key-values

```
hosts/$host_id/ip = "$host_ip"
hosts/$host_id/paths = {"celery": "/usr/bin/celery"}
hosts/$host_id/ports = {"storagedriver": [[26200, 26299]],
                        "mds": [[26300, 26399]],
                        "arakoon": [26400]}
hosts/$host_id/promotecompleted = True|False
hosts/$host_id/setupcompleted = True|False
hosts/$host_id/storagedriver = {"rsp": "/var/rsp",
                                "vmware_mode": "ganesha"}
hosts/$host_id/type = "MASTER|EXTRA|UNCONFIGURED"
hosts/$host_id/versions = {"ovs": 4,
                           "$plugin_a": $plugin_a_version}
```

The ```$host_id``` in above keys can be found on the respective node in ```/etc/openvstorage_id```

#### Arakoon
All arakoon keys stated below are relative to ```/ovs/arakoon```. E.g. ```ovsdb``` will be ```/ovs/arakoon/ovsdb```.

##### FWK
```
$clustername/metadata = {"internal": True|False,
                         "type": "FWK",
                         "in_use": True|False}
$clustername/config = "[global]
                       cluster = 1KHMxMRgfwFxzD1C,E0nLvrGxEVKMwL8O,duwg544y8C7LP0WY
                       cluster_id = ovsdb
                       tlog_max_entries = 5000
                       plugins =

                       [1KHMxMRgfwFxzD1C]
                       tlog_compression = snappy
                       client_port = 26400
                       messaging_port = 26401
                       name = 1KHMxMRgfwFxzD1C
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/ovsdb/db
                       ip = $ip1
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/ovsdb/tlogs
                       log_dir = /var/log/arakoon/ovsdb

                       [duwg544y8C7LP0WY]
                       tlog_compression = snappy
                       client_port = 26400
                       messaging_port = 26401
                       name = duwg544y8C7LP0WY
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/ovsdb/db
                       ip = $ip2
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/ovsdb/tlogs
                       log_dir = /var/log/arakoon/ovsdb

                       [E0nLvrGxEVKMwL8O]
                       tlog_compression = snappy
                       client_port = 26400
                       messaging_port = 26401
                       name = E0nLvrGxEVKMwL8O
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/ovsdb/db
                       ip = $ip3
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/ovsdb/tlogs
                       log_dir = /var/log/arakoon/ovsdb"
```

##### SD
```
$clustername/metadata = {"internal": True|False,
                         "type": "SD",
                         "in_use": True|False}
$clustername/config = "[global]
                       cluster = 1KHMxMRgfwFxzD1C,E0nLvrGxEVKMwL8O,duwg544y8C7LP0WY
                       cluster_id = voldrv
                       tlog_max_entries = 5000
                       plugins =

                       [1KHMxMRgfwFxzD1C]
                       tlog_compression = snappy
                       client_port = 26402
                       messaging_port = 26403
                       name = 1KHMxMRgfwFxzD1C
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/voldrv/db
                       ip = $ip1
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/voldrv/tlogs
                       log_dir = /var/log/arakoon/voldrv

                       [duwg544y8C7LP0WY]
                       tlog_compression = snappy
                       client_port = 26402
                       messaging_port = 26403
                       name = duwg544y8C7LP0WY
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/voldrv/db
                       ip = $ip2
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/voldrv/tlogs
                       log_dir = /var/log/arakoon/voldrv

                       [E0nLvrGxEVKMwL8O]
                       tlog_compression = snappy
                       client_port = 26402
                       messaging_port = 26403
                       name = E0nLvrGxEVKMwL8O
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/voldrv/db
                       ip = $ip3
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/voldrv/tlogs
                       log_dir = /var/log/arakoon/voldrv"
```

##### ABM

These type of arakoon clusters can be defined multiple times
```
$clustername/metadata = {"internal": True|False,
                         "type": "ABM",
                         "in_use": True|False}
$clustername/config = "[global]
                       cluster = 1KHMxMRgfwFxzD1C,E0nLvrGxEVKMwL8O,duwg544y8C7LP0WY
                       cluster_id = abm_1
                       tlog_max_entries = 5000
                       plugins =

                       [1KHMxMRgfwFxzD1C]
                       tlog_compression = snappy
                       client_port = 26404
                       messaging_port = 26405
                       name = 1KHMxMRgfwFxzD1C
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/abm_1/db
                       ip = $ip1
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/abm_1/tlogs
                       log_dir = /var/log/arakoon/abm_1

                       [duwg544y8C7LP0WY]
                       tlog_compression = snappy
                       client_port = 26404
                       messaging_port = 26405
                       name = duwg544y8C7LP0WY
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/abm_1/db
                       ip = $ip2
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/abm_1/tlogs
                       log_dir = /var/log/arakoon/abm_1

                       [E0nLvrGxEVKMwL8O]
                       tlog_compression = snappy
                       client_port = 26404
                       messaging_port = 26405
                       name = E0nLvrGxEVKMwL8O
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/abm_1/db
                       ip = $ip3
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/abm_1/tlogs
                       log_dir = /var/log/arakoon/abm_1"
```

##### NSM

These type of arakoon clusters can be defined multiple times per ABM arakoon cluster
```
$clustername/metadata = {"internal": True|False,
                         "type": "NSM",
                         "in_use": True|False}
$clustername/config = "[global]
                       cluster = 1KHMxMRgfwFxzD1C,E0nLvrGxEVKMwL8O,duwg544y8C7LP0WY
                       cluster_id = nsm_0
                       tlog_max_entries = 5000
                       plugins =

                       [1KHMxMRgfwFxzD1C]
                       tlog_compression = snappy
                       client_port = 26406
                       messaging_port = 26407
                       name = 1KHMxMRgfwFxzD1C
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/nsm_0/db
                       ip = $ip1
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/nsm_0/tlogs
                       log_dir = /var/log/arakoon/nsm_0

                       [duwg544y8C7LP0WY]
                       tlog_compression = snappy
                       client_port = 26406
                       messaging_port = 26407
                       name = duwg544y8C7LP0WY
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/nsm_0/db
                       ip = $ip2
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/nsm_0/tlogs
                       log_dir = /var/log/arakoon/nsm_0

                       [E0nLvrGxEVKMwL8O]
                       tlog_compression = snappy
                       client_port = 26406
                       messaging_port = 26407
                       name = E0nLvrGxEVKMwL8O
                       fsync = true
                       home = /opt/OpenvStorage/db/arakoon/nsm_0/db
                       ip = $ip3
                       log_level = info
                       tlog_dir = /opt/OpenvStorage/db/arakoon/nsm_0/tlogs
                       log_dir = /var/log/arakoon/nsm_0"
```
