## ETCD

All framework keys stated below are relative to ```/ovs/framework```. E.g. ```/cluster_id``` will be ```/ovs/framework/cluster_id```.

```json
/cluster_id = "$cluster_id"
/external_etcd = "$external_etcd"
/registered = True|False
/memcache = {"endpoints": [$endpoint_1, $endpoint2]},
/messagequeue = {"endpoints": [$endpoint_3, $endpoint_4],
                 "protocol": "amqp",
                 "user": "ovs",
                 "port": 5672,
                 "password": "$unencrypted_password",
                 "queues": {"storagedriver": "volumerouter"}},
/plugins/installed = {"backends": [$plugin_a],
                      "generic": [$plugin_a, $plugin_b]},
/versions = {"ovs": 4,
             $plugin_a: $plugin_a_version},
/stores = {"persistent": "pyrakoon",
           "volatile": "memcache"},
/paths = {"cfgdir": "/opt/OpenvStorage/config",
          "basedir": "/opt/OpenvStorage",
          "ovsdb": "/opt/OpenvStorage/db"},
/support = {"enablesupport": True|False,
            "enabled": True|False,
            "interval": 60},
/storagedriver = {"mds_safety": 2,
                  "mds_tlogs": 100,
                  "mds_maxload": 75},
/webapps = {"html_endpoint": "/",
            "oauth2": {"mode": "local|remote",
                       "authorize_uri": "$autorize_url_for_remote",
                       "client_id": "$client_id_for_remote",
                       "client_secret": "$client_secret_for_remote",
                       "scope": "$scope_for_remote",
                       "token_uri": "$token_uri_for_remote"}}
/hosts/$host_id/storagedriver = {"rsp": "/var/rsp",
                                 "vmware_mode": "ganesha"},
/hosts/$host_id/ports = {"storagedriver": [[26200, 26299]],
                         "mds": [[26300, 26399]],
                         "arakoon": [26400]},
/hosts/$host_id/setupcompleted = True|False,
/hosts/$host_id/type = "MASTER|EXTRA|UNCONFIGURED"
```

The ```/ovs/framework/webapps/oauth2``` mode can be either ```local``` or ```remote```. When using ```remote```, certain extra keys on how to reach the remote authentication platform should be given.

* authorize_uri: If a user is not logged in, he will be redirected to this page for authenitcation
* client_id: OVS client identification towards remote oauth2 platform
* client_secret: OVS client password for authenitcation to remote oauth2 platform
* scope: Requested scope for OVS users
* token_uri: URI where OVS can request the token
