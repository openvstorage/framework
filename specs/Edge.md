# Edge

## Introduction
The [Edge](https://github.com/openvstorage/home/wiki/Edge) is a lightweight software component which exposes a block device.


## Design choices
- No Edge manager is required
 - This means we can't list all disks connected to certain host without retrieving the whole list
- If possible don't model the edge


## Required changes
- Implement storage and management IP correctly
    - DTL and MDS should use the storage Ip
    - Edge/NFS should use the storage IP
-  Storage Router detail page
    - Change IP to Management IP
    - Add new tab (icon)[http://fontawesome.io/icon/link/]
        - Title: Edge Client Connections
        - Content: table with vdiskname , vpool , ip , type, protocol, Read - Read/Write
    - vDisk detail page
        - Add under Details: Edge Clients: #
        - Add new tab (icon)[http://fontawesome.io/icon/link/]
            - Title: Edge Client Connections
            - Content:  ip , type, protocol, Read - Read/Write
