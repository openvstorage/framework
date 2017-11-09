### The support agent
The support agent is a small process (ovs-support-agent) which sends heartbeat data towards the OpenvStorage monitoring server.
It is also capable of processing certain tasks so that the OpenvStorage team can easily provide support for this cluster.

### Configuring the agent
Tweaking the agent is done using the GUI:
![Alt text](https://user-images.githubusercontent.com/17570109/32617221-b056ff26-c574-11e7-8359-bc1a4eec32ce.png "Configuring support agent")

All aspects about the support agent can be tweaked:
 - Heartbeat can be enabled/disabled (Enables or disables the agent as a whole)
 - Task processing can be enabled/disabled:
   - Task: remote access: the monitoring agent opens a tunnel towards the OpenvStorage monitoring server upon request
   - Task: upload log files: the monitoring agent uploads log files on the OpenvStorage monitoring ftp server

### The agent during failures
The support agent is capable of running under every circumstance. When the configuration management is no longer available, 
the agent will be looking for user settings under /opt/OpenvStorage/config/support_agent.json. When the option could not be found, 
the agent will default to idling and can only be activated via a manual intervention.

The /opt/OpenvStorage/config/support_agent.json is an optional file which can provide default behaviour for the support agent when all else fails.
This file is of a JSON format.

Possible settings for /opt/OpenvStorage/config/support_agent.json:
```
{
    "support_agent": true or false. Indicates if the support agent should run when the user setting could not be fetched from the config management.
    Note: The support agent will always send it's heartbeat when abled.
    "remote_access": true or false. Indicates if the support agent should process tasks given by the OpenvStorage monitoring server..
    "interval": <float>. Indicates the loop interval (in seconds) for the support agent.
} 
```