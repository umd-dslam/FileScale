#!/usr/bin/env python

import sys, os

# Time frequency is specified in milliseconds and transaction frequency is specified as
# the number of transactions. You can specify either or both types of frequency. If you
# specify both, whichever limit is reached first initiates a write.
# https://docs.voltdb.com/UsingVoltDB/CmdLogConfig.php

deploymentText = """<?xml version="1.0"?>
<deployment>
    <cluster hostcount="##HOSTCOUNT##" kfactor="##K##" />
    <httpd enabled="true"><jsonapi enabled="true" /></httpd>

    <commandlog enabled="true" synchronous="true" >
        <frequency time="2" />
    </commandlog>
    <snapshot frequency="1h" retain="12" prefix="prefix" enabled="false"/>
</deployment>
"""

#deploymentText= """<?xml version="1.0"?>
#<deployment>
#    <cluster hostcount="##HOSTCOUNT##" kfactor="##K##" />
#    <httpd enabled="true"><jsonapi enabled="true" /></httpd>
#
#    <commandlog enabled="true" synchronous="false" >
#        <frequency time="200" transactions="10000"/>
#    </commandlog>
#    <snapshot frequency="1h" retain="12" prefix="prefix" enabled="false"/>
#</deployment>
#"""

deploymentText = deploymentText.replace("##HOSTCOUNT##", sys.argv[1])
deploymentText = deploymentText.replace("##K##", sys.argv[2])

with open('/root/voltdb-ent/deployment.xml', 'w') as f:
    f.write(deploymentText)

os.execv("/root/voltdb-ent/bin/voltdb",
         ["voltdb",
          "create",
          "--deployment=/root/voltdb-ent/deployment.xml",
          "--host=" + sys.argv[3]])

# run voltdb
# ./deploy.py 1 0 0.0.0.0 &
