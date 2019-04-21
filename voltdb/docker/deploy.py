#!/usr/bin/env python

import sys, os

deploymentText = """<?xml version="1.0"?>
<deployment>
    <cluster hostcount="##HOSTCOUNT##" kfactor="##K##" />
    <httpd enabled="true"><jsonapi enabled="true" /></httpd>
</deployment>
"""

deploymentText = deploymentText.replace("##HOSTCOUNT##", sys.argv[1])
deploymentText = deploymentText.replace("##K##", sys.argv[2])

with open('/root/voltdb-ent/deployment.xml', 'w') as f:
    f.write(deploymentText)

os.execv("/root/voltdb-ent/bin/voltdb",
         ["voltdb",
          "create",
          "--deployment=/root/voltdb-ent/deployment.xml",
          "--host=" + sys.argv[3]])
