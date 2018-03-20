#!/bin/bash
GID=1 # Docker containers executes Raphtory as 'bin' user
SUDOER_UID=`id -u $USER`
mkdir -p ./logs/
chown $SUDOER_UID:1 -R ./logs
find ./logs -type d -exec chmod 775 {} \;
find ./logs -type f -exec chmod 664 {} \;

