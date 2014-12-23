#!/bin/bash

# storage driver / failovercache rotate on daily basis
# all files ending in a date format: YYYY-MM-DD will be compressed
# nr of days to keep files = nr of files itself
NR_OF_FILES_TO_KEEP=15

DATE_CMD=`which date`
FIND_CMD=`which find`
GREP_CMD=`which grep`
COMPRESS_CMD=`which gzip`
LSOF_CMD=`which lsof`
RM_CMD=`which rm`

PATH="/var/log/ovs/volumedriver"

TODAY=`$DATE_CMD --rfc-3339=date`
YEAR=`$DATE_CMD +%Y`


cleanup () {
  $FIND_CMD $1/*.gz -type f -mtime +${NR_OF_FILES_TO_KEEP} -exec $RM_CMD {} \;
}

compress () {
  $COMPRESS_CMD $1
}


FILES_TO_COMPRESS=$($FIND_CMD $PATH | $GREP_CMD $YEAR- | $GREP_CMD -v $TODAY | $GREP_CMD -v .gz$)

for file in $FILES_TO_COMPRESS
do
  # skip when in use
  $LSOF_CMD $file >/dev/null 2>&1
  IN_USE=$?
  if [ $IN_USE != 0 ]; then
    compress $file
  fi
done

# cleanup old files
cleanup $PATH
