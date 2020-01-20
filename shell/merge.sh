#!/bin/sh
source /etc/profile
yestoday=`date -d '1 days ago' +%Y-%m-%d`
month=`date -d ${yestoday} +%m`
year=`date -d ${yestoday} +%Y`
bin=`dirname $0`
bin=`cd $bin;pwd`
hadoop jar $bin/hdfsmerge.jar /data_center/$1/year=${year}/month=${month}/ /data_center/$1/year=${year}/month=${month}/${yestoday}.txt ${yestoday}.*.txt true




定时脚本 crontab -e

20 1 * * * /root/app/scripts/hdfs_merge.sh article > /dev/null 2>&1 &
30 1 * * * /root/app/scripts/hdfs_merge.sh weibo > /dev/null 2>&1 &
