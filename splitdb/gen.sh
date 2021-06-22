# !/bin/bash

DBDIR=$1
if [ -z $DBDIR ]; then
  echo "give the path to the original database directory"
  exit -1
fi
#DBDIR="../Tree-Based-FIM/datasets"

bash split_interval.sh -p $DBDIR -d retail -g 20000
bash split_interval.sh -p $DBDIR -d kosarak -g 20000
bash split_interval.sh -p $DBDIR -d chainstore -g 20000
bash split_interval.sh -p $DBDIR -d record -g 20000
bash split_interval.sh -p $DBDIR -d retail -g 40000
bash split_interval.sh -p $DBDIR -d kosarak -g 40000
bash split_interval.sh -p $DBDIR -d chainstore -g 40000
bash split_interval.sh -p $DBDIR -d record -g 40000
bash split_interval.sh -p $DBDIR -d retail -g 60000
bash split_interval.sh -p $DBDIR -d kosarak -g 60000
bash split_interval.sh -p $DBDIR -d chainstore -g 60000
bash split_interval.sh -p $DBDIR -d record -g 60000
bash split_interval.sh -p $DBDIR -d retail -g 80000
bash split_interval.sh -p $DBDIR -d kosarak -g 80000
bash split_interval.sh -p $DBDIR -d chainstore -g 80000
bash split_interval.sh -p $DBDIR -d record -g 80000
bash split_interval.sh -p $DBDIR -d retail -g 100000
bash split_interval.sh -p $DBDIR -d kosarak -g 100000
bash split_interval.sh -p $DBDIR -d chainstore -g 100000
bash split_interval.sh -p $DBDIR -d record -g 100000