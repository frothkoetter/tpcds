DATDIR=/home/cdsw/tpcds
SCALE=10
mkdir $DATDIR
mkdir $DATDIR/$SCALE

# java -jar tpcds-1.3-SNAPSHOT-jar-with-dependencies.jar --scale 10 --directory /home/cdsw/tpcds/10

FILES="call_center catalog_page catalog_returns catalog_sales customer_address customer customer_demographics
date_dim dbgen_version household_demographics income_band inventory item promotion reason ship_mode store store_returns
store_sales time_dim warehouse web_page web_returns web_sales web_site"
for dat in $FILES; do
  mkdir $DATDIR/$SCALE/$dat
  mv $DATDIR/$SCALE/$dat.dat $DATDIR/$SCALE/$dat/$dat.csv
done 

HDFSDIR=s3a://goes-se-sandbox01/tpcds/$SCALE/
hdfs dfs -mkdir $HDFSDIR

hdfs dfs -copyFromLocal $DATDIR/$SCALE $HDFSDIR
