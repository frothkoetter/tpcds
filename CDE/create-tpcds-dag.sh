# Create a resource
./cde resource delete --name tpcds-dag 
./cde resource create --name tpcds-dag 
./cde resource upload --name tpcds-dag --local-path tpcds-dag.py

# Create Job of “airflow” type and reference the DAG
./cde job delete --name tpcds-dag-job
./cde job create --name tpcds-dag-job --type airflow --dag-file tpcds-dag.py  --mount-1-resource tpcds-dag 

#Trigger Airflow job to run
#./cde job run --name tpcds-dag-job 

