# Create a resource
./cde resource upload --name tpcds-dag --local-path query-tpcds-dag.py

# Create Job of “airflow” type and reference the DAG
./cde job delete --name query-tpcds-dag-job
./cde job create --name query-tpcds-dag-job --type airflow --dag-file query-tpcds-dag.py  --mount-1-resource tpcds-dag 

#Trigger Airflow job to run
#./cde job run --name tpcds-dag-job 

