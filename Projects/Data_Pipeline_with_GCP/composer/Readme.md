#use sbt compile spark file
docker run -dit -p 9999:8080 -v /home/wbl/sbt:/home/wbl/sbt bigtruedata/sbt
docker exec -it dac2fd bash
sbt clean assembly

#upload test Spark jar file
gsutil cp /sparkjar/scala.jar gs://data-engineering-project

#create composer environment
gcloud composer environments create mytestairflow \
    --location=us-east1 \
    --zone=us-east1-b \
    --machine-type=n1-standard-2 \
    --image-version composer-1.14.4-airflow-1.10.10 \
    --disk-size=100
    --python-version=3

#upload DAG file
gcloud composer environments storage dags import \
  --environment airflow  --location us-east1 \
  --source airflow-dag/Dataproc_workflow.py

#add Variable through airflow UI
project: GCP project id ('helical-decoder-322615')
region: GCP region ('us-east1')
subnet: VPC subnet id ('default')
zone: GCP zone ('us-east1-b')

#upload test data
gsutil cp /data/banking.csv gs://data-engineering-prject

