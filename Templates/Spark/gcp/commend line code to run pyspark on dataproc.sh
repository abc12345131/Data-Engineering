#copy the Python script from Cloud Storage so you can run it as a Cloud Dataproc Job.

gsutil cp gs://$PROJECT_ID/sparktodp/spark_analysis.py spark_analysis.py

#Create a launch script.

nano submit_onejob.sh

#Paste the following into the script:
#!/bin/bash
gcloud dataproc jobs submit pyspark --id=mjtelco-test-1 --cluster=mjtelco --region=global gs://qwiklabs-gcp-01-52268eca75ba/benchmark.py -- 20

#Press CTRL+X then Y to exit and save.

#Make the script executable:

chmod +x submit_onejob.sh

#Launch the PySpark Analysis job:

./submit_onejob.sh $PROJECT_ID
