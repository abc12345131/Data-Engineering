#copy the Python script from Cloud Storage so you can run it as a Cloud Dataproc Job.

gsutil cp gs://$PROJECT_ID/sparktodp/spark_analysis.py spark_analysis.py

#Create a launch script.

nano submit_onejob.sh

#Paste the following into the script:
#!/bin/bash
gcloud dataproc jobs submit pyspark \
       --cluster sparktodp \
       --region us-central1 \
       spark_analysis.py \
       -- --bucket=$1

#Press CTRL+X then Y to exit and save.

#Make the script executable:

chmod +x submit_onejob.sh

#Launch the PySpark Analysis job:

./submit_onejob.sh $PROJECT_ID
