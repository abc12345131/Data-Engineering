#create composer environment wiyh bottom code
gcloud composer environments create myairflow \
    --location=us-central1 \
    --zone=us-central1-c \
    --machine-type=n1-standard-1 \
    --image-version composer-1.14.4-airflow-1.10.14 \
    --disk-size=20
    --python-version=3