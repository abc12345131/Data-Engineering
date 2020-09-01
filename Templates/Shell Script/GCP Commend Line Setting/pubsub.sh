#Create your topic and publish a simple message

gcloud pubsub topics create sandiego

#Publish a simple message

gcloud pubsub topics publish sandiego --message "hello"

#Create a subscription for the topic

gcloud pubsub subscriptions create --topic sandiego mySub1

#Pull the first message that was published to your topic

gcloud pubsub subscriptions pull --auto-ack mySub1

#Publish another message and then pull it using the subscription

gcloud pubsub topics publish sandiego --message "hello again"
gcloud pubsub subscriptions pull --auto-ack mySub1