from kafka import KafkaConsumer
import json, ast, sys, os
from google.cloud import storage


if __name__ == "__main__":
    consumer = KafkaConsumer(sys.argv[1],  bootstrap_servers=[sys.argv[2]])
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = sys.argv[3]
    client = storage.Client()
    bucket = client.get_bucket(sys.argv[4])
    for message in consumer:
        data = message.value.decode("utf-8")
        data = ast.literal_eval(data)
        blob = bucket.blob("task_2.json")
        blob.upload_from_string(
            data=json.dumps(data, indent=4),
            content_type='application/json'
        )
