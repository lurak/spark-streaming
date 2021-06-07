from kafka.producer import KafkaProducer
import json, requests
import ast, sys


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=[sys.argv[1]],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    r = requests.get("https://stream.meetup.com/2/rsvps", stream=True)

    for line in r.iter_lines():
        line = line.decode("utf-8")
        line = ast.literal_eval(line)
        if line['group']['group_country'] == 'us':
            producer.send('meetup', line)
        else:
            continue
