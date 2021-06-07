import ast, json
import sys
from datetime import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka.producer import KafkaProducer

TOPICS = {"Computer programming", "Big Data", "Machine Learning", "Python", "Java", "Web Development"}


def sender(x, topic, producer):
    res = x.take(1000)
    for data in res:
        producer.send(topic, data)


def map_function(x):
    res = x['group']['group_topics']
    topics = set()
    for topic in res:
        topics.add(topic['topic_name'])
    return topics, {'event': {
                         "event_name": x['event']['event_name'],
                         "event_id": x['event']['event_id'],
                         "time": datetime.strftime(datetime.fromtimestamp(x['event']['time']//1000)
                                                   ,"%Y-%d-%m-%H:%M:%S")
                              },
                    "group_topics": list(topics),
                    'group_city': x['group']['group_city'],
                    'group_country': x['group']['group_country'],
                    'group_id': x['group']['group_id'],
                    'group_name': x['group']['group_name'],
                    'group_state': x['group']['group_state']}


def filter_function(x):
    return x[0] & TOPICS


def reduce_function(x, y):
    x['cities'].extend(y['cities'])
    return x


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=[sys.argv[1]],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    sc = SparkContext("local[*]", "NetworkWordCount")
    ssc = StreamingContext(sc, 1)
    sc.setLogLevel('ERROR')
    kvs = KafkaUtils.createStream(ssc, sys.argv[2], "1", {"meetup": 3})
    data = kvs.map(lambda x: x[1]).map(lambda x: ast.literal_eval(x))
    events = data.map(lambda x: {'event': {
        "event_name": x['event']['event_name'],
        "event_id": x['event']['event_id'],
        "time": datetime.strftime(datetime.fromtimestamp(x['event']['time'] // 1000)
                                  , "%Y-%d-%m-%H:%M:%S")
    },
        'group_city': x['group']['group_city'],
        'group_country': x['group']['group_country'],
        'group_id': x['group']['group_id'],
        'group_name': x['group']['group_name'],
        'group_state': x['group']['group_state']})
    events.foreachRDD(lambda x: sender(x, 'US-meetups', producer))
    events.pprint(10)
    cur_time = datetime.now()
    cities = data.window(60, 60).map(lambda x: {"month": cur_time.month,
                                                "day_of_the_moth":  cur_time.day,
                                                "hours": cur_time.hour,
                                                "minute": cur_time.minute,
                                                "cities": [x['group']['group_city']]})\
        .reduce(reduce_function)
    cities.pprint(10)
    cities.foreachRDD(lambda x: sender(x, 'US-cities-every-minute', producer))
    events = data.map(map_function).filter(filter_function).map(lambda x: x[1])
    events.foreachRDD(lambda x: sender(x, 'Programming-meetups', producer))
    events.pprint(10)
    ssc.start()
    ssc.awaitTermination()
