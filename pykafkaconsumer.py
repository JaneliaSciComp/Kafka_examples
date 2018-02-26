from pykafka import KafkaClient
client = KafkaClient(hosts="kafka.int.janelia.org:9092")
topic = client.topics['test']
consumer = topic.get_simple_consumer()
for message in consumer:
    if message is not None:
        print ("%s:%d:%d: key=%s value=%s" % ('test', message.partition_id,
                                              message.offset,
                                              message.partition_key,
                                              message.value))
