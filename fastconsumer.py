from confluent_kafka import Consumer, KafkaError

c = Consumer({'bootstrap.servers': 'kafka', 'group.id': 'mygroup',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['test'])
running = True
while running:
    msg = c.poll()
    if not msg.error():
        print ("%s:%s:%d: key=%s value=%s" % (msg.topic(), msg.partition(),
                                              msg.offset(),
                                              str(msg.key()),
                                              msg.value().decode('utf-8')))
    elif msg.error().code() != KafkaError._PARTITION_EOF:
        print(msg.error())
        running = False
c.close()
