from kafka import KafkaProducer
from kafka.errors import KafkaError
from random import randint
from time import gmtime, strftime, sleep

# Produce
producer = KafkaProducer(bootstrap_servers=['kafka'])
messagenum = 1
while True:
    future = producer.send('test', 'Periodic message ' + str(messagenum) + ' ' + strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime()))
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        # Decide what to do if produce request failed...
        print "Failed!"
        pass
    messagenum += 1
    print 'Topic:', record_metadata.topic
    print 'Partition:', record_metadata.partition
    print 'Offset:', record_metadata.offset
    sleep(randint(1,10))
