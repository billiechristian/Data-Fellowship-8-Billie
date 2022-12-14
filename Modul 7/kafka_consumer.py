from ctypes import *
CDLL(r'C:\Users\Billie\anaconda3\Lib\site-packages\confluent_kafka.libs\librdkafka-09f4f3ec.dll')

from confluent_kafka import Consumer
import json

################
topic_name = "twitter-data"
c=Consumer({'bootstrap.servers':'localhost:9092'
    ,'group.id':'python-consumer'
    ,'auto.offset.reset':'earliest'})

print('Kafka Consumer has been initiated...')

print('Available topics to consume: ', c.list_topics().topics)
c.subscribe([topic_name])
################
def main():
    while True:
        msg=c.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data=msg.value().decode('utf-8')
        print(data)
        print('\n \n')
    c.close()
        
if __name__ == '__main__':
    main()