from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from random import randint
from faker import Faker

def delivery_status(err,msg):
    if err:
        print(f'Message failed to deliver due to the following error {str(err)}')
    else:
        print(f'The Message has been delivered successfully {msg.value()}. Partition-->{msg.partition()} at the Offset--->{msg.offset()}')
        print(f'The following messages are pushed to Topic--->{msg.topic()}')

def login_info():
    info={'bootstrap.servers':'xxxxxxxxxxxxxxx',#Bootstrap server is the place where my kafka cluster is running 
          'security.protocol':'SASL_SSL',
          'sasl.mechanism':'PLAIN',
          'sasl.username':'xxxxxxxxx',#API key can be used as username for sasl mechanism
          'sasl.password':'xxxxxxxxxxx',#API secret can be used for password
          'acks':'1',#Upon a successful delivery to the topic it will show a message
          'partitioner':'consistent_random'#This parameter helps to send messages to any random partition in the topic
          }
    
    return info


p=Producer(login_info())

print(p.list_topics().topics)

t=input('Enter the Topic Name for which you want to push the messages:')

while True:
    try:
        msg={'id':randint(1,1000),'Name':Faker('en_US').name(),'Country':Faker('en_US').country()}
        p.poll(1.0)
        p.produce(topic=t,value=str(msg),on_delivery=delivery_status)
    except KeyboardInterrupt:
        break

print('Flusing records')

p.flush()



