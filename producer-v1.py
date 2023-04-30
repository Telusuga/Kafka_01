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
    info={'bootstrap.servers':'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',
          'security.protocol':'SASL_SSL',
          'sasl.mechanism':'PLAIN',
          'sasl.username':'GXPZJPTLQMOSBVKA',
          'sasl.password':'c35F9t9ywdnocNNb1TAyGYaVmCIPynX0iPAWYxoe+ygDjBqBQA3hITu2cfykNtNt',
          'acks':'1',
          'partitioner':'consistent_random'
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


'''
for i in range(0,10):
    msg={'id':randint(1,1000),'Name':Faker('en_US').name(),'Country':Faker('en_US').country()}

    p.produce(topic=t,value=str(msg),on_delivery=delivery_status)
'''
p.flush()



