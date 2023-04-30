from confluent_kafka import Consumer

def login_info():
    info={'bootstrap.servers':'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',
          'security.protocol':'SASL_SSL',
          'sasl.mechanism':'PLAIN',
          'sasl.username':'GXPZJPTLQMOSBVKA',
          'sasl.password':'c35F9t9ywdnocNNb1TAyGYaVmCIPynX0iPAWYxoe+ygDjBqBQA3hITu2cfykNtNt',
          'group.id':'my-group',
          'auto.offset.reset':'earliest'
          }
    
    return info

c=Consumer(login_info())
print(c.list_topics().topics)
t=input('Enter the require topic name that you want to subscribe to:')
c.subscribe([t])

while True:
    try:
        msg=c.poll(5.0)
        if msg is None:
            print('Waiting for messages')
            continue
        if msg is not None:
            print(f'The following msg is {msg.value()}')
    except KeyboardInterrupt:
        print('Closing the consumer connection')
        break

c.close()