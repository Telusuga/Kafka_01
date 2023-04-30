from confluent_kafka import Consumer

def login_info():
    info={'bootstrap.servers':'xxxxxxxxxxxxxxxxx',
          'security.protocol':'SASL_SSL',
          'sasl.mechanism':'PLAIN',
          'sasl.username':'xxxxxxxxxx',
          'sasl.password':'xxxxxxxxxxxxx',
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
