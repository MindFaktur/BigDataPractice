import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

API_KEY = 'YVQGJUPDBWPGH4UA'
ENDPOINT_SCHEMA_URL  = 'https://psrc-l6o18.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'yNE9zEl+ooRjW5gpZTRw7Oj+eSA1g192GICVTHXRrBybWv1/oGshKuvvdEDjcKiE'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '7QJTATNQ6CMRXYUO'
SCHEMA_REGISTRY_API_SECRET = '7H2Es1cZUO+EgEMB/8VfRabd9g/tARoZdEOQLC/1eXOoqsrhi2UB8BqIKk3Jc0j8'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class restaurant_orders:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return restaurant_orders(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):
    out_file = open("output.csv", "a")
    doc_count = 0
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema = schema_registry_client.get_schema(schema_id="100003")
    schema_str = schema.schema_str
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=restaurant_orders.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id':1,
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if order is not None:
                out_file.write("User record {}: order: {}\n".format(msg.key(), order))
        except KeyboardInterrupt:
            break
    
    consumer.close()

main("restaurent-take-away-data")