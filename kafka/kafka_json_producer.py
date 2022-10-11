#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of JSONSerializer.

import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "restaurant_orders.csv"
columns=['order_number', 'order_date', 'item_name', 'quantity', 'product_price', 'total_products']

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


def get_order_instance(file_path):
    orders = []
    csv_file = open(file_path, "r")
    for line in csv_file:
      if line.startswith("O"):
        continue
      data = line.replace("\n","").split(",")
      data[0] = int(data[0])
      data[3] = int(data[3])
      data[4] = float(data[4])
      data[5] = int(data[5])
      order=restaurant_orders(dict(zip(columns,data)))
      orders.append(order)
      yield order

def order_to_dict(order:restaurant_orders, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return order.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema = schema_registry_client.get_schema(schema_id="100003")
    schema_str = schema.schema_str
    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, order_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for order in get_order_instance(file_path=FILE_PATH):
            print(order)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), order_to_dict),
                            value=json_serializer(order, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("restaurent-take-away-data")
