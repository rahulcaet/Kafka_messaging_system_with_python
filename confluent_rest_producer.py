#!/usr/bin/env python3

import requests
import base64
import json
import sys

def producer(host, port, topic):
    url = "http://{host}:{port}/topics/{topic}".format(host=host,
                                                       port=port,
                                                       topic=topic)

    print('url is:', url)
    headers = {
        "Content-Type" : "application/vnd.kafka.binary.v1+json",
    }
    # Create one or more messages
    payload = {"records":
           [{
               "value":"S2Fma2E="
           }],
    }
    # Send the message
    r = requests.post(url, data=json.dumps(payload), headers=headers)
    if r.status_code != 200:
       print("Status Code: " + str(r.status_code))
       print(r.text)

def list_all_topics(host, port):
    url = "http://{host}:{port}/topics".format(host=host,
                                               port=port)

    print('url is:', url)

    #get all topics
    r = requests.get(url)
    if r.status_code != 200:
        print("Status Code: " + str(r.status_code))
    print(r.text)

def get_info_about_one_partition(host, port, topic):
    url = "http://{host}:{port}/topics/{topic}".format(host=host,
                                                       port=port,
                                                       topic=topic)

    print('url is:', url)
    r = requests.get(url)
    if r.status_code != 200:
        print("Status Code: " + str(r.status_code))
    print(r.text)


if __name__ == '__main__':
    list_all_topics(sys.argv[1], sys.argv[2])
    get_info_about_one_partition(sys.argv[1], sys.argv[2], 'user2')
    producer(sys.argv[1], sys.argv[2], sys.argv[3])
