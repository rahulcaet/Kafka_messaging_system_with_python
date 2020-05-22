import requests
import base64
import json
import sys

#NOTE: creating customer is failing don't know wy? even manual curl command not working, ref: https://docs.confluent.io/1.0/kafka-rest/docs/intro.html

#curl -X POST -H "Content-Type: application/vnd.kafka.v1+json" --data '{"id": "my_instance", "format": "binary", "auto.offset.reset": "smallest"}' http://localhost:8082/consumers/my_binary_consumer

def consumer(host, port, groupid, topic):
    #Base URL for interacting with REST server
    baseurl = "http://{host}:{port}/consumers/{groupid}".format(host=host,
                                                                port=port,
                                                                groupid=groupid)


    #Create the Consumer instance
    print("Creating consumer instance")
    print('baseurl is:', baseurl)
    payload = {
        "format":"binary",
        "auto.offset.reset":"smallest"
    }
    headers = {
        "Content-Type" : "application/vnd.kafka.v1+json"
    }
    r = requests.post(baseurl, data=json.dumps(payload), headers=headers)

    if r.status_code !=200:
        print("Status Code: " + str(r.status_code))
        print(r.text)
        sys.exit("Error thrown while creating consumer")

    # Base URI is used to identify the consumer instance
    base_uri = r.json()["base_uri"]

    #Get the messages from the consumer
    headers = {
        "Accept" : "application/vnd.kafka.binary.v1+json",
    }
    # Request messages for the instance on the Topic
    r = requests.get(base_uri + "/topics/{topic}".format(topic=topic), headers = headers, timeout =20)

    if r.status_code != 200:
        print("Status Code: " + str(r.status_code))
        print(r.text)
        sys.exit("Error thrown while getting message")

    # Output all messages
    for message in r.json():
        if message["key"] is not None:
            print("Message Key:" + base64.b64decode(message["key"]))
        print("Message Value:" + base64.b64decode(message["value"]))

    # When we're done, delete the consumer
    headers = {
        "Accept" : "application/vnd.kafka.v1+json",
    }

    r = requests.delete(base_uri, headers=headers)

    if r.status_code != 204:
        print("Status Code: " + str(r.status_code))
        print(r.text)

if __name__ == '__main__':
    consumer(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
