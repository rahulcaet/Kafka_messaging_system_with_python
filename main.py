from kafka_producer import *
from kafka_consumer import *
from time import sleep
from kafka import KafkaConsumer
import json

if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }

    url = 'https://www.allrecipes.com/recipes/96/salad/'


    print('running producer...')
    all_recipes = get_recipes(url, headers)

    print('all_recipes are: ', all_recipes)

    if len(all_recipes) > 0:
        kafka_producer = connect_kafka_producer()
        for recipe in all_recipes:
            publish_message(kafka_producer, 'raw_recipes', 'raw', recipe.strip())
        if kafka_producer is not None:
            kafka_producer.close()
            

    print('now running consumer...')

    parsed_records = []
    topic_name = 'raw_recipes'
    parsed_topic_name = 'parsed_recipes'
    final_output = {}
    calories_threshold = 20

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    for msg in consumer:
        # print('msg is:', msg)
        html = msg.value
        result = parse(html)
        parsed_records.append(result)

    #print('parsed_records: ', parsed_records)

    consumer.close()
    sleep(5)

    if len(parsed_records) > 0:
        print('Publishing records..')
        producer = connect_kafka_producer()
        for rec in parsed_records:
            publish_message(producer, parsed_topic_name, 'parsed', rec)

    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)


    for msg in consumer:
        record = json.loads(msg.value)
        calories = int(record['calories'])
        title = record['title']

        if calories > calories_threshold:
            final_output[title] = calories_threshold
        sleep(3)

    for title in final_output:
        print('Alert: {} calories count is {}'.format(title, final_output[title]))

    if consumer is not None:
        consumer.close()


