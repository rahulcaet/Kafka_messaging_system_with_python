import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from time import sleep


def fetch_raw(recipe_url, headers):
    html = None
    print('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url, headers)
        if r.status_code == 200:
            html = r.text
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html.strip()


def get_recipes(url, headers):
    recipies = []

    print('Accessing list')

    try:
        r = requests.get(url, headers)
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            links = soup.select('.fixed-recipe-card__h3 a')
            idx = 0
            for link in links:
                sleep(2)
                recipe = fetch_raw(link['href'], headers)
                recipies.append(recipe)
                idx += 1
                if idx > 2:
                    break
    except Exception as ex:
        print('Exception in get_recipes' , str(ex))
        print(str(ex))
    finally:
        return recipies


def on_send_success(record_metadata):
    print('Message Succesfully Sent {topic} {partition} {ooffset}'.format(topic=record_metadata.topic,
                                                                          partition=record_metadata.partition,
                                                                          offset=record_metadata.ooffset))

def on_send_error(excp):
    print('Error while sending message', str(excp))

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name,
                               key=key_bytes,
                               value=value_bytes).add_callback(on_send_success).add_errback(on_send_error)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return producer


if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }

    url = 'https://www.allrecipes.com/recipes/96/salad/'
    all_recipes = get_recipes(url)

    if len(all_recipes) > 0:
        kafka_producer = connect_kafka_producer()
        for recipe in all_recipes:
            publish_message(kafka_producer, 'raw_recipes', 'raw', recipe.strip())
        if kafka_producer is not None:
            kafka_producer.close()

