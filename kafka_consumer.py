
import json
from time import sleep

from bs4 import BeautifulSoup
from kafka import KafkaConsumer



def parse(markup):
    title = '-'
    submit_by = '-'
    description = '-'
    calories = 0
    ingredients = []
    rec = {}

    try:

        soup = BeautifulSoup(markup, 'lxml')
        # title
        title_section = soup.select('.recipe-summary__h1')
        # submitter
        submitter_section = soup.select('.submitter__name')
        # description
        description_section = soup.select('.submitter__description')
        # ingredients
        ingredients_section = soup.select('.recipe-ingred_txt')

        # calories
        calories_section = soup.select('.calorie-count')
        if calories_section:
            calories = calories_section[0].text.replace('cals', '').strip()

        if ingredients_section:
            for ingredient in ingredients_section:
                ingredient_text = ingredient.text.strip()
                if 'Add all ingredients to list' not in ingredient_text and ingredient_text != '':
                    ingredients.append({'step': ingredient.text.strip()})

        if description_section:
            description = description_section[0].text.strip().replace('"', '')

        if submitter_section:
            submit_by = submitter_section[0].text.strip()

        if title_section:
            title = title_section[0].text

        rec = {'title': title, 'submitter': submit_by, 'description': description, 'calories': calories,
               'ingredients': ingredients}

    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
    finally:
        return json.dumps(rec)

