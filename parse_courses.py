import requests
from bs4 import BeautifulSoup
import re
from subject_codes import codes
from pymongo import MongoClient


def create_courses(dept_to_courses, gold_nuggets, silver_nuggets):

    for department in codes:
        i = 0
        vgm_url = 'http://www.columbia.edu/cu/bulletin/uwb/subj/' + codes[department] + '/_Fall2021_text.html'
        status = requests.get(vgm_url).status_code
        if status == 404:
            vgm_url = 'http://www.columbia.edu/cu/bulletin/uwb/sel/' + codes[department] + '_Fall2021_text.html'
        html_text = requests.get(vgm_url).text

        x = re.split("[A-Z]+[0-9]{4} ", html_text)

        for item in x:
            if i == 0:
                i+=1
                continue
            split = re.split("  +", item)
            if len(split) < 5:
                continue

            link = re.search("/.+/\"", split[0])


            if link != None:
                link = "http://www.columbia.edu" + link.group()[:-1]

            try:
                section = re.search(">.*<", split[0]).group()[1:-1]
            except:
                section = None

            title_words = split[3].lower().split(" ")
            title = ''
            for word in title_words:
                title += word.capitalize() + ' '
            title = title[:-1]

            try:
                id = re.search('[A-Z]+[0-9]{4}', link).group()
            except:
                id = None

            if any(char.isdigit() for char in split[4]):
                instructor = split[5].split('\n')[0]
                time = split[4]
            else:
                instructor = split[4].split('\n')[0]
                time = None

            if instructor in gold_nuggets:
                nugget = 'Gold'
            elif instructor in silver_nuggets:
                nugget = 'Silver'
            else:
                continue

            course = {
                'link': link,
                'section': section,
                'call_no': split[1],
                'points': split[2],
                'title': title,
                'time': time,
                'instructor': instructor,
                'id': id,
                'nugget': nugget,
                '_id': split[1]
            }


            if not department in dept_to_courses:
                dept_to_courses[department] = []

            dept_to_courses[department].append(course)

    return dept_to_courses


def get_silver_nuggets():

    silver_nuggets = set()

    url = 'https://web.archive.org/web/20210118142350/http://www.culpa.info/silver_nuggets'
    base_url = 'https://web.archive.org/web/20171023163449/http://culpa.info/professors/silver_nuggets?'

    text = BeautifulSoup(requests.get(url).text, "html.parser").get_text()
    for line in text.split('\n'):
        if ',' in line:
            instructor = line.replace('(TA)', '').strip()
            silver_nuggets.add(instructor)


    for i in range(1,6):
        text = BeautifulSoup(requests.get(base_url + 'page=' + str(i)).text, "html.parser").get_text()
        for line in text.split('\n'):
            if ',' in line:
                instructor = line.replace('(TA)', '').strip()
                silver_nuggets.add(instructor)


    return silver_nuggets



def get_gold_nuggets():

    gold_nuggets = set()

    url = 'https://web.archive.org/web/20210118135043/http://www.culpa.info/gold_nuggets'
    text = BeautifulSoup(requests.get(url).text, "html.parser").get_text()
    for line in text.split('\n'):
        if ',' in line:
            instructor = line.replace('(TA)', '').strip()
            gold_nuggets.add(instructor)

    return gold_nuggets

def populate_database():

    dept_to_courses = dict()
    silver_nuggets = get_silver_nuggets()
    gold_nuggets = get_gold_nuggets()
    create_courses(dept_to_courses, gold_nuggets, silver_nuggets)

    url = ''
    cluster = MongoClient(url)
    db = cluster['fall2021']

    for department, course_list in dept_to_courses.items():
        db[department].insert_many(course_list)


populate_database()
