from bs4 import BeautifulSoup
import requests
import mysql.connector
from datetime import datetime


# TODO: da mettere in un file resources
TAGS = ['h1','h2','h3','h4','h5','h6']
INSERT_INTO_HEADLINE = "INSERT INTO headline (codSource, tag, title, href) VALUES ('{codSource}', '{tagName}', '{title}', '{href}');"
INSERT_INTO_HEADLINE_COUNT = "INSERT INTO headlineCount (idHeadline, codiceGiorno, count) VALUES (LAST_INSERT_ID(),'{codiceGiorno}',1);"
UPDATE_HEADLINE_COUNT = "UPDATE headlineCount SET count = count+1 WHERE idHeadline='{idHeadline}' AND codiceGiorno='{codiceGiorno}';"
SELECT_ID_HEADLINE = "SELECT id FROM headline WHERE title='{title}' AND href='{href}';"

CODICE_GIORNO = datetime.today().strftime('%Y%m%d')

SOURCES = {'REP':'https://www.repubblica.it','COR':'https://www.corriere.it','ANS':'https://www.ansa.it'}

def pairwise(list):
    i = iter(list)
    curr = next(i)

    while True:
        try:
            prev = curr
            curr = next(i)
            yield prev, curr
        except StopIteration:
            return

def scrape_start(codiceGiornale, url):

    html = (requests.get(url).text)
    soup = BeautifulSoup(html,'html.parser')

    conn = mysql.connector.connect(host='containers-us-west-109.railway.app',port=7617,user='root', passwd='dZXeqUyKjXCGVkCJfYb3',db='railway')
    mycursor = conn.cursor()

    for a, next_a in pairwise(soup.findAll('a', href=True)):

        tagName = a.parent.name
        title = a.text.strip()
        href = a['href'].replace(url,'')

        if tagName in TAGS and title and href:

            if len(href) > 200:
                href='N.A.'
            if len(title) > 200 or ' ' not in title:
                continue
            if href == next_a['href'].replace(url,''):
                next_a.string = title + ' ' + next_a.text.strip()
                continue

            mycursor.execute(SELECT_ID_HEADLINE.format(href=href, title=title.replace('\'','\'\'')))
            idHeadline = mycursor.fetchone()

            if idHeadline:
                mycursor.execute(UPDATE_HEADLINE_COUNT.format(idHeadline=idHeadline[0],codiceGiorno=CODICE_GIORNO))
            else:
                print (title)
                mycursor.execute(INSERT_INTO_HEADLINE.format(codSource=codiceGiornale,tagName=tagName, title=title.replace('\'','\'\''), href=href))
                mycursor.execute(INSERT_INTO_HEADLINE_COUNT.format(codiceGiorno=CODICE_GIORNO))

    conn.commit()

if __name__ == "__main__":
    for el in SOURCES:
        scrape_start(el, SOURCES[el])