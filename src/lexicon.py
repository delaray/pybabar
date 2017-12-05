from bs4 import BeautifulSoup, SoupStrainer
import requests

import scraper

#--------------------------------------------------------------------
# Merriam Webster Scraper
#--------------------------------------------------------------------

def extract_pos (response):
    pos = []
    soup = BeautifulSoup(response.content, 'lxml')
    for link in soup.find_all('span'):
        if link.get('class')==['fl']:
            pos.append(link)
    pos = pos[0].contents
    return pos[0]

def get_word_pos(word):
    word_url = 'https://www.merriam-webster.com/dictionary/' + word
    response = scraper.get_url_response(word_url)
    pos = extract_pos(response)
    return pos

#--------------------------------------------------------------------


def get_word_definition(word):
    print ("Not implemmented")
    return None



#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------
