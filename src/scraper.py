from bs4 import BeautifulSoup, SoupStrainer

#import urllib3
#http = urllib3.PoolManager()

import requests

#--------------------------------------------------------------------

Sample_Wikipedia_Url = 'https://en.wikipedia.org/wiki/Elephant'

Wikipedia_Stop_Words = ['File:',
                        'Special:',
                        'International_Standard_Book_Number',
                        'Digital_object_identifier',
                        'Category:',
                        'Portal:',
                        'Help:',
                        'Wikipedia:',
                        'Template:',
                        'Talk:',
                        'Template_talk:',
                        'Main_Page',
                        'PubMed',
                        'https:',
                        'wikimediafoundation']

#--------------------------------------------------------------------

def get_url_response (url):
    return requests.get(url)

#--------------------------------------------------------------------

def get_url_data (url):
    response = get_url_response(url)
    return response.content

#--------------------------------------------------------------------

#soup_data = get_url_data(Sample_Wikipedia_Url)

#--------------------------------------------------------------------

def link_contains_stop_word (link, stop_words):
    result = False
    for x in stop_words:
        if x in link['href']:
            result = True
    return result

#--------------------------------------------------------------------

def extract_urls (response, filter='/wiki/', stop_words=Wikipedia_Stop_Words):
    urls = []
    soup = BeautifulSoup(response.content, 'lxml')
    for link in soup.find_all('a', href=True):
        if filter in link['href'] and not link_contains_stop_word (link, stop_words):
            urls.append(link['href'])
    urls = list(set(urls))
    return urls

#--------------------------------------------------------------------

def get_related_wikipedia_topics (topic):
    topic_url = 'https://en.wikipedia.org/wiki/' + topic
    response = get_url_response(topic_url)
    urls = extract_urls(response)
    result =  [x[6:] for x in urls]
    #result.remove(topic)
    return result

#------------------------------------------------------------------------------------------

def potential_subtopics(topic,topics):
    return [x for x in topics if topic.lower() in x.lower()]

#------------------------------------------------------------------------------------------
# End of File 
#------------------------------------------------------------------------------------------

