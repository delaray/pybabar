import os
import sys
import json
from flask import Flask
from flask import request
from concurrent.futures import ThreadPoolExecutor
from time import sleep

sys.path.append('./src')

server = Flask(__name__)

#----------------------------------------------------------------------
# Route: /home
#----------------------------------------------------------------------

home_str = (
   "<html><style> body {background-color:#90EE90}</style>"
   "<body><h1>Babar Python 3.8 server</h1>" 
   "<a>Version 0.1 </a><br>" 
   "<p>This Python server handles crawling, scraping and all neural based "
    "machine learning activities. The server also handles requests to generate "
    "word embeddings from data in specified file. Finally it "
    "allows for the automated generation of topic-based classifiers.</p>"
    "<br>"
    "<p>The following routes are currently defined:</p>"
    "<style> table {border:1px solid black}</style>"
    "<style> th {border:1px solid black;color:Blue}</style>"
    "<style> td {border:1px solid black}</style>"
    "<table>"
    "<tr>"
    "<th>Route</th>"
    "<th>Description</th>"
    "</tr>"
    "<tr>"
    "<td><a>home</a></td>"
    "<td><a>Displays this page</a></td>"
    "</tr>"
    "<tr>"
    "<td><a>topic-embeddings?file</a></td>"
    "<td><a>Build word embeddings from data in file</a></td>"
    "</tr>"
    "</table>"
    "<br></body></html>")

#----------------------------------------------------------------------

@server.route("/")
def hello(): return home_str

#----------------------------------------------------------------------

@server.route("/home")
def home(): return home_str

#----------------------------------------------------------------------

@server.route("/create-embedding")
def create_embedding():
    name = request.args.get('name')
    filename = request.args.get('file')
    print ('Name: ' + name)
    print ('File: ' + filename)
    return "Creating embedding..."


#----------------------------------------------------------------------
# Futures
#----------------------------------------------------------------------
 
def return_after_5_secs(message):
    sleep(5)
    return message
 
pool = ThreadPoolExecutor(3)
 
future = pool.submit(return_after_5_secs, ("hello"))
print(future.done())
sleep(5)
print(future.done())
print(future.result())

#----------------------------------------------------------------------
# Initialize server
#----------------------------------------------------------------------

def initialize_server():
   return True

#----------------------------------------------------------------------

if __name__ == "__main__":
   initialize_server()
   server.run(host='0.0.0.0', port=5000)

#----------------------------------------------------------------------
# Route: /taxonomy
#----------------------------------------------------------------------

# @server.route("/taxonomy")
# def get_taxonomy():
#    taxonomy = tx.TAXONOMY_TREE
#    return json.dumps(taxonomy, indent=5)

#----------------------------------------------------------------------
# Route: /predict-category
#----------------------------------------------------------------------

# def make_prediction_results(cat):
#    res = ("<html><body><h2> \n"
#           "Prediction Results</h21><br>"
#           "Category: " + str(cat) +
#           "<br></body></html>")
#    return res

#----------------------------------------------------------------------

#
# @server.route("/predict-category")
# def predict_category():
#    category = request.args.get('category')
#    text = request.args.get('text')
#    print ('\nCategory: ' + category)
#    classifier = cl.CATEGORY_CLASSIFIERS.get(category)
#    if classifier is not None:
#       res = pr.predict_category(classifier, text)
#       val = {'category' : res[0], 'confidence' : str(res[1])}
#       return json.dumps(val)
#    else:
#       return json.dumps({'Error 59' : 'Classifier not found.'})

#----------------------------------------------------------------------
# End of File
#----------------------------------------------------------------------
