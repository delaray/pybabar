import os
import sys
import json
from flask import Flask
from flask import request

sys.path.append('./src')

server = Flask(__name__)

#----------------------------------------------------------------------
# Route: /home
#----------------------------------------------------------------------

home_str = (
   "<html><body><h1> \n" 
   "Welcome to the Babar Python 3.8 server.</h1>" 
   "<br><a>This is version 0.1 </a><br>" 
   "<p>The Python sever handles crawling, scraping and all neural based machine learning activities. This server handles requests to generate word embeddings for a single topic or a collection of topics, and also allows for the automated generation of topic-based classifiers.\nThe following routes are defined:\n</p>" 
   "<br><a>/home      : Displays this page. </a>" 
   "<br><a>/topic-embeddings?<topic>  : Build word embeddings for <topic>. </a>"
   "<br></body></html>")

#----------------------------------------------------------------------

@server.route("/")
def hello(): return home_str

#----------------------------------------------------------------------

@server.route("/home")
def home(): return home_str

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
