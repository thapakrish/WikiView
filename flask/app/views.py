from flask import jsonify
from app import app
from flask import render_template
from flask import request
from flask import json 


@app.route('/')
@app.route('/index')
def index():
  return render_template("index.html")

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster


cluster = Cluster(['a','b','c','d'])


#session = cluster.connect('wiki')
session = cluster.connect('wiki')

######### PAGEVIEWS
@app.route('/pageviews', methods=['GET'])
def pageviews():
 return render_template("pageviews.html")


@app.route("/pageviews", methods=['POST'])
def pageviews_post():

  titles = request.form["title"]
  title_list = titles.split(',')
  print(" Titles: ----{} \n".format(title_list))

  jsonresponse = []

  for pgtitle in title_list:
    pgtitle = pgtitle.lstrip()
    pgtitle = pgtitle.replace(" ","_").title()
    print(" Page title: ----{} \n".format(pgtitle))
    stmt = "SELECT * FROM hourly WHERE title=%s"
    response = session.execute(stmt, parameters=[pgtitle])
    response_title = []
    for val in response:
      response_title.append(val)
    jsonTitle = [{"title": x.title, "ymdh": x.ymdh[:13], "vcount": x.vcount} for x in response_title]
    jsonresponse.extend(jsonTitle)

#  print(jsonresponse)
  return render_template("pageviewsop.html", output=jsonresponse)



######### Graph

@app.route('/graph', methods=['GET'])
def graph():
 return render_template("graph.html")


@app.route("/graph", methods=['POST'])
def graph_post():
  session = cluster.connect('graph')
  titles = request.form["title"]
  pgtitle = titles.lstrip()
  pgtitle = pgtitle.replace(" ","_").title()

  jsonresponse = []

  stmt = "SELECT * FROM graph.g2 WHERE pgfrom=%s"
  response = session.execute(stmt, parameters=[pgtitle])
  response_title = []
  for val in response:
   response_title.append(val)
  jsonTitle = [{"pgfrom": x.pgfrom, "pgto": x.pgto, "pgtoto": [x.encode('UTF8') for x in x.pgtoto]} for x in response_title]
  jsonresponse.extend(jsonTitle)

#  print(jsonresponse)
  return render_template("graphop.html", output=jsonresponse)


