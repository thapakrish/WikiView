from flask import jsonify
from app import app
from flask import render_template
from flask import request
from flask import json 

@app.route('/')
@app.route('/index')
def index():
  return render_template("base.html")
  """
  user = { 'nickname': 'Miguel' } # fake user
  mylist = [1,2,3,4]
  return render_template("index.html", title = 'Home', user = user, mylist = mylist)
  """

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

cluster = Cluster(['172.31.1.100','172.31.1.107','172.31.1.104','172.31.1.101','172.31.1.110','172.31.1.109'])

#session = cluster.connect('playground')

"""
@app.route('/api/<email>/<date>')
def get_email(email, date):
       stmt = "SELECT * FROM email WHERE id=%s and date=%s"
       response = session.execute(stmt, parameters=[email, date])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
       return jsonify(emails=jsonresponse)
"""
"""
@app.route('/email')
def email():
 return render_template("email.html")

@app.route("/email", methods=['POST'])
def email_post():
 emailid = request.form["emailid"]
 date = request.form["date"]

 #email entered is in emailid and date selected in dropdown is in date variable respectively

 stmt = "SELECT * FROM email WHERE id=%s and date=%s"
 response = session.execute(stmt, parameters=[emailid, date])
 response_list = []
 for val in response:
    response_list.append(val)
 jsonresponse = [{"fname": x.fname, "lname": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
 return render_template("emailop.html", output=jsonresponse)
"""

@app.route('/realtime')
def realtime():
 return render_template("realtime.html")



#session1 = cluster.connect('wiki_test')
session1 = cluster.connect('wiki_test')

@app.route('/pageviews')
def pageviews():
 return render_template("pageviews.html")

@app.route("/pageviews", methods=['POST'])
def pageviews_post():
  title = request.form["title"]

  #email entered is in emailid and date selected in dropdown is in date variable respectively

  # stmt = "SELECT * FROM test1 WHERE title=%s"
  stmt = "SELECT * FROM test2 WHERE title=%s"
  response = session1.execute(stmt, parameters=[title])
  response_list = []
  for val in response:
    response_list.append(val)
    # jsonresponse = [{"title": x.title, "ymdh": x.ymdh[:13], "vcount": x.vcount} for x in response_list]
  jsonresponse = [{"title": x.title, "ymdh": x.ymdh[:13], "vcount": x.vcount} for x in response_list]
  # print(jsonresponse)
  return render_template("pageviewsop.html", output=jsonresponse)





session2 = cluster.connect('wiki_test')
@app.route('/trending')
def trending():
 return render_template("trending.html")

@app.route("/trending", methods=['POST'])
def trending_post():
  title = request.form["title"]

  #email entered is in emailid and date selected in dropdown is in date variable respectively

  # stmt = "SELECT * FROM test1 WHERE title=%s"
  stmt = "SELECT * FROM test2 WHERE title=%s"
  response = session2.execute(stmt, parameters=[title])
  response_list = []
  for val in response:
    response_list.append(val)
    # jsonresponse = [{"title": x.title, "ymdh": x.ymdh[:13], "vcount": x.vcount} for x in response_list]
  jsonresponse = [{"title": x.title, "ymdh": x.ymdh[:13], "vcount": x.vcount} for x in response_list]
  # print(jsonresponse)
  return render_template("trendingop.html", output=jsonresponse)

