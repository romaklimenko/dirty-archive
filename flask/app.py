from flask import Flask, render_template, request
from pymongo import MongoClient, DESCENDING

app = Flask(__name__)

client = MongoClient('mongodb://localhost:27017')

db = client.dirty

@app.route("/")
def index():
  return render_template('index.html')

@app.route("/api/media")
def api_media():
  return db.media.find_one(
    { 'selected': { '$exists': False }, 'hash': { '$exists': True }, 'content_type': { '$regex': '^image' } },
    { 'content_type': 1, 'usage': 1, 'hash': 1, 'length': 1, 'ts': 1 }, sort=[('ts', DESCENDING)])

@app.route("/api/media/review", methods=['POST'])
def api_review():
  hash = request.form['hash']
  verdict = request.form['verdict'] == 'true'
  print('hash', hash)
  print('verdict', verdict)

  result = db.media.update_many(
    { 'hash': hash },
    {
      '$set': { 'selected': verdict },
      '$currentDate': { 'reviewed': True }
    })
  return result.raw_result, 200
