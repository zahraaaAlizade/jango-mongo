from datetime import date
import pymongo
from datetime import datetime
from pprint import pprint
from pymongo import MongoClient
client = MongoClient("localhost",27017)
db=client.city
data= db.data1
print(data.count())


result=data.find({'$and':[{'location.city':'گلستان'},{'dob.age':{'$gt':50}}]}, {'name.first':1,'name.last':1 , '_id':0})
for i in result:
    pprint(i)
print("-------------------------------------------------------------------")

result2=db.data.find({'registered.age':{'$gte':20}},{'name.first':1,'name.last':1,'location':1})
for i in result2:
    pprint(i)

print("-------------------------------------------------------------------")
result3 = data.find({
"$expr": {"$and": [{"$eq": [{ "$dayOfMonth": {'$dateFromString': {'dateString': '$dob.date'}}}, datetime.now().day]},{"$eq": [ { "$month": {'$dateFromString': {'dateString': '$dob.date'}}}, datetime.now().month ]}]}},{'name.first':1,'name.last':1, 'email':1,'dob.date':1,"_id":0})
for i in result3:
    pprint(i)

print("-------------------------------------------------------------------")
result4=db.data.aggregate([{'$group':{'_id':'$location.state','count':{'$sum' :1}}}])
for i in result4:
    pprint(i)

print("-------------------------------------------------------------------")
result5 = data.aggregate([{"$facet":{"maximum":[{"$group": {"_id": "$location.city", "count": {"$sum": 1}}}, {"$sort": {"count" : -1}} ,{"$limit" : 1}],
"minimum": [{"$group": {"_id": "$location.city", "count": {"$sum": 1}}}, {"$sort": {"count": 1}}, {"$limit": 1}],}}])
for i in result5:
    pprint(i)

print("-------------------------------------------------------------------")
result6=db.data.aggregate([{'$unwind':"$location"},{"$unwind":"$dob"},{"$group":{"_id" :"$location.city","avg_age":{"$avg":"$dob.age"}}}])
result7=db.data.aggregate([{"$unwind":"$location"},{"$unwind":"$dob"},{"$group":{"_id":"$location.city","avg_age":{"$avg":"$dob.age"}}},{"$project":{"comp": {"$cmp": [ "$avg_age", 46.36842105263158 ] }}}])
for i in result6:
    pprint(i)
for i in result7:
    pprint(i)

print("-------------------------------------------------------------------")

result8=db.data.aggregate([{"$unwind":"$dob"},{"$project":{"_id":0,"name.first":1,"name.last":1,"youth":{"$lt":["$dob.age",16]},"middle":{"$and":[{"$gt":["$dob:age",16]},{"$lt":["$dob.age",40]}]},"old":{"$gt":["$dob.age",40]}}}])
for i in result8:
    pprint(i)

print(db.data1.count())
