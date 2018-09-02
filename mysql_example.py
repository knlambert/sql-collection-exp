# coding utf-8

from sqlcollection import Client

# import logging
# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

#
# lookup=[
#     {
#         u"to": u"hour",
#         u"localField": u"project",
#         u"from": u"project",
#         u"foreignField": u"id",
#         u"as": u"project"
#     }, {
#         u"to": u"project",
#         u"localField": u"client",
#         u"from": u"client",
#         u"foreignField": u"id",
#         u"as": u"project.client"
#     }
# ]
import json
import datetime

client = Client(url=u'mysql+mysqlconnector://root:localroot1234@127.0.0.1/')
user = client.user_api._user

description = user.get_description(auto_lookup=3)
print(json.dumps(description, indent=4))

cursor = user.find(query={}, auto_lookup=1).sort(u"id").limit(2).skip(0)
for item in cursor:
    print(item)
#
# print(u"___count")
# count = hour.find(auto_lookup=0).limit(3).count()
# print(count)
#
# print(u"___count with_limit_and_skip")
# count = hour.find(auto_lookup=0).limit(3).count(with_limit_and_skip=True)
# print(count)
# quit()
#
# ret = user.insert_one({
#         "email": "test2",
#         "name": "test",
#         "hash": "test",
#         "salt": "test",
#         "customer_id": {
#             "id": 1
#         }
# }, auto_lookup=1)
# print(ret.inserted_id)
# #
ret = user.delete_many({
    u"customer_id.id": 2
}, auto_lookup=3)
#
# ret = hour.update_many({
#     u"project.id": 2
# }, {
#     u"$set": {
#         u"issue": u"updated",
#         u"project": {
#             u"id": 1
#         }
#     }
# }, auto_lookup=3)
