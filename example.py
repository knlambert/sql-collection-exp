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

client = Client(url=u'mysql://root:localroot1234@127.0.0.1/')
hour = client.hours_count.hour

description = hour.get_description(auto_lookup=3)

print(json.dumps(description, indent=4))


# cursor = hour.find(auto_lookup=1, projection={
#     u"project": -1,
#     u"affected_to.id": -1
# }).limit(4)
# for item in cursor:
#     print(item)
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
# ret = hour.insert_one({
#         u'issue': '',
#         u'affected_to': {
#             u"id": 1
#         },
#         u'project': {
#             u"id": 2
#         },
#         u'started_at': datetime.datetime(2017, 12, 18, 14, 0),
#         u'minutes': 333
# }, auto_lookup=1)
# print(ret.inserted_id)
# #
# # ret = hour.delete_many({
# #     u"project.id": 2
# # }, auto_lookup=3)
#
# ret = hour.update_many({
#     u"project.id": 2
# }, {
#     u"$set": {
#         u"issue": u"updated",
#         u"project.client.name": u"BNC2"
#     }
# }, auto_lookup=3)
