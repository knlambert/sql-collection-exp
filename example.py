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
import datetime


client = Client(url=u'mysql://root:localroot1234@127.0.0.1/')
hour = client.hours_count.hour

cursor = hour.find(query={u"project.client.id": 1}, auto_lookup=3).limit(1).skip(0)


for row in cursor:
    print(row)

ret = hour.insert_one({
        u'issue': '',
        u'affected_to': {
            u"id": 1
        },
        u'project': {
            u"id": 1
        },
        u'started_at': datetime.datetime(2017, 12, 18, 14, 0),
        u'minutes': 333
}, auto_lookup=1)
print(ret.inserted_id)

ret = hour.delete_many({
    u"minutes": 333
})