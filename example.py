# coding utf-8

from sqlcollection import Client

# import logging
# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

client = Client(url=u'mysql://root:localroot1234@127.0.0.1/')
result = client.hours_count.hour.find(lookup=[
    {
        u"to": u"hour",
        u"localField": u"project",
        u"from": u"project",
        u"foreignField": u"id",
        u"as": u"project"
    }, {
        u"to": u"project",
        u"localField": u"client",
        u"from": u"client",
        u"foreignField": u"id",
        u"as": u"project.client"
    }
])
for row in result:
    print(row)