# coding utf-8

from sqlcollection import Client

client = Client(url=u'mysql://root:localroot1234@127.0.0.1/')
result = client.hours_count.project.find(lookup={
    u"to": u"project",
    u"localField": u"client_id",
    u"from": u"client",
    u"foreignField": u"id",
    u"as": u"client"
})

for row in result:
    print(row)