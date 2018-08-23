# coding utf-8

from sqlcollection import Client


client = Client(url=u'postgres://postgres:postgresql@127.0.0.1/')
user = client.user_api._user


cursor = user.find(query={}, auto_lookup=1).sort(u"id").limit(2).skip(0)
for item in cursor:
    print(item)
