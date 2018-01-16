# coding utf-8

from sqlcollection import Client


client = Client(url=u'mysql://root:localroot1234@127.0.0.1/')
client.user_api.user

client.user_api.list()