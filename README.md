[![Build Status](https://travis-ci.org/knlambert/sqlcollection.svg?branch=master)](https://travis-ci.org/knlambert/sqlcollection)

# Purpose

This is a light wrapper built around SQLAlchemy. It gives you the ability to request
a SQL database with a MongoDB like API.

I've developed this to make it easier to build flexible REST Api around SQL databases.

# Usage

```python

import datetime
from sqlcollection import Client

# Connection string, there for MySQL.
client = Client(url=u'mysql://login:password@127.0.0.1/')

# Pick database user_api with table user.
user = client.user_api.user

# Fetch users with age greater than 12.
cursor = user.find(query={
    u"age": {
        u"$gte": 12
    }
)

# Pick database user_api with table teenager.
teenager = client.user_api.teenager

# For each user fetched before, insert the teneager
# with no update date.
for user in cursor:
    teenager.insert_one({
        u"name": user[u"name"],
        u"update_date": None
    })

# Delete teenagers with more than 18 years old 
# (Because obviously they are adults).
teenager.delete_many({
    u"age": {
        "$gte": 18
    }
})

# Put an update date for all users without one.
teenager.update_many({
    u"update_date": None
}, {
    u"$set": {
        u"update_date": datetime.datetime.now()
    }
})
```