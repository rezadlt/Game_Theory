#!/usr/bin/env python
# coding: utf-8

# ## Prepare Environment

# In[2]:


# !pip3 install pymongo
# !pip3 install mongoengine
# !pip3 install Faker
# !pip3 install mongomock


# In[3]:


# %load_ext nb_black
# %load_ext autoreload
# %autoreload 2


# In[4]:


# Prepare environment for importing from src
import sys
import os

sys.path.insert(0, "..")


# ## Import Dependencies 

# In[5]:


import random
import datetime

from mongoengine import connect, get_connection

from src.data import initialize_db
from src.utils import drop_db


# ## Connect to Mock DB

# In[6]:


from pymongo import MongoClient

client = connect("assignment", host="mongodb://127.0.0.1:27017")


# In[7]:


if not os.environ.get("TEST"):
    drop_db(client, "assignment")


# ## Generate Fake Data & Insert Them to DB

# In[ ]:


if not os.environ.get("TEST"):
    initialize_db()


# In[ ]:


print(client.assignment.list_collection_names())
print(client.assignment.patient.find_one())


# ## Examples

# In[ ]:


print(client.assignment.drug.find_one({"formula": "CH3COOH"}))


# In[ ]:





# In[ ]:


list(
    client.assignment.drug.aggregate(
        [{"$group": {"_id": "$formula", "count": {"$sum": 1}}}]
    )
)


# In[ ]:





# In[ ]:


client.assignment.patient.aggregate(
    [
        {
            "$lookup": {
                "from": "doctor",
                "localField": "doctor_id",
                "foreignField": "_id",
                "as": "doctor",
            }
        },
        {"$match": {"doctor.first_name": "Robert"}},
        {"$count": "patients"},
    ]
).next()


# In[ ]:





# ## Query Assignments

# In[ ]:


# نام داروخانه‌هایی که شماره تلفن آنها با 1+ شروع می‌شود
print("##1##")
a1 = list(
    client.assignment.pharmacy.find(
        filter={'telephone':{'$regex': '\+1*'}},  # Complete the filter
        projection={"name": 1, "_id": 0},
    )
)
print(a1)


# In[ ]:





# In[ ]:


#   متولد شده‌اند datetime.datetime(2000, 1, 1, 0, 0) شماره ملی افرادی که بعد از تاریخ 
print("##2##")
a2 = list(
    client.assignment.patient.find(
        filter={"birthdate" : {'$gt' : datetime.datetime(2000, 1, 1, 0, 0)}},  # Complete the filter
        projection={"national_id": 1, "_id": 0},
    )
)
print(a2)


# In[ ]:





# In[ ]:


# تعداد نسخه هایی که دارای حداقل 15 دارو هستند
print("##3##")
a3 = client.assignment.prescription.find(
    filter={'items': {'$exists': True}, '$where':'this.items.length > 15'}  # Complete the filter
).count()
print(a3)


# In[ ]:





# In[ ]:


#    است "Robert" کد ملی بیمارانی که اسم پزشک آنها
print("##4##")
a4 = list(
    client.assignment.patient.aggregate(
        [          
            {
                "$lookup": {
                    "from": "doctor",
                    "localField": "doctor_id",
                    "foreignField": "_id",
                    "as": "doctor",
                }
            },
            {"$match": {"doctor.first_name": "Robert"}},
            {'$project': {'_id': 0, 'national_id': 1}}
        ]
    )
)
print(a4)


# In[ ]:





# In[ ]:


#    نام داروخانه‌ای که دارویی به گرانترین قیمت به آن فروخته‌شده است
print("##5##")
a5 = client.assignment.sale.aggregate(
    [  
        {
            '$sort': {'price': -1}
        },
        {
            '$lookup': {
                'from': 'pharmacy',
                'localField': 'pharmacy_id',
                'foreignField': '_id',
                'as': 'pharmacy'
            }
        },
        {
            '$unwind': '$pharmacy'
        },
        {
            '$replaceRoot': {'newRoot': '$pharmacy'}
        },
        {
            '$project': {
                '_id': 0,
                'name': 1
            }
        }
    ]
).next()
print(a5)


# In[ ]:





# In[ ]:


#   نام و فرمول پنج دارویی که گران ترین قیمت برای آنها ثبت شده است  
print("##6##")
a6 = list(
    client.assignment.sale.aggregate(
        [
            {
                '$sort': {'price': -1}
            },
            {
                '$lookup': {
                    'from': 'drug',
                    'localField': 'drug_id',
                    'foreignField': '_id',
                    'as': 'drug'
                }
            },
            {
                '$unwind': '$drug'
            },
            {
                '$replaceRoot': {'newRoot': '$drug'}
            },
            {
                '$project': {
                    '_id': 0,
                    'name': 1,
                    'formula': 1
                }
            },
            {   '$limit' : 5 }
        ]
    )
)
print(a6)


# In[ ]:





# In[ ]:


#   تجویز شده اند datetime.datetime(2020, 9, 23, 0, 0) نام تمام داروهایی که در تاریخ 
print("##7##")
a7 = list(
    client.assignment.prescription.aggregate(
        [
            {
                '$match': {'date': datetime.datetime(2020, 9, 23, 0, 0)}
            },
            {
                '$lookup': {
                    'from': 'drug',
                    'localField': 'items.drug_id',
                    'foreignField': '_id',
                    'as': 'drug'
                }
            },
            {
                '$unwind': '$drug'
            },
            {
                '$replaceRoot': {'newRoot': '$drug'}
            },
            {
                '$project': {
                    '_id': 0,
                    'name': 1,
                }
            },
        ]
    )
)
print(a7)


# In[ ]:





# In[ ]:


#    را تولید می کنند "C2H6Na4O12" نام تمام کارخانه هایی که داروی با فرمول 
print("##8##")
a8 = list(
    client.assignment.drug.aggregate(
        [
            {
                '$match': {'formula': 'C2H6Na4O12'}
            },
            {
                '$lookup': {
                    'from': 'company',
                    'localField': 'company_id',
                    'foreignField': '_id',
                    'as': 'company'
                }
            },
            {
                '$unwind': '$company'
            },
            {
                '$replaceRoot': {'newRoot': '$company'}
            },
            {
                '$project': {
                    '_id': 0,
                    'name': 1,
                }
            }
        ]
    )
)
print(a8)


# In[ ]:





# In[ ]:


#   وجود دارد BasketItem کاربرانی که در سبد آنها حداقل ده 
print("##9##")
a9 = list(
    client.assignment.user.find(
        filter={'basket': {'$size'< 10}},  # Complete the filter
        projection={"email": 1, "_id": 0},
    )
)
print(a9)


# In[ ]:





# In[ ]:


#   هستند "XL" اجناس که سایز (sum quantity)میزان موجودی
print("##10##")
a10 = client.assignment.product_item.aggregate(
    [
        { 
            '$match': {'size': 'XL'}
        },
        {
            '$group': {'_id': None, 'sum': {'$sum': '$quantity'} }
        },
        {
            '$project': {'_id': 0}
        }
    ]
).next()
print(a10)


# In[ ]:





# In[ ]:


#  شماره ملی رانندگانی که پلاک آنها به 25 ختم می شود  
print("##11##")
a11 = list(
    client.assignment.driver.find(
        filter={'license_plate': {'$regex': '.+25$'}},  # Complete the filter
        projection={"_id": 0, "national_id": 1},
    )
)
print(a11)


# In[ ]:





# In[ ]:


# در آن وجود دارد "great" و یا "good" متن کامنت هایی که کلمه 

print("##12##")
a12 = list(
    client.assignment.comment.find(
        filter={'text': {'$regex': '.*(good|great).*'}},
        projection={"_id": 0, "text": 1},
    )
)
print(a12)


# In[ ]:





# In[ ]:


# تعداد نظراتی که ریتینگ آن‌ها 5 است
print("##13##")
a13 = client.assignment.comment.aggregate(
    [
        { "$match": {
                "rating": 5
            }
        },
        {
            "$count": "count"
        }
    ]
).next()
print(a13)


# In[ ]:





# In[ ]:


answers = {
    "a1": a1,
    "a2": a2,
    "a3": a3,
    "a4": a4,
    "a5": a5,
    "a6": a6,
    "a7": a7,
    "a8": a8,
    "a9": a9,
    "a10": a10,
    "a11": a11,
    "a12": a12,
    "a13": a13,
}


# In[ ]:


import json
with open("answers.json", "r") as json_file:
    target = json.load(json_file)


# In[ ]:


correct = 0
for i in range(1, 14):
    if answers["a{}".format(i)] == target["a{}".format(i)]:
        print("Query {:2d} Correct!".format(i))
        correct += 1
    else:
        print("Query {:2d} Wrong!".format(i))
print(correct)


# ## Print Result to File  

# In[ ]:


# Set your student number
student_number = 97110411
file_path = os.path.join(
    os.getenv("OUTPUT_DIR", "."), "{}.json".format(student_number)
)
with open(file_path, "w") as file:
    corrects = []
    wrongs = []
    for i in range(1, 14):
        if answers["a{}".format(i)] == target["a{}".format(i)]:
            corrects.append(i)
        else:
            wrongs.append(i)
    json.dump({"corrects": corrects, "wrongs": wrongs, "score": len(corrects)}, file)


# In[ ]:




