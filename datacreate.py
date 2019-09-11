
# coding: utf-8

# In[ ]:


import pandas as pd
import string
import random


# In[113]:


def generateID(number = 100000):
    ID = range(1,number+1)
    return ID


# In[114]:


def generateString(number = 100000, minNum = 10, maxNum = 20):
    Name = []
    for i in range(number):
        nameLen  = random.randint(minNum,maxNum)
        singleName = [random.choice(string.ascii_letters) for i in range(nameLen)]
        Name.append(''.join(singleName))
    return Name


# In[115]:


def generateMyPageNandCode(number = 100000):
    CountryCode = [random.randint(1,248) for _i in range(number)]
    Nation = country_code[0]CountryCode
    return Nation,CountryCode


# In[116]:


MyPageID = generateID(myPageLen)
MyName = generateString(myPageLen )
MyPageNationality, MyPageCountryCode = generateMyPageNandCode(myPageLen)
MyPageHobby = generateString(myPageLen)


# In[117]:


MyPage = pd.DataFrame([MyPageID, MyName, MyPageNationality, MyPageCountryCode, MyPageHobby]).T

MyPage.to_csv('MyPagetest.txt',  sep=',',index = False, header = False)


# The Friends dataset

# In[118]:


def generateIDandFriend(MyPageID, number = 20000000):
    PersonID = []
    MyFriend = []
    for i in range(number):
        randResult = random.sample(MyPageID, 2)
        PersonID.append(randResult[0])
        MyFriend.append(randResult[1])
    return PersonID, MyFriend


# In[119]:


def generateDate(start, end ,number = 20000000):
    DateofFriendship = []
    for i in range(number):
        df = random.randint(start, end)
        DateofFriendship.append(df)
    return DateofFriendship


# In[120]:


FriendRel = generateID(friendsLen)
PersonID,MyFriend = generateIDandFriend(MyPageID,friendsLen)
DateofFriendship = generateDate(1,1000000,friendsLen)
Desc = generateString(friendsLen, 20, 50)


# In[122]:


Friends = pd.DataFrame([FriendRel, PersonID, MyFriend, DateofFriendship, Desc]).T

Friends.to_csv('Friendstest.txt',sep=',', index = False, header = False)


# The AccessLog dataset

# In[123]:


def generateRandomNum(refList, number = 10000000):
    numList = []
    for i in range(number):
        randResult = random.choice(refList)
        numList.append(randResult)
    return numList


# In[124]:


AccessId = generateMyPageID(accessIdLen)
ByWho = generateRandomNum(MyPageID,accessIdLen)
WhatPage = generateRandomNum(MyPageID,accessIdLen)
TypeOfAccess = generateString(accessIdLen, 20, 50)
AccessTime = generateDate(1,1000000,accessIdLen)


# In[125]:


AccessTime = pd.DataFrame([AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime]).T

AccessTime.to_csv('AccessTimetest.txt',sep=',', index = False, header = False)


# In[90]:


len(TypeOfAccess)

