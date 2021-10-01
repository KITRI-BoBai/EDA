from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas as pd
import json
import time

def get_latest():

  url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
  parameters1 = {
    'start':'1',
    'limit':'5000',
    'convert':'USD'
  }

  parameters2 = {
    'start' : '5001',
    'limit' : '3000',
    'convert' : 'USD'
  }

  #API_KEY는 자신의 API키로 바꿔줘야 한다.
  headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': '7bf50af0-6214-40eb-8d63-90831fc465da',
  }

  session = Session()
  session.headers.update(headers)

  try:
    response1 = session.get(url, params=parameters1)
    response2 = session.get(url, params=parameters2)
    datas = json.loads(response1.text)
    datas2 = json.loads(response2.text)

    list_data = datas['data'] #'status'부분 빼고 data 배열 부분만 처리
    for data in datas2['data']:
      list_data.append(data)

  except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)
  
  return list_data

def list_chunk(lst, n):
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def info_id(id):
  url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/info'
  parameters = {
    #'slug':slug,
    #'limit':'249',
    #'convert':'USD'
    #'aux' : 'urls,description,logo'
    'id':id
  }

#API_KEY는 자신의 API키로 바꿔줘야 한다.
  headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': '7bf50af0-6214-40eb-8d63-90831fc465da',
  }

  session = Session()
  session.headers.update(headers)

  try:
    time.sleep(2) 
    response = session.get(url, params = parameters)
    data = json.loads(response.text)
#    print(data)
    return data
  except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)

#datas = datas['data']
def get_info(datas):
  id_list_all=[]        #6500개의 id를 일차원 list로 저장
  id_list_div=[]   #quote쿼리를 위해서 100개씩 id를 잘라서 저장
  info={}
  for i in range(len(datas)):
    id = datas[i]['id']
    id_list_all.append(id)

  id_input_div = list_chunk(id_list_all,100) #100개단위로 리스트 저장

  for i in range(len(id_input_div)): #100개씩 쿼리를 진행
    id=''
    data=[]
    #id_list_100 = str(id_input_div[i])
    for j in range(len(id_input_div[i])):
      id += str(id_input_div[i][j]) + ','

    if i == 0 :
      info =  info_id(id[0:-1])
    else :
      info_tmp = info_id(id[0:-1])
      for f in info_tmp['data'].keys():
        info['data'][f] = info_tmp['data'][f]

  return info
  
def datas_info_gather(datas,info):
  for i in range(len(datas)):
    id = str(datas[i]['id'])
    datas[i]['logo'] = info['data'][id]['logo']
    datas[i]['description'] = info['data'][id]['description']
    datas[i]['subreddit'] = info['data'][id]['subreddit']
    datas[i]['urls'] = info['data'][id]['urls']
    datas[i]['twitter_username'] = info['data'][id]['twitter_username']
  
  return datas


if __name__=='__main__':
  datas = get_latest()   # CoinmarketCap에 있는 코인들의 정보가 List 형태로 반환
  info =  get_info(datas)     # 추가적인 정보를 더해서 List반환
  datas2 = datas_info_gather(datas,info)
  df = pd.json_normalize(datas)
  df.to_csv("./result/coinmarketcap.csv",index=False)