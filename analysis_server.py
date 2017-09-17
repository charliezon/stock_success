from concurrent import futures
import time

import grpc

import analysis_pb2
import analysis_pb2_grpc

import os
import json

from decimal import Decimal as D
import math

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

def round_float(g, pos=2):
    if g<0:
        f = -g
    else:
        f = g
    p1 = pow(D('10'), D(str(pos+1)))
    last = D(str(int(D(str(f))*p1)))%D('10')
    p = pow(D('10'), D(str(pos)))
    if last >= 5:
        result = float(math.ceil(D(str(f))*p)/p)
    else:
        result = float(math.floor(D(str(f))*p)/p)
    if g<0:
        return -result
    else:
        return result

class AnalysisServer(analysis_pb2_grpc.AnalysisServicer):

  def __init__(self, cache_path, content_path, raw_path, shanghai_index_path, shenzhen_index_path):
    self._cache = load_cache(cache_path)
    self._cache_path = cache_path

    self._contents = load_contents(content_path, raw_path)
    self._raw_path = raw_path
    self._content_path = content_path

    self._shanghai_data, self._shanghai_date_list = load_index_data(shanghai_index_path)
    self._shenzhen_data, self._shenzhen_date_list = load_index_data(shenzhen_index_path)

    self._shanghai_index_path = shanghai_index_path
    self._shenzhen_index_path = shenzhen_index_path

    print('Load complete. Serving...')

  def get_result(self, e1, e2, e3, e4, profit_sign, turnover_sign, increase_sign, buy_or_follow=False, win_percent=0, lose_percent=0, lose_cache=0, days=30, force=False):
    shanghai_date_list = self._shanghai_date_list
    shenzhen_date_list = self._shenzhen_date_list
    shanghai_data = self._shanghai_data
    shenzhen_data = self._shenzhen_data

    status_str = get_status_str(e1, e2, e3, e4, profit_sign=profit_sign, turnover_sign=turnover_sign, increase_sign=increase_sign)
    status_str = ' and '.join([status_str, ('buy' if buy_or_follow else 'follow'), str(win_percent), str(lose_percent), str(lose_cache), str(days)])

    status_str1 = get_status_str(e1, e2, e3, e4, profit_sign=profit_sign)
    status_str1 = ' and '.join([status_str1, ('buy' if buy_or_follow else 'follow'), str(win_percent), str(lose_percent), str(lose_cache), str(days)])

    status_str2 = get_status_str(e1, e2, e3, e4, turnover_sign=turnover_sign)
    status_str2 = ' and '.join([status_str2, ('buy' if buy_or_follow else 'follow'), str(win_percent), str(lose_percent), str(lose_cache), str(days)])

    status_str3 = get_status_str(e1, e2, e3, e4, increase_sign=increase_sign)
    status_str3 = ' and '.join([status_str3, ('buy' if buy_or_follow else 'follow'), str(win_percent), str(lose_percent), str(lose_cache), str(days)])

    if not force and status_str in self._cache.keys() and status_str1 in self._cache.keys() and status_str2 in self._cache.keys() and status_str3 in self._cache.keys():
      all_numerator = self._cache[status_str]['numerator']
      all_denominator = self._cache[status_str]['denominator']
      all_result = self._cache[status_str]['result']
      profit_numerator = self._cache[status_str1]['numerator']
      profit_denominator = self._cache[status_str1]['denominator']
      profit_result = self._cache[status_str1]['result']
      turnover_numerator = self._cache[status_str2]['numerator']
      turnover_denominator = self._cache[status_str2]['denominator']
      turnover_result = self._cache[status_str2]['result']
      increase_numerator = self._cache[status_str3]['numerator']
      increase_denominator = self._cache[status_str3]['denominator']
      increase_result = self._cache[status_str3]['result']
      print('Using cache.')
    else:
      print('Calculating...')
      data = []
      data1 = []
      data2 = []
      data3 = []
      for content in self._contents:
        for i in range(len(content)):
          date = content[i][0]
          open_price = content[i][1]
          high_price = content[i][2]
          low_price = content[i][3]
          close_price = content[i][4]
          volume = content[i][5]

          winner = content[i][7]
          turnover = content[i][70]
          increase = content[i][39]

          buy = int(content[i][101])
          follow = int(content[i][102])

          if i + days + 1 < len(content) and ((buy_or_follow and buy == 1) or ((not buy_or_follow) and follow == 1)):
            new_open_price = content[i+1][1]
            win_price = round_float(new_open_price * (1+win_percent))
            lose_price = round_float(new_open_price * (1-lose_percent+lose_cache))
            success = 0
            for j in range(i+2, i+ days + 2):
              new_high_price = content[j][2]
              new_low_price = content[j][3]
              if new_low_price <= lose_price:
                success = 0
                break
              if new_high_price >= win_price:
                success = 1
                break

            if date in shanghai_date_list and date in shenzhen_date_list:
              pre_date = shanghai_date_list[shanghai_date_list.index(date) - 1]
              if not (pre_date in shanghai_date_list and pre_date in shenzhen_date_list):
                continue
            else:
              continue

            signs = {}

            signs['E1'] = (date in shanghai_data) and (shanghai_data[date][4]>shanghai_data[date][8])
            signs['E2'] = (date in shenzhen_data) and (shenzhen_data[date][4]>shenzhen_data[date][8])
            signs['E3'] = shanghai_data[pre_date][1]+shanghai_data[pre_date][4]<=shanghai_data[date][1]+shanghai_data[date][4] and shenzhen_data[pre_date][1]+shenzhen_data[pre_date][4]<=shenzhen_data[date][1]+shenzhen_data[date][4]
            signs['E4'] = shanghai_data[date][4]>shanghai_data[date][1] and shenzhen_data[date][4]>shenzhen_data[date][1] and shanghai_data[date][4]>max(shanghai_data[pre_date][1], shanghai_data[pre_date][4]) and shenzhen_data[date][4]>max(shenzhen_data[pre_date][1], shenzhen_data[pre_date][4])

            signs['E5'] = winner is not None and winner > 0.9
            signs['E6'] = winner is not None and winner > 0.6 and winner <= 0.9
            signs['E7'] = winner is not None and winner > 0.3 and winner <= 0.6

            signs['E81'] = winner is not None and winner > 0.25 and winner <= 0.3
            signs['E82'] = winner is not None and winner > 0.2 and winner <= 0.25
            signs['E83'] = winner is not None and winner > 0.15 and winner <= 0.2
            signs['E84'] = winner is not None and winner > 0.1 and winner <= 0.15
            signs['E85'] = winner is not None and winner > 0.05 and winner <= 0.1
            signs['E86'] = winner is not None and winner >= 0 and winner <= 0.05

            signs['E9'] = turnover is not None and turnover <= 0.005
            signs['E10'] = turnover is not None and turnover > 0.005 and turnover <= 0.01
            signs['E11'] = turnover is not None and turnover > 0.01 and turnover <= 0.03
            signs['E12'] = turnover is not None and turnover > 0.03 and turnover <= 0.05
            signs['E13']  = turnover is not None and turnover > 0.05 and turnover <= 0.10
            signs['E14']  = turnover is not None and turnover > 0.10 and turnover <= 0.20
            signs['E15']  = turnover is not None and turnover > 0.20

            signs['E16']  = increase is not None and increase <= 0
            signs['E17']  = increase is not None and increase > 0 and increase <= 0.02
            signs['E18']  = increase is not None and increase > 0.02 and increase <= 0.04
            signs['E19']  = increase is not None and increase > 0.04 and increase <= 0.06
            signs['E20']  = increase is not None and increase > 0.06 and increase <= 0.09
            signs['E21']  = increase is not None and increase > 0.09

            if (((e1 and e2 and e3 and (not e4)) and signs['E1'] and signs['E2'] and signs['E3']) or ((e1 and e2 and (not e3) and e4) and signs['E1'] and signs['E2'] and signs['E4']) or (e1 == signs['E1'] and e2 == signs['E2'] and e3 == signs['E3'] and e4 == signs['E4'])):
              if ((profit_sign is None or (profit_sign is not None and signs[profit_sign])) and (turnover_sign is None or (turnover_sign is not None and signs[turnover_sign])) and (increase_sign is None or (increase_sign is not None and signs[increase_sign]))):
                data.append(success)
              if profit_sign is not None and signs[profit_sign]:
                data1.append(success)
              if turnover_sign is not None and signs[turnover_sign]:
                data2.append(success)
              if increase_sign is not None and signs[increase_sign]:
                data3.append(success)

      all_numerator, all_denominator, all_result = self.succ(data)
      self._cache[status_str] = {'numerator': all_numerator, 'denominator': all_denominator, 'result': all_result}

      profit_numerator, profit_denominator, profit_result = self.succ(data1)
      self._cache[status_str1] = {'numerator': profit_numerator, 'denominator': profit_denominator, 'result': profit_result}

      turnover_numerator, turnover_denominator, turnover_result = self.succ(data2)
      self._cache[status_str2] = {'numerator': turnover_numerator, 'denominator': turnover_denominator, 'result': turnover_result}

      increase_numerator, increase_denominator, increase_result = self.succ(data3)
      self._cache[status_str3] = {'numerator': increase_numerator, 'denominator': increase_denominator, 'result': increase_result}

      save_cache(self._cache_path, self._cache)

    return { 'all_numerator':all_numerator, 'all_denominator':all_denominator, 'all_result':all_result,
             'profit_numerator':profit_numerator, 'profit_denominator':profit_denominator, 'profit_result':profit_result,
             'turnover_numerator':turnover_numerator, 'turnover_denominator':turnover_denominator, 'turnover_result':turnover_result,
             'increase_numerator':increase_numerator, 'increase_denominator':increase_denominator, 'increase_result':increase_result }

  def succ(self, data):
    num_success = 0
    for i in range(0, len(data)):
      num_success += data[i]
    if len(data) > 0:
      numerator = num_success
      denominator = len(data)
      result = num_success/len(data)
    else:
      numerator = 0
      denominator = 0
      result = 0
    return numerator, denominator, result

  def RequireAnalysis(self, request, context):

    e1 = request.e1
    e2 = request.e2
    e3 = request.e3
    e4 = request.e4
    force = request.force
    buy_or_follow = request.buy_or_follow
    profit_rate = request.profit
    turnover_rate = request.turnover
    increase_rate = request.increase
    win_percent = request.win_percent
    lose_percent = request.lose_percent
    lose_cache = request.lose_cache
    days = request.days

    profit_sign, turnover_sign, increase_sign = get_signs(profit_rate, turnover_rate, increase_rate)

    result = self.get_result(e1, e2, e3, e4, profit_sign=profit_sign, turnover_sign=turnover_sign, increase_sign=increase_sign, buy_or_follow=buy_or_follow, win_percent=win_percent, lose_percent=lose_percent, lose_cache=lose_cache, days=days, force=force)

    return analysis_pb2.AnalysisReply(
      e1=e1,
      e2=e2,
      e3=e3,
      e4=e4,
      buy_or_follow=buy_or_follow,
      all_numerator=result['all_numerator'],
      all_denominator=result['all_denominator'],
      all_result=result['all_result'],
      profit_numerator=result['profit_numerator'],
      profit_denominator=result['profit_denominator'],
      profit_result=result['profit_result'],
      turnover_numerator=result['turnover_numerator'],
      turnover_denominator=result['turnover_denominator'],
      turnover_result=result['turnover_result'],
      increase_numerator=result['increase_numerator'],
      increase_denominator=result['increase_denominator'],
      increase_result=result['increase_result'],
      profit_sign=profit_sign,
      turnover_sign=turnover_sign,
      increase_sign=increase_sign )

def process_index(path):
  data = {}
  content = []
  with open(path, 'r', encoding='gbk') as f:
    i = 0
    # 前35行数据不要
    for line in f.readlines():
      line_data = line.strip().split('\t')
      if i > 35 and len(line_data) == 10:
        item = []
        item.append(line_data[0].strip())
        for j in range(1, len(line_data)):
          if line_data[j].strip() == '':
            item.append(None)
          else:
            item.append(float(line_data[j].strip()))
        content.append(item)
      i += 1

  for i in range(len(content)):
    date = content[i][0]
    open_price = content[i][1]
    high_price = content[i][2]
    low_price = content[i][3]
    close_price = content[i][4]
    volume = content[i][5]

    ma1 = content[i][6]
    ma2 = content[i][7]
    ma3 = content[i][8]
    ma4 = content[i][9]

    data_item = []
    data_item.append(date)
    data_item.append(open_price)
    data_item.append(high_price)
    data_item.append(low_price)
    data_item.append(close_price)
    data_item.append(volume)
    data_item.append(ma1)
    data_item.append(ma2)
    data_item.append(ma3)
    data_item.append(ma4)

    data[date] = data_item
  return data

def load_cache(path):
  cache = {}
  if not os.path.exists(path):
    return cache
  with open(path, 'r') as f:
    cache = json.load(f)
  return cache

def save_cache(path, cache):
  with open(path, 'w') as f:
    f.write(json.dumps(cache))

def load_contents(content_path, raw_path):
    if not os.path.exists(content_path):
      print('Loading content from txt...')
      path = os.path.abspath(raw_path)
      contents = []
      for x in os.listdir(path):
        new_path = os.path.join(path, x)
        if os.path.isdir(new_path):
          content = load_contents(new_path)
          contents.extend(content)
        elif os.path.isfile(new_path) and os.path.splitext(x)[1]=='.txt':
          content = process_file(new_path)
          contents.append(content)
      with open(content_path, 'w') as f:
        f.write(json.dumps(contents))
    else:
      print('Loading content from json...')
      with open(content_path, 'r') as f:
        contents = json.load(f)
    return contents

def process_file(path):
  print(path)
  content = []
  name = None
  with open(path, 'r', encoding='gbk') as f:
    i = 0
    # 前35行数据不要
    for line in f.readlines():
      line_data = line.strip().split('\t')
      if i == 0:
        name = line_data
      if i > 35 and len(line_data) == 108:
        item = []
        item.append(line_data[0].strip())
        for j in range(1, len(line_data)):
          if line_data[j].strip() == '':
            item.append(None)
          else:
            item.append(float(line_data[j].strip()))
        content.append(item)
      i += 1
  return content

def load_index_data(path):
  data = process_index(path)
  date_list = sorted(data.keys())
  return data, date_list

def get_signs(profit_rate, turnover_rate, increase_rate):
  profit_sign = None
  profit_sign = 'E5' if profit_rate is not None and profit_rate > 0.9 else profit_sign
  profit_sign = 'E6' if profit_rate is not None and profit_rate > 0.6 and profit_rate <= 0.9 else profit_sign
  profit_sign = 'E7' if profit_rate is not None and profit_rate > 0.3 and profit_rate <= 0.6 else profit_sign
  profit_sign = 'E81' if profit_rate is not None and profit_rate > 0.25 and profit_rate <= 0.3 else profit_sign
  profit_sign = 'E82' if profit_rate is not None and profit_rate > 0.2 and profit_rate <= 0.25 else profit_sign
  profit_sign = 'E83' if profit_rate is not None and profit_rate > 0.15 and profit_rate <= 0.2 else profit_sign
  profit_sign = 'E84' if profit_rate is not None and profit_rate > 0.1 and profit_rate <= 0.15 else profit_sign
  profit_sign = 'E85' if profit_rate is not None and profit_rate > 0.05 and profit_rate <= 0.1 else profit_sign
  profit_sign = 'E86' if profit_rate is not None and profit_rate >= 0 and profit_rate <= 0.05 else profit_sign

  turnover_sign = None
  turnover_sign = 'E9' if turnover_rate is not None and turnover_rate <= 0.005 else turnover_sign
  turnover_sign = 'E10' if turnover_rate is not None and turnover_rate > 0.005 and turnover_rate <= 0.01 else turnover_sign
  turnover_sign = 'E11' if turnover_rate is not None and turnover_rate > 0.01 and turnover_rate <= 0.03 else turnover_sign
  turnover_sign = 'E12' if turnover_rate is not None and turnover_rate > 0.03 and turnover_rate <= 0.05 else turnover_sign
  turnover_sign = 'E13' if turnover_rate is not None and turnover_rate > 0.05 and turnover_rate <= 0.10 else turnover_sign
  turnover_sign = 'E14' if turnover_rate is not None and turnover_rate > 0.10 and turnover_rate <= 0.20 else turnover_sign
  turnover_sign = 'E15' if turnover_rate is not None and turnover_rate > 0.20 else turnover_sign

  increase_sign = None
  increase_sign = 'E16' if increase_rate is not None and increase_rate <= 0 else increase_sign
  increase_sign = 'E17' if increase_rate is not None and increase_rate > 0 and increase_rate <= 0.02 else increase_sign
  increase_sign = 'E18' if increase_rate is not None and increase_rate > 0.02 and increase_rate <= 0.04 else increase_sign
  increase_sign = 'E19' if increase_rate is not None and increase_rate > 0.04 and increase_rate <= 0.06 else increase_sign
  increase_sign = 'E20' if increase_rate is not None and increase_rate > 0.06 and increase_rate <= 0.09 else increase_sign
  increase_sign = 'E21' if increase_rate is not None and increase_rate > 0.09 else increase_sign
  return profit_sign, turnover_sign, increase_sign

def get_status_str(e1, e2, e3, e4, profit_sign=None, turnover_sign=None, increase_sign=None):
  status = []
  if (e1 and e2 and e3 and (not e4)):
    status = ['E1', 'E2', 'E3']
  elif (e1 and e2 and (not e3) and e4):
    status = ['E1', 'E2', 'E4']
  else:
    if e1:
        status.append('E1')
    else:
        status.append('not E1')

    if e2:
        status.append('E2')
    else:
        status.append('not E2')

    if e3:
        status.append('E3')
    else:
        status.append('not E3')

    if e4:
        status.append('E4')
    else:
        status.append('not E4')

  if not (profit_sign is None):
    status.append(profit_sign)
  if not (turnover_sign is None):
    status.append(turnover_sign)
  if not (increase_sign is None):
    status.append(increase_sign)

  return ' and '.join(status)

def serve():

  server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
  analysis_pb2_grpc.add_AnalysisServicer_to_server(AnalysisServer(
    '/home/fcloud/workspace/analysis/data/20170831/cache.json',
    '/home/fcloud/workspace/analysis/data/20170831/contents.json',
    '/home/fcloud/workspace/analysis/data/20170831//raw',
    '/home/fcloud/workspace/analysis/data/20170831/999999.txt',
    '/home/fcloud/workspace/analysis/data/20170831/399001.txt'
    ), server)

  # analysis_pb2_grpc.add_AnalysisServicer_to_server(AnalysisServer(
  #   'F:/项目/StockMining/data_for_dl/data/data_buy_follow_index_1/cache.json',
  #   'F:/项目/StockMining/data_for_dl/data/data_buy_follow_index_1/contents.json',
  #   'F:/项目/StockMining/data_for_dl/data/data_buy_follow_index_1/raw',
  #   'F:/项目/StockMining/data_for_dl/data/data_buy_follow_index_1/999999.txt',
  #   'F:/项目/StockMining/data_for_dl/data/data_buy_follow_index_1/399001.txt'
  #   ), server)

  server.add_insecure_port('[::]:50059')
  server.start()
  try:
    while True:
      time.sleep(_ONE_DAY_IN_SECONDS)
  except KeyboardInterrupt:
    server.stop(0)

if __name__ == '__main__':
  serve()