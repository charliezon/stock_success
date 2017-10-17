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

  def __init__(self, cache_path):
    self._cache = load_cache(cache_path)
    self._cache_path = cache_path

    print('Load complete. Serving...')

  def get_result(self, e1, e2, e3, e4, profit_sign, turnover_sign, increase_sign, buy_or_follow=False, win_percent=0, lose_percent=0, lose_cache=0, days=30, force=False):

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
      print('Not exist in cache...')

    return { 'all_numerator':all_numerator, 'all_denominator':all_denominator, 'all_result':all_result,
             'profit_numerator':profit_numerator, 'profit_denominator':profit_denominator, 'profit_result':profit_result,
             'turnover_numerator':turnover_numerator, 'turnover_denominator':turnover_denominator, 'turnover_result':turnover_result,
             'increase_numerator':increase_numerator, 'increase_denominator':increase_denominator, 'increase_result':increase_result }

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

def load_cache(path):
  cache = {}
  if not os.path.exists(path):
    return cache
  with open(path, 'r') as f:
    cache = json.load(f)
  return cache

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
    'data/cache.json'
    ), server)

  server.add_insecure_port('[::]:50059')
  server.start()
  try:
    while True:
      time.sleep(_ONE_DAY_IN_SECONDS)
  except KeyboardInterrupt:
    server.stop(0)

if __name__ == '__main__':
  serve()