import argparse
import json
import grpc

import analysis_pb2
import analysis_pb2_grpc

from decimal import Decimal as D
import math

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

parser = argparse.ArgumentParser(description='manual to this script')
parser.add_argument('--e1', type=bool, default=True)
parser.add_argument('--e2', type=bool, default=True)
parser.add_argument('--e3', type=bool, default=False)
parser.add_argument('--e4', type=bool, default=False)
parser.add_argument('--force', type=bool, default=False)
parser.add_argument('--buy_or_follow', type=bool, default=False, help='True: buy, False: follow')
parser.add_argument('--profit', type=float, default=0, help='The profit rate. If it is 10%, you should input 0.10')
parser.add_argument('--turnover', type=float, default=0, help='The turnover rate. If it is 10%, you should input 0.10')
parser.add_argument('--increase', type=float, default=0, help='The increase rate. If it is 10%, you should input 0.10')
parser.add_argument('--win_percent', type=float, default=0.034, help='The win_percent rate. If it is 10%, you should input 0.10')
parser.add_argument('--lose_percent', type=float, default=0.1, help='The lose_percent rate. If it is 10%, you should input 0.10')
parser.add_argument('--lose_cache', type=float, default=0.005, help='The lose_cache rate. If it is 10%, you should input 0.10')
parser.add_argument('--days', type=int, default=30, help='The maximum days for holding')
args = parser.parse_args()

e1 = args.e1
e2 = args.e2
e3 = args.e3
e4 = args.e4
force = args.force
buy_or_follow = args.buy_or_follow
profit = args.profit
turnover = args.turnover
increase = args.increase
win_percent = args.win_percent
lose_percent = args.lose_percent
lose_cache = args.lose_cache
days = args.days

print('沪指位于20日均线以上：%s' % ('是' if e1 else '否'))
print('深指位于20日均线以上：%s' % ('是' if e2 else '否'))
print('沪深重心都上升：%s' % ('是' if e3 else '否'))
print('沪深都收阳且收盘价都大于前一日开收盘价之大者：%s' % ('是' if e4 else '否'))
print('强制重新计算：%s' % ('是' if force else '否'))
print('目标指标：%s' % ('买入' if buy_or_follow else '追涨'))
print('获利比例：%s' % profit)
print('换手率：%s' % turnover)
print('涨幅：%s' % increase)
print('止盈率：%s' % win_percent)
print('止损率：%s' % lose_percent)
print('止损缓冲：%s' % lose_cache)
print('最大持股天数：%s天' % days)

channel = grpc.insecure_channel('localhost:50059')
stub = analysis_pb2_grpc.AnalysisStub(channel)
response = stub.RequireAnalysis(analysis_pb2.AnalysisRequest(
    e1=e1,
    e2=e2,
    e3=e3,
    e4=e4,
    force=force,
    buy_or_follow=buy_or_follow,
    profit=profit,
    turnover=turnover,
    increase=increase,
    win_percent=win_percent,
    lose_percent=lose_percent,
    lose_cache=lose_cache,
    days=days ))

status = []

if response.e1:
    status.append('E1')
else:
    status.append('not E1')

if response.e2:
    status.append('E2')
else:
    status.append('not E2')

if response.e3:
    status.append('E3')
else:
    status.append('not E3')

if response.e4:
    status.append('E4')
else:
    status.append('not E4')

status.append(response.profit_sign)
status.append(response.turnover_sign)
status.append(response.increase_sign)

print(' and '.join(status))
print('综合成功率: %s / %s = %s' % (response.all_numerator, response.all_denominator, round_float(response.all_result, 4)))
print('获利成功率: %s / %s = %s' % (response.profit_numerator, response.profit_denominator, round_float(response.profit_result, 4)))
print('换手成功率: %s / %s = %s' % (response.turnover_numerator, response.turnover_denominator, round_float(response.turnover_result, 4)))
print('涨幅成功率: %s / %s = %s' % (response.increase_numerator, response.increase_denominator, round_float(response.increase_result, 4)))