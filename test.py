#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Chaoliang Zhong'

import math
from decimal import Decimal as D
import os
import csv
import random
from operator import itemgetter, attrgetter

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

def process_file(path):
    data = []
    data1 = []
    data2 = []
    data3 = []
    data4 = []
    data12 = []
    data13 = []
    data14 = []
    data23 = []
    data24 = []
    data34 = []
    data123 = []
    data124 = []
    data134 = []
    data234 = []
    data1234 = []
    data1304 = []

    content = []
    name = None
    with open(path, 'r') as f:
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

        if i + days + 1 < len(content) and follow == 1:
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

            #if (success == 0 or success == 1) and (date in shanghai_data) and (shanghai_data[date][4]>shanghai_data[date][8]) and (date in shenzhen_data) and (shenzhen_data[date][4]>shenzhen_data[date][8] and shanghai_data[pre_date][1]+shanghai_data[pre_date][4]<=shanghai_data[date][1]+shanghai_data[date][4] and shenzhen_data[pre_date][1]+shenzhen_data[pre_date][4]<=shenzhen_data[date][1]+shenzhen_data[date][4]):
            #if (success == 0 or success == 1) and (date in shanghai_data) and (shanghai_data[date][4]>shanghai_data[date][8]) and (date in shenzhen_data) and (shenzhen_data[date][4]>shenzhen_data[date][8]):
            #if (success == 0 or success == 1) and (date in shanghai_data) and (shanghai_data[date][4]>shanghai_data[date][8]) and (date in shenzhen_data) and (shenzhen_data[date][4]>shenzhen_data[date][8] and close_price/content[i-1][4]<1+increase):
            e1 = (date in shanghai_data) and (shanghai_data[date][4]>shanghai_data[date][8])
            e2 = (date in shenzhen_data) and (shenzhen_data[date][4]>shenzhen_data[date][8])
            e3 = shanghai_data[pre_date][1]+shanghai_data[pre_date][4]<=shanghai_data[date][1]+shanghai_data[date][4] and shenzhen_data[pre_date][1]+shenzhen_data[pre_date][4]<=shenzhen_data[date][1]+shenzhen_data[date][4]
            e4 = shanghai_data[date][4]>shanghai_data[date][1] and shenzhen_data[date][4]>shenzhen_data[date][1] and shanghai_data[date][4]>max(shanghai_data[pre_date][1], shanghai_data[pre_date][4]) and shenzhen_data[date][4]>max(shenzhen_data[pre_date][1], shenzhen_data[pre_date][4])

            e5 = winner is not None and winner > 0.9
            e6 = winner is not None and winner > 0.6 and winner <= 0.9
            e7 = winner is not None and winner > 0.3 and winner <= 0.6
            e8 = winner is not None and winner <= 0.3

            e81 = winner is not None and winner > 0.25 and winner <= 0.3
            e82 = winner is not None and winner > 0.2 and winner <= 0.25
            e83 = winner is not None and winner > 0.15 and winner <= 0.2
            e84 = winner is not None and winner > 0.1 and winner <= 0.15
            e85 = winner is not None and winner > 0.05 and winner <= 0.1
            e86 = winner is not None and winner >= 0 and winner <= 0.05

            e9 = turnover is not None and turnover <= 0.005
            e10 = turnover is not None and turnover > 0.005 and turnover <= 0.01
            e11 = turnover is not None and turnover > 0.01 and turnover <= 0.03
            e12 = turnover is not None and turnover > 0.03 and turnover <= 0.05
            e13 = turnover is not None and turnover > 0.05 and turnover <= 0.10
            e14 = turnover is not None and turnover > 0.10 and turnover <= 0.20
            e15 = turnover is not None and turnover > 0.20

            e16 = increase is not None and increase <= 0
            e17 = increase is not None and increase > 0 and increase <= 0.02
            e18 = increase is not None and increase > 0.02 and increase <= 0.04
            e19 = increase is not None and increase > 0.04 and increase <= 0.06
            e20 = increase is not None and increase > 0.06 and increase <= 0.09
            e21 = increase is not None and increase > 0.09

            # if (success == 0 or success == 1) and (date in shanghai_data) and (shanghai_data[date][4]>shanghai_data[date][8]):
            # #if (success == 0 or success == 1):
            #     #buy_dates.add(date)
            #     data_item = []
            #     #data_item.append(date)
            #     #data_item.append(round_float(close_price/content[i-1][4]))
            #     #data_item.append(name)
            #     #data_item.append(content[i-1][4])
            #     #data_item.append(close_price)
            #     data_item.append(success)
            #     data.append(data_item)
            if e20:
                data.append([success])
                if e1:
                    data1.append([success])
                if e2:
                    data2.append([success])
                if e3:
                    data3.append([success])
                if e4:
                    data4.append([success])
                if e1 and e2:
                    data12.append([success])
                if e1 and e3:
                    data13.append([success])
                if e1 and e4:
                    data14.append([success])
                if e2 and e3:
                    data23.append([success])
                if e2 and e4:
                    data24.append([success])
                if e3 and e4:
                    data34.append([success])
                if e1 and e2 and e3:
                    data123.append([success])
                if e1 and e2 and e4:
                    buy_dates.add(date)
                    data124.append([success])
                if e1 and e3 and e4:
                    data134.append([success])
                if e2 and e3 and e4:
                    data234.append([success])
                if e1 and e2 and e3 and e4:
                    data1234.append([success])
                if e1 and (e3 or e4):
                    data1304.append([success])

    return data, data1, data2, data3, data4, data12, data13, data14, data23, data24, data34, data123, data124, data134, data234, data1234, data1304

def process_index(path):
    data = {}
    content = []
    with open(path, 'r') as f:
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

def process_folder(path):
    path = os.path.abspath(path)
    data = []
    data1 = []
    data2 = []
    data3 = []
    data4 = []
    data12 = []
    data13 = []
    data14 = []
    data23 = []
    data24 = []
    data34 = []
    data123 = []
    data124 = []
    data134 = []
    data234 = []
    data1234 = []
    data1304 = []
    for x in os.listdir(path):
        new_path = os.path.join(path, x)
        if os.path.isdir(new_path):
            d, d1, d2, d3, d4, d12, d13, d14, d23, d24, d34, d123, d124, d134, d234, d1234, d1304 = process_folder(new_path)
            data[0:0] = d
            data1[0:0] = d1
            data2[0:0] = d2
            data3[0:0] = d3
            data4[0:0] = d4
            data12[0:0] = d12
            data13[0:0] = d13
            data14[0:0] = d14
            data23[0:0] = d23
            data24[0:0] = d24
            data34[0:0] = d34
            data123[0:0] = d123
            data124[0:0] = d124
            data134[0:0] = d134
            data234[0:0] = d234
            data1234[0:0] = d1234
            data1304[0:0] = d1304
        elif os.path.isfile(new_path) and os.path.splitext(x)[1]=='.txt':
            print(new_path)
            d, d1, d2, d3, d4, d12, d13, d14, d23, d24, d34, d123, d124, d134, d234, d1234, d1304 = process_file(new_path)
            data[0:0] = d
            data1[0:0] = d1
            data2[0:0] = d2
            data3[0:0] = d3
            data4[0:0] = d4
            data12[0:0] = d12
            data13[0:0] = d13
            data14[0:0] = d14
            data23[0:0] = d23
            data24[0:0] = d24
            data34[0:0] = d34
            data123[0:0] = d123
            data124[0:0] = d124
            data134[0:0] = d134
            data234[0:0] = d234
            data1234[0:0] = d1234
            data1304[0:0] = d1304
    return data, data1, data2, data3, data4, data12, data13, data14, data23, data24, data34, data123, data124, data134, data234, data1234, data1304

# def write_csv(path, data):
#     with open(path, 'w', newline='') as f:
#         writer = csv.writer(f)
#         writer.writerows(data)

def suc(data, num):
    num_success = 0
    for i in range(0, len(data)):
        num_success += data[i][0]
    if len(data) > 0:
        print(str(num)+'成功率:' + str(num_success) + '/' + str(len(data)) + '=' + str(round_float(num_success/len(data))))
    else:
        print(str(num)+'成功率:0')

win_percent = 0.034
lose_percent = 0.1
lose_cache = 0.005
days = 30
#increase = 0.05

buy_dates = set([])

shanghai_data = process_index('F:/项目/StockMining/data_for_dl/data/data_buy_follow_index_1/999999.txt')
shenzhen_data = process_index('F:/项目/StockMining/data_for_dl/data/data_buy_follow_index_1/399001.txt')
shanghai_date_list = sorted(shanghai_data.keys())
shenzhen_date_list = sorted(shenzhen_data.keys())

data, data1, data2, data3, data4, data12, data13, data14, data23, data24, data34, data123, data124, data134, data234, data1234, data1304 = process_folder('F:/项目/StockMining/data_for_dl/data/data_buy_follow_index_1/raw')

print('e20')
suc(data, 0)
suc(data1, 1)
suc(data2, 2)
suc(data3, 3)
suc(data4, 4)
suc(data12, 12)
suc(data13, 13)
suc(data14, 14)
suc(data23, 23)
suc(data24, 24)
suc(data34, 34)
suc(data123, 123)
suc(data124, 124)
suc(data134, 134)
suc(data234, 234)
suc(data1234, 1234)
suc(data1304, 1304)
print(sorted(buy_dates))

# #random.shuffle(data)
# data = sorted(data, key=itemgetter(0,1))
# write_csv('../../data/test/csv/data.csv', data)

# title = []
# title.append('date')
# title.append('increase')
# title.append('name')
# title.append('pre_close')
# title.append('close')
# title.append('success')
# data.insert(0, title)
# write_csv('../../data/test/csv/data_weka.csv', data)