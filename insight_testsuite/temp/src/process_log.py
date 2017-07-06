#!/usr/bin/env python

import sys
import json
import time
from pprint import pprint
import datetime
from collections import defaultdict
import copy
degree = 0
transaction_limit =0
from decimal import Decimal
graph = None

class Node(object):

    def __init__(self,id):
        self.id=id
        self.transactions = []
        self.friends=[]

    def add_friend(self, friends_id):
        self.friends.append(friends_id)

    def add_transaction(self, timestamp,amount):
        self.transactions.append([timestamp,amount])

    def get_transaction(self):
        return self.transactions

    def get_friends(self):
        return self.friends

    def remove_friend(self, friends_id):
        self.friends.remove(friends_id)

class Graph(object):

    def __init__(self):
        self.nodes ={}

    def add_nodes(self,id):
        new_node = Node(id)
        self.nodes[id] = new_node

    def get_nodes(self):
        return self.nodes.keys()

def createGraph():
    # Import file as JSON
    global degree
    global transaction_limit
    global graph
    graph = Graph()
    with open('C:/Users/Navneet/PycharmProjects/anomaly_detection/log_input/batch_log.json') as f:
        for line in f:
            data = json.loads(line)
            print(data)
            if 'D' in data:
                degree = data["D"]
                transaction_limit = data["T"]
            else:
                if(data['event_type']== 'purchase'):
                    id = data['id']
                    amount=data['amount']
                    timestamp = data['timestamp']
                    if(graph.nodes.__contains__(id)):
                        graph.nodes[id].add_transaction(timestamp,amount)
                        #print(graph.nodes[id].get_transaction())
                    else:
                        graph.add_nodes(id)
                        graph.nodes[id].add_transaction(timestamp,amount)
                        #print(graph.nodes[id].get_transaction())

                if(data['event_type']=='befriend'):
                    id1=data['id1']
                    id2=data['id2']
                    if (not id1 in graph.nodes):
                        graph.add_nodes(id1)
                    if (not id2 in graph.nodes):
                        graph.add_nodes(id2)
                    graph.nodes[id1].add_friend(id2)
                    graph.nodes[id2].add_friend(id1)

                if(data['event_type']=='unfriend'):
                    id1 = data['id1']
                    id2 = data['id2']
                    graph.nodes[id1].remove_friend(id2)
                    graph.nodes[id2].remove_friend(id1)


def stream_data():
    global calculate
    global graph

    def calculate(transactions):
        sum_of_trasactions = 0
        mean = 0
        sum_of_squares = 0
        length = len(transactions)

        for trans in transactions:
            sum_of_trasactions = sum_of_trasactions + Decimal(trans[1])

        mean = round(Decimal(sum_of_trasactions / length), 2)

        for trans in transactions:
            sum_of_squares = sum_of_squares + (Decimal(trans[1]) - mean) * (Decimal(trans[1]) - mean)

        sd = round(Decimal(sum_of_squares / length).sqrt(), 2)

        return mean, sd

    file = open('C:/Users/Navneet/PycharmProjects/anomaly_detection/log_output/flagged_purchases.json', "w")

    with open('C:/Users/Navneet/PycharmProjects/anomaly_detection/log_input/stream_log.json') as stream_file:
        for line in stream_file:
            data = json.loads(line)
            print (data)
            if (data['event_type'] == 'purchase'):
                id = data['id']
                amount = data['amount']
                timestamp = data['timestamp']
                if (graph.nodes.__contains__(id)):
                    graph.nodes[id].add_transaction(timestamp, amount)
                else:
                    graph.add_nodes(id)
                    graph.nodes[id].add_transaction(timestamp, amount)
                number_transaction = 0
                total_amount=0
                friend_list = graph.nodes[id].get_friends()
                social_degree = 1
                #print(friend_list)
                new_friend_list = set(copy.deepcopy(friend_list))
                #print(friend_list)
                while 1:
                    if(str(social_degree)== degree):
                        break
                    else:
                        social_degree= social_degree+1
                        tempID = id
                        for x in friend_list:
                            new_friends = set(graph.nodes[x].get_friends())

                            #print(new_friends)
                            if new_friends:
                                new_friend_list |= new_friends
                                if tempID in new_friend_list:
                                    new_friend_list.remove(tempID)

                        if new_friend_list:
                            friend_list = copy.deepcopy(new_friend_list)

                trasactions = []

                for id in friend_list:
                    trans = graph.nodes[id].get_transaction()
                    for transac in trans:
                        trasactions.append(transac)

                if str(len(trasactions)) in ['1','0'] :
                    pass
                elif len(trasactions)>50:
                    mean, sd = calculate(trasactions)
                    #print (mean)
                    #print (sd)
                else:
                    trasactions = sorted(trasactions, key = lambda x:time.mktime(time.strptime(x[0], '%Y-%m-%d %H:%M:%S')))
                    mean, sd = calculate(trasactions[0:49])
                    #print (mean)
                    #print (sd)

                if(Decimal(amount) >= Decimal(mean + (3*sd))):
                    #print(amount)
                    #print('cc')
                    #print (data)
                    data['mean'] = str(mean)
                    data['sd'] = str(sd)
                    file.write(str(data)+'\n')

                #print (trasactions)
                #print(friend_list)


    file.close()



if __name__ == '__main__':
    createGraph()
    stream_data()