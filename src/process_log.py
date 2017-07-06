#!/usr/bin/env python

"""# Summerized Approach

My approach toward tackling the Anomaly Detection problem is pretty simple. After understanding the problem 
statement, I divided the problem in sub-problems.

First, I decide to use the graph data structure as i have to develop a social network of users, theirs friends 
and purchase transaction done by users. Then I focused to create a graph in python using the batch_log.json

After successfully developing the intial network using graph, I started streaming transaction from 
stream-log.json and applying the logic to calculate the anomaly transaction and update the anomalies
transaction to flagged_purchase file."""

__author__      = "Navneet Jain"
__email__ = "navneet.jain@nyu.edu"

import sys
import json
import time
from pprint import pprint
import copy
degree = 0
transaction_limit =0
from decimal import Decimal
graph = None
#Every user have a node and transaction and friends list associate with them.
class Node(object):

    def __init__(self,id):
        self.id=id
        self.transactions = []
        self.friends=[]

    #Adds friends to a users list
    def add_friend(self, friends_id):
        self.friends.append(friends_id)

    #Adds transaction for the current users
    def add_transaction(self, timestamp,amount):
        self.transactions.append([timestamp,amount])

    #return transaction for particular user
    def get_transaction(self):
        return self.transactions

    #return the list of friends of the current user
    def get_friends(self):
        return self.friends

    #remove the request user from friend-list and update the friends id.
    def remove_friend(self, friends_id):
        self.friends.remove(friends_id)

#This graph contain all the user in the social network with each user represented with a node.
class Graph(object):

    def __init__(self):
        self.nodes ={}

    #Add a new user
    def add_nodes(self,id):
        new_node = Node(id)
        self.nodes[id] = new_node

    #returns list of users
    def get_nodes(self):
        return self.nodes.keys()

#Read the batch.json file and create the desired social network with reading each transaction line by line.
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
                #if the transaction type is purchase then add it the correspording user. if user doest exist, add new user.
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
                # Add friend: add the user_ids in both the user's friend list.
                if(data['event_type']=='befriend'):
                    id1=data['id1']
                    id2=data['id2']
                    if (not id1 in graph.nodes):
                        graph.add_nodes(id1)
                    if (not id2 in graph.nodes):
                        graph.add_nodes(id2)
                    graph.nodes[id1].add_friend(id2)
                    graph.nodes[id2].add_friend(id1)
                #Remove Friend: remove friends from each others friend list.
                if(data['event_type']=='unfriend'):
                    id1 = data['id1']
                    id2 = data['id2']
                    graph.nodes[id1].remove_friend(id2)
                    graph.nodes[id2].remove_friend(id1)

#streaming the stream_log.json file and find the anomalies transaction then update them to flagged_purchased.json file
def stream_data():
    global calculate
    global graph

    #This method will calcuale the standard deviation and mean for each transaction to find anomalies transaction
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
    # Create a flagged_purchase.json file to store the anomalies purchase.
    file = open('C:/Users/Navneet/PycharmProjects/anomaly_detection/log_output/flagged_purchases.json', "w")
    #start reading the stream_log.json file.
    with open('C:/Users/Navneet/PycharmProjects/anomaly_detection/log_input/stream_log.json') as stream_file:
        for line in stream_file:
            data = json.loads(line)
            #print (data)
            # if the transaction type is purchase then add it the correspording user. if user doest exist, add new user.
            if (data['event_type'] == 'purchase'):
                id = data['id']
                amount = data['amount']
                timestamp = data['timestamp']
                if (graph.nodes.__contains__(id)):
                    graph.nodes[id].add_transaction(timestamp, amount)
                else:
                    graph.add_nodes(id)
                    graph.nodes[id].add_transaction(timestamp, amount)

                #Start : Logic to find the friends upto the desired degree and store them in a list for the current purchase
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
                #End: friends finding logic

                #Start: Logic to have all the transaction for the perforing anomaly detection.
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

            #End: Login to find all the transaction in the social network for the particular user.

            # Add friend: add the user_ids in both the user's friend list.
            if (data['event_type'] == 'befriend'):
                id1 = data['id1']
                id2 = data['id2']
                if (not id1 in graph.nodes):
                    graph.add_nodes(id1)
                if (not id2 in graph.nodes):
                    graph.add_nodes(id2)
                graph.nodes[id1].add_friend(id2)
                graph.nodes[id2].add_friend(id1)

            # Remove Friend: remove friends from each others friend list.
            if (data['event_type'] == 'unfriend'):
                id1 = data['id1']
                id2 = data['id2']
                graph.nodes[id1].remove_friend(id2)
                graph.nodes[id2].remove_friend(id1)
                #print (trasactions)
                #print(friend_list)

    file.close()

if __name__ == '__main__':
    createGraph()
    stream_data()