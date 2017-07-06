Entry in Anomaly Detection (Insight Data Engineering Challenge) by Navneet Jain

# Table of Contents
1. [Summerized Approach](README.md#summerized Approach)
2. [Run Instruction & Environment](README.md#run instructions )
3. [Output](README.md#output)


# Summerized Approach

My approach toward tackling the Anomaly Detection problem is pretty simple. After understanding the problem statement, I divided the problem in sub-problems.

First, I decide to use the graph data structure as i have to develop a social network of users, theirs friends and purchase transaction done by users.

Then I focused to create a graph in python using the batch_log.json

After successfully developing the intial network using graph, I started streaming transaction from stream-log.json and 
applying the logic to calculate the anomaly transaction and update the anomalies transaction to flagged_purchase file.


# Run instructions
Python version: Python 3.6.1
Tool: JetBrains PyCharm Community Edition

# Output file
I have added the flagged_purchase file in log_output directory after successfully execute the code on batch_log.json and stream_log.json files.
