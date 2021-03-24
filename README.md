# YACS - Yet Another Centralized Scheduler

A centralized scheduling framework used to coordinate tasks between several 
worker nodes. Project component for the UE18CS322 Big Data course.

The master node receives a job request sent to a designated port. Each job 
consists of several tasks with dependencies (map tasks need to be 
performed before reduce tasks). These tasks are assigned to worker nodes 
based on one of 3 scheduling algorithms:

- Random Scheduling
- Round Robin Scheduling
- Least Loaded Scheduling

After the workers complete their tasks, they sent a response back to the 
master node, and remove the tasks from their task pool.

An analysis script is also provided which can used to compare the 
performance of each of the three scheduling algorithms.


## Status

The master and worker nodes are able to successfully coordinate to complete 
tasks, and their results are logged for later analysis.

Currently only simulated tasks are supported, i.e. every task consists of a 
positive number representing seconds, which the worker node has to sleep for.


## Requirements

The following dependencies in the form of python3 libraries are required:
- numpy
- matplotlib

Install them using pip:
```
pip install -r requirements.txt
```

## Execution

A sample config.json has been provided which is used by the master node to 
learn about the entire network of worker nodes. The execution instructions 
mentioned here are in reference to the sample config.

### On host machines

Execute the following instructions in order on 5 different terminals, after 
changing to the appropriate service's directory:

```
python3 master.py config.json <scheduling-algorithm>
```
```
python3 worker.py 4000 1
```
```
python3 worker.py 4001 2
```
```
python3 worker.py 4002 3
```
```
python3 requests.py <number-of-requests>
```

### Using docker

A docker-compose.yml file is provided which automates the above process 
for the sample config.json

While in the repository folder, run:
```
docker-compose up
```

This will create containers for the master node and for allthe worker nodes, 
and also a container for the requests generator.