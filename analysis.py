import re
from pprint import pprint
import datetime
import matplotlib.pyplot as plt
import numpy as np

types = {"LL": "Least Loaded", "RANDOM": "Random", "RR": "Round Robin"}


def getdt(line):
    dtpattern = r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\,\d{3}"
    date = re.search(dtpattern, line)
    dt = datetime.datetime.strptime(date.group(0), r"%Y-%m-%d %H:%M:%S,%f")
    return dt


def getstats(t):
    jobs = {}

    print("-" * 50)
    print(f"For {types[t]} scheduling")
    print("-" * 50 + "\n")
    log = open(f"master_{t}.log")

    for line in log:
        if "INFO" in line:
            test = re.search(r"started job (\d+)", line)
            if test:
                job_id = test.group(1)
                jobs[job_id] = [getdt(line), {}, {}]
            test = re.search(r"started task (\w+) of job (\d+)", line)
            if test:
                task_id = test.group(1)
                job_id = test.group(2)
                jobs[job_id][1][task_id] = getdt(line)
            test = re.search(r"task (\w+) of job (\d+).*finished executing", line)
            if test:
                task_id = test.group(1)
                job_id = test.group(2)
                jobs[job_id][2][task_id] = getdt(line)

    job_times = []
    task_times = []
    for job_id in jobs.keys():
        last_task_time = jobs[job_id][0]
        print(f"In Job {job_id}")
        for task_id in jobs[job_id][1]:
            task_time = (
                jobs[job_id][2][task_id] - jobs[job_id][1][task_id]
            ).total_seconds()
            task_times.append(task_time)
            print(f"Time Taken for task {task_id} : {task_time}")
            last_task_time = jobs[job_id][2][task_id]
        job_time = (last_task_time - jobs[job_id][0]).total_seconds()
        job_times.append(job_time)
        print(f"Total job time : {job_time}\n")
    log.close()
    res = [
        np.mean(job_times),
        np.median(job_times),
        np.mean(task_times),
        np.median(task_times),
    ]
    print(f"Mean Job Time : {res[0]}")
    print(f"Median Job Time : {res[1]}")
    print(f"Mean Task Time : {res[2]}")
    print(f"Median Task Time : {res[3]}")
    return res


def plotline(t):
    log = open(f"master_{t}.log")
    initial_time = None
    workers = {}

    def getdt(line):
        dtpattern = r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\,\d{3}"
        date = re.search(dtpattern, line)
        dt = datetime.datetime.strptime(date.group(0), r"%Y-%m-%d %H:%M:%S,%f")
        return dt

    first_line = True
    for line in log:
        if first_line:
            initial_time = getdt(line)
            first_line = False
        else:
            if "DEBUG" not in line:
                test = re.search(r"started.*on worker (\d+)", line)
                if test:
                    worker_id = test.group(1)
                    time = (getdt(line) - initial_time).total_seconds()
                    try:
                        workers[worker_id][0].append(workers[worker_id][0][-1] + 1)
                        workers[worker_id][1].append(time)
                    except:
                        workers[worker_id] = [[0, 1], [0, time]]
                test = re.search(r"worker (\d+) has finished executing", line)
                if test:
                    worker_id = test.group(1)
                    time = (getdt(line) - initial_time).total_seconds()
                    workers[worker_id][0].append(workers[worker_id][0][-1] - 1)
                    workers[worker_id][1].append(time)
    log.close()

    plt.figure()
    for worker_id in sorted(list(workers.keys())):
        plt.plot(
            workers[worker_id][1], workers[worker_id][0], label=f"Worker {worker_id}"
        )
    plt.title(f"Workers for {types[t]} scheduling")
    plt.xlabel("Time")
    plt.ylabel("Number of filled slots")
    plt.legend()
    plt.savefig(f"worker_plot_{t}.png")


plotline("RANDOM")
plotline("RR")
plotline("LL")

random_stats = getstats("RANDOM")
rr_stats = getstats("RR")
ll_stats = getstats("LL")

sceduling_types = [types["RANDOM"], types["RR"], types["LL"]]
job_means = [random_stats[0], rr_stats[0], ll_stats[0]]
job_medians = [random_stats[1], rr_stats[1], ll_stats[1]]
task_means = [random_stats[2], rr_stats[2], ll_stats[2]]
task_medians = [random_stats[3], rr_stats[3], ll_stats[3]]

x = np.arange(len(sceduling_types))
width = 0.20

fig, ax = plt.subplots(figsize=(15, 8))
rects1 = ax.bar(x - 3 * width / 2, job_means, width, label="Mean Job Time")
rects1 = ax.bar(x - width / 2, job_medians, width, label="Median Job Time")
rects2 = ax.bar(x + width / 2, task_means, width, label="Mean Task Time")
rects2 = ax.bar(x + 3 * width / 2, task_medians, width, label="Median task Time")

ax.set_ylabel("Time")
ax.set_title("Scheduling Types")
ax.set_xticks(x)
ax.set_xticklabels(sceduling_types)
ax.legend()


plt.savefig("time_plot.png")