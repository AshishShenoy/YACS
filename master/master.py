import sys
import socket
import threading
import logging
import json
import time
import random


JOB_REQUESTS_PORT = 5000
WORKER_RESPONSES_PORT = 5001
ALL_MAPPERS_COMPLETED_CODE = -1


thread_lock = threading.Lock()
random.seed(3)


def read_args():
    if len(sys.argv) != 3:
        print("Usage: python master.py /path/to/config <scheduling-algorithm>")
        exit(1)

    config_file = sys.argv[1]
    scheduling_algo = sys.argv[2]

    with open(config_file, "r") as f:
        config = json.loads(f.read())

    return config, scheduling_algo


def init_logging(scheduling_algo):
    logging.basicConfig(
        filename=f"../logs/master_{scheduling_algo}.log",
        filemode="w",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.disable(logging.DEBUG)


def preprocess_workers(workers):
    for worker in workers:
        worker["free_slots"] = worker["slots"]

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", int(worker["port"])))
        s.listen(50)
        worker["socket"] = s

    workers_dict = {}
    for worker in workers:
        workers_dict[worker["worker_id"]] = worker

    return workers_dict


def send_task_to_worker(worker, job_id, task):
    worker_socket = worker["socket"]
    c, addr = worker_socket.accept()
    c.settimeout(5)

    task_json = {
        "job_id": job_id,
        "task_id": task["task_id"],
        "duration": task["duration"],
    }
    c.send(json.dumps(task_json).encode())
    c.close()

    logging.info(f"started task {task['task_id']} of job {job_id} on worker {worker['worker_id']}")


def listen_for_jobs(workers, scheduling_algo, jobs):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as job_request_socket:
        job_request_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        job_request_socket.bind(("localhost", JOB_REQUESTS_PORT))
        job_request_socket.listen(50)

        selected_worker_index = 0
        all_worker_ids = list(workers.keys())

        while True:
            client_socket, address = job_request_socket.accept()
            client_socket.settimeout(5)
            job_request = json.loads(client_socket.recv(2048).decode())

            job_request["unfinished_map_tasks"] = len(job_request["map_tasks"])
            jobs[job_request["job_id"]] = job_request

            logging.info(f"started job {job_request['job_id']}")

            for task in job_request["map_tasks"]:
                assigned = False
                while not assigned:
                    thread_lock.acquire()
                    if scheduling_algo == "RANDOM":
                        selected_worker_id = random.randint(1, len(workers))

                    elif scheduling_algo == "RR":
                        selected_worker_id = all_worker_ids[selected_worker_index]

                    elif scheduling_algo == "LL":
                        selected_worker_id = max(workers, key=lambda worker: workers[worker]["free_slots"])

                    if workers[selected_worker_id]["free_slots"] > 0:
                        send_task_to_worker(workers[selected_worker_id], job_request["job_id"], task)
                        workers[selected_worker_id]["free_slots"] -= 1

                        logging.debug(
                            f'worker {selected_worker_id} has {workers[selected_worker_id]["free_slots"]} free slots'
                        )
                        thread_lock.release()
                        assigned = True

                    else:
                        thread_lock.release()
                        if scheduling_algo == "LL":
                            logging.debug(f"all workers have filled slots")
                            time.sleep(1)
                        else:
                            logging.debug(f"all slots of worker {selected_worker_id} are full")
                            time.sleep(0.1)
                    
                    selected_worker_index = (selected_worker_index + 1) % len(workers)
            client_socket.close()


def finish_task_from_worker(workers, server_worker_socket, jobs):
    client_socket, address = server_worker_socket.accept()
    client_socket.settimeout(5)
    completed_task = json.loads(client_socket.recv(2048).decode())

    logging.info(
        f"task {completed_task['task_id']} of job {completed_task['job_id']} on worker {completed_task['worker_id']} has finished executing"
    )

    thread_lock.acquire()
    workers[completed_task["worker_id"]]["free_slots"] += 1
    logging.debug(
        f'worker {completed_task["worker_id"]} has {workers[completed_task["worker_id"]]["free_slots"]} free slots'
    )
    thread_lock.release()

    if "M" in completed_task["task_id"]:
        jobs[completed_task["job_id"]]["unfinished_map_tasks"] -= 1
        logging.debug(
            f"job {completed_task['job_id']} has {jobs[completed_task['job_id']]['unfinished_map_tasks']} remaining map tasks"
        )
    client_socket.close()


def listen_to_workers(workers, scheduling_algo, jobs):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_worker_socket:
        server_worker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_worker_socket.bind(("localhost", WORKER_RESPONSES_PORT))
        server_worker_socket.listen(50)

        selected_worker_index = 0
        all_worker_ids = list(workers.keys())

        while True:
            finish_task_from_worker(workers, server_worker_socket, jobs)

            for job_id in list(jobs.keys()):
                if jobs[job_id]["unfinished_map_tasks"] == 0:
                    for task in jobs[job_id]["reduce_tasks"]:
                        assigned = False
                        while not assigned:
                            thread_lock.acquire()
                            if scheduling_algo == "RANDOM":
                                selected_worker_id = random.randint(1, len(workers))

                            elif scheduling_algo == "RR":
                                selected_worker_id = all_worker_ids[selected_worker_index]

                            elif scheduling_algo == "LL":

                                selected_worker_id = max(workers, key=lambda worker: workers[worker]["free_slots"])

                            if workers[selected_worker_id]["free_slots"] > 0:
                                send_task_to_worker(workers[selected_worker_id], job_id, task)
                                workers[selected_worker_id]["free_slots"] -= 1

                                logging.debug(
                                    f'worker {selected_worker_id} has {workers[selected_worker_id]["free_slots"]} free slots'
                                )
                                thread_lock.release()
                                assigned = True

                            else:
                                thread_lock.release()
                                if scheduling_algo == "LL":
                                    logging.debug(f"all workers have filled slots")
                                    time.sleep(1)
                                else:
                                    logging.debug(f"all slots of worker {selected_worker_id} are full")
                                    time.sleep(0.1)
                                
                                finish_task_from_worker(workers, server_worker_socket, jobs)
                            
                            selected_worker_index = (selected_worker_index + 1) % len(workers)

                    jobs[job_id]["unfinished_map_tasks"] = ALL_MAPPERS_COMPLETED_CODE


def main():
    config, scheduling_algo = read_args()

    init_logging(scheduling_algo)

    workers = preprocess_workers(config["workers"])

    jobs = {}

    job_listen_thread = threading.Thread(target=listen_for_jobs, args=[workers, scheduling_algo, jobs])
    job_listen_thread.start()

    worker_listen_thread = threading.Thread(target=listen_to_workers, args=[workers, scheduling_algo, jobs])
    worker_listen_thread.start()


if __name__ == "__main__":
    main()
