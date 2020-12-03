import sys
import socket
import threading
import logging
import json
import time


thread_lock = threading.Lock()


def init_logging(worker_id):
    logging.basicConfig(
        filename=f"worker{worker_id}.log",
        filemode="w",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    # logging.disable(logging.DEBUG)


def read_args():
    if len(sys.argv) != 3:
        print("Usage: python Worker.py <port> <worker-id>")
        exit()

    port_no = int(sys.argv[1])
    worker_id = int(sys.argv[2])

    return port_no, worker_id


def listen_for_tasks(port_no, worker_id, tasks):
    logging.info(f"worker {worker_id} listening on port {port_no}")

    while True:
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect(("localhost", port_no))
        task = json.loads(c.recv(256).decode())
        c.close()

        thread_lock.acquire()
        tasks.append(task)
        logging.info(f"task {task['task_id']} of job {task['job_id']} has been added to the task list")
        thread_lock.release()


def execute_tasks(port_no, worker_id, tasks):
    logging.info(f"worker {worker_id} has started executing tasks")

    while True:
        time.sleep(1)

        thread_lock.acquire()
        for task in tasks:
            task["duration"] -= 1

        tasks_to_end = []
        for task in tasks:
            if task["duration"] == 0:
                tasks_to_end.append(task)

        for task in tasks_to_end:
            tasks.remove(task)
            logging.info(f"task {task['task_id']} of job {task['job_id']} has finished executing")

            task_completed_json = {"task_id": task["task_id"], "job_id": task["job_id"], "worker_id": worker_id}

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect(("localhost", 5001))
                client_socket.send((json.dumps(task_completed_json) + '\n').encode())

        thread_lock.release()


def main():
    port_no, worker_id = read_args()

    init_logging(worker_id)

    tasks = []

    listening_thread = threading.Thread(target=listen_for_tasks, args=[port_no, worker_id, tasks])
    listening_thread.start()

    execute_thread = threading.Thread(target=execute_tasks, args=[port_no, worker_id, tasks])
    execute_thread.start()


if __name__ == "__main__":
    main()