import argparse
import multiprocessing
import queue
import random
import threading
import time


def make_item(producer, number):
    return producer, number


def thread_producer(number, items, buffer):
    for item_number in range(items):
        item = make_item(number, item_number)
        buffer.put(item)
        print(f"[producer {number}] produced item {item_number} buffer size: {buffer.qsize()}", flush=True)
        time.sleep(random.uniform(0.01, 0.04))


def thread_consumer(number, buffer, counts):
    count = 0
    while True:
        item = buffer.get()
        if item is None:
            buffer.task_done()
            break

        count += 1
        print(f"[consumer {number}] consumed item {item[1]} buffer size: {buffer.qsize()}", flush=True)
        buffer.task_done()
        time.sleep(random.uniform(0.01, 0.04))

    counts.put(count)
    print(f"[consumer {number}] received stop signal - exiting", flush=True)


def process_producer(number, items, buffer):
    for item_number in range(items):
        item = make_item(number, item_number)
        buffer.put(item)
        print(f"[producer {number}] produced item {item_number}", flush=True)
        time.sleep(random.uniform(0.01, 0.04))


def process_consumer(number, buffer, counts):
    count = 0
    while True:
        item = buffer.get()
        if item is None:
            break

        count += 1
        print(f"[consumer {number}] consumed item {item[1]}", flush=True)
        time.sleep(random.uniform(0.01, 0.04))

    counts.put(count)
    print(f"[consumer {number}] received stop signal - exiting", flush=True)


def run_threads(producer_count, consumer_count, items, capacity):
    buffer = queue.Queue(maxsize=capacity)
    counts = queue.Queue()
    producers = []
    consumers = []

    for number in range(producer_count):
        producer = threading.Thread(target=thread_producer, args=(number + 1, items, buffer))
        producers.append(producer)
        producer.start()

    for number in range(consumer_count):
        consumer = threading.Thread(target=thread_consumer, args=(number + 1, buffer, counts))
        consumers.append(consumer)
        consumer.start()

    for producer in producers:
        producer.join()

    for _ in range(consumer_count):
        buffer.put(None)

    for consumer in consumers:
        consumer.join()

    consumed = sum(counts.get() for _ in consumers)
    return consumed, buffer.qsize()


def run_processes(producer_count, consumer_count, items, capacity):
    buffer = multiprocessing.Queue(maxsize=capacity)
    counts = multiprocessing.Queue()
    producers = []
    consumers = []

    for number in range(producer_count):
        producer = multiprocessing.Process(target=process_producer, args=(number + 1, items, buffer))
        producers.append(producer)
        producer.start()

    for number in range(consumer_count):
        consumer = multiprocessing.Process(target=process_consumer, args=(number + 1, buffer, counts))
        consumers.append(consumer)
        consumer.start()

    for producer in producers:
        producer.join()

    for _ in range(consumer_count):
        buffer.put(None)

    for consumer in consumers:
        consumer.join()

    consumed = sum(counts.get() for _ in consumers)
    buffer.close()
    buffer.join_thread()
    return consumed, 0


def run(name, function, producer_count, consumer_count, items, capacity):
    total = producer_count * items
    started = time.perf_counter()
    consumed, left = function(producer_count, consumer_count, items, capacity)
    elapsed = time.perf_counter() - started

    print(f"{name}: produced {total}, consumed {consumed}, items left {left}")
    print(f"{name}: {elapsed:.3f} seconds")
    return total == consumed and left == 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--producers", type=int, default=2)
    parser.add_argument("--consumers", type=int, default=2)
    parser.add_argument("--items", type=int, default=5)
    parser.add_argument("--capacity", type=int, default=3)
    args = parser.parse_args()

    print("Threading version")
    threads_worked = run("threads", run_threads, args.producers, args.consumers, args.items, args.capacity)
    print()

    print("Multiprocessing version")
    processes_worked = run("processes", run_processes, args.producers, args.consumers, args.items, args.capacity)
    print()

    if threads_worked and processes_worked:
        print("all producers finished, all consumers finished, 0 items left")
    else:
        print("the counts did not match")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
