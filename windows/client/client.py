import asyncio
from functools import wraps
import aiohttp
from aiohttp import ClientSession, ClientTimeout
from typing import Any, Dict
from datetime import datetime
import json
import random
from pathlib import Path
from uuid import uuid4

from matplotlib.pylab import f


# set client class for data collection
class DataClient:
    def __init__(self, base_url: str):
        self.target_url = base_url
        self.session = ClientSession(timeout=ClientTimeout(total=0), connector=aiohttp.TCPConnector(limit=5000))

    # send request
    async def fetch_data(self, headers, data) -> Dict[str, Any]:
        async with self.session.post(self.target_url, headers=headers, json=data) as response:
            response_data = dict(await response.json())
            print(f"[function] (fetch_data) response_data: {response_data}")
            if response_data.get("error") == 0:
                return response_data
            else:
                # check error message
                raise Exception(response_data.get("message"))
        
    async def close(self):
        await self.session.close()


def generate_task_data(task_class: str, **kwargs):
    if task_class not in ['HDD', 'MEM', 'CPU']:
        raise ValueError(f"Invalid task_class: {task_class}. Must be one of 'HDD', 'MEM', 'CPU'.")

    if task_class == 'HDD':
        size_mb = kwargs.get('size_mb', None)
        task_data = {
            "size_mb": size_mb,
        }
    elif task_class == 'MEM':
        size_mb = kwargs.get('size_mb', None)
        task_data = {
            "size_mb": size_mb,
        }
    elif task_class == 'CPU':
        number = kwargs.get('number', None)
        task_data = {
            "number": number,
        }
    else:
        raise ValueError(f"Unsupported task_class: {task_class}")
    
    task_index = uuid4().hex
    data = {
        "task_index": task_index,
        **task_data,
    }
    headers = {
        "task_index": task_index,
        "Content-Type": "application/json",
        "task_class": task_class,
    }
    # debug: show generated task
    print(f"[generate_task_data] task_index={data['task_index']} task_class={task_class}")
    return headers, data


def save_results_to_file(data_list: list[Dict[str, Any]], results: Dict[str, Any], task_class: str, folder: str) -> Path:
    # save results to file 
    # style:
    # check task_index in data and results
    #   request number from data
    #   response number from results
    # add request number to result['request_number']
    # add response number to result['response_number']

    filename = f"results_{task_class}.json"

    # count startwith "data" folder sum and add index to "data_t3"
    parent_path = Path(__file__).parent / 'data_t3' / folder
    parent_path.mkdir(parents=True, exist_ok=True)

    filepath = parent_path / filename

    with open(filepath, "w") as f:
        # prepare results
        output = dict()

        # for key, value in results.items():
            # print(f"[save_results_to_file] result from results: {key}: {value}")

        exclude_key_names = {"success", "error", "message", "task_index", "result"}

        for req in data_list:
            task_index = req.get("task_index")
            matching_result: dict = results.get(task_index)
            if matching_result:
                # judge task_class to get task_data
                # task data for all k, v but whatever it is
                tasks_data = { k: v for k, v in matching_result.items() if k not in exclude_key_names }
                print(f"[save_results_to_file] tasks_data: {tasks_data}")
                # add to output
                for k, v in tasks_data.items():
                    if output.get(k):
                        output[k].append(v)
                    else:
                        output[k] = [v]

        json.dump(output, f, indent=4)

    print(f"Saved results to {filepath}")
    return filepath


def tasks(task_func):
    @wraps(task_func)
    async def wrapper(client: DataClient, *args, **kwargs):
        tasks = []
        data_list = []
        return await task_func(client, data_list, tasks, *args, **kwargs)
    return wrapper


@tasks
async def hdd_tasks(client: DataClient, data_list: list[Dict[str, Any]], tasks: list[asyncio.Task], folder: str = None, task_count: int = 5):
    for i in range(task_count):
        size_mb=random.randint(10, 500)
        headers, data = generate_task_data(task_class="HDD", size_mb=size_mb)
        task = asyncio.create_task(client.fetch_data(headers, data))    
        tasks.append(task)
        print(f"[hdd_task] enqueued task {i+1}/{task_count} id={data['task_index']} size_mb={data['size_mb']}")
        data_list.append(data)

    # gather results
    results: list[dict[str, Any]] = await asyncio.gather(*tasks, return_exceptions=True)
    return_results= dict()

    # collect results
    for result in results:
        if not isinstance(result, dict):
            print(f"[hdd_task] encountered exception: {result}")
            continue

        task_index = result.get("task_index")
        if task_index:
            inner_response = result.pop("response_data")
            print(f"[hdd_task] inner_response: {inner_response}")
            return_results[task_index] = {**result, **inner_response}
        print(f"[hdd_task] received result for task_index={result['task_index']}")
    
    # save results to file
    path = save_results_to_file(data_list=data_list, results=return_results, task_class="HDD", folder=folder)
    # print log
    print(f"Sent {len(data_list)} requests at once with speed fast")
    print(f"Success save file to path: {path}")


@tasks
async def mem_tasks(client: DataClient, data_list: list[Dict[str, Any]], tasks: list[asyncio.Task], folder: str = None, task_count: int = 10):
    for i in range(task_count):
        headers, data = generate_task_data(task_class="MEM", size_mb=random.randint(200, 1000))
        task = asyncio.create_task(client.fetch_data(headers, data))
        tasks.append(task)
        print(f"[mem_task] enqueued task {i+1}/{task_count} id={data['task_index']} size_mb={data['size_mb']}")
        data_list.append(data)

    # gather results
    results: list[dict] = await asyncio.gather(*tasks, return_exceptions=True)
    return_results = dict()

    # collect results
    for result in results:
        if isinstance(result, Exception):
            print(f"[mem_task] encountered exception: {result}")
            continue
        task_index = result.get("task_index")
        if task_index:
            inner_response = result.pop("response_data")
            print(f"[mem_task] inner_response: {inner_response}")
            return_results[task_index] = {**result, **inner_response}
        print(f"[mem_task] received result for task_index={result['task_index']}")

    path = save_results_to_file(data_list=data_list, results=return_results, task_class="MEM", folder=folder)
    # print log
    print(f"Sent {len(data_list)} requests at once with speed fast")
    print(f"Success save file to path: {path}")


@tasks
async def cpu_tasks(client: DataClient, data_list: list[Dict[str, Any]], tasks: list[asyncio.Task], folder: str = None, task_count: int = 5):
    # batch_size = random.randint(10, 20)
    batch_size = task_count
    for i in range(batch_size):  # send batch_size requests at once
        number = random.randint(1, 500_000)
        headers, data = generate_task_data(task_class="CPU", number=number)
        data_list.append(data)
        task = asyncio.create_task(client.fetch_data(headers, data))
        tasks.append(task)
        print(f"[fast] enqueued task {i+1}/{batch_size} id={data['task_index']} size_mb={data.get('size_mb', 'N/A')}")

    # gather results
    results: list[dict] = await asyncio.gather(*tasks, return_exceptions=True)
    return_results = dict()

    # collect results
    for result in results:
        if isinstance(result, Exception):
            print(f"[cpu_task] encountered exception: {result}")
            continue
        task_index = result.get("task_index")
        if task_index:
            return_results[task_index] = result
    path = save_results_to_file(data_list=data_list, results=return_results, task_class="CPU", folder=folder)
    # print log
    print(f"Sent {len(data_list)} requests at once with speed fast")
    print(f"Success save file to path: {path}")


async def main():
    # create client instance
    url = "http://192.168.0.100:8080/"
    client = DataClient(url)
    folder = datetime.now().strftime("%Y_%m_%d %H_%M_%S")

    await hdd_tasks(client, folder=folder, task_count=2000)

    await mem_tasks(client, folder=folder, task_count=2000)
    
    await cpu_tasks(client, folder=folder, task_count=2000)

    # close client session
    await client.close()

    # finish
    print("Data collection completed.")

if __name__ == "__main__":
    asyncio.run(main())