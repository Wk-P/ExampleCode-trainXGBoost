from aiohttp import web
from functools import wraps
import asyncio
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from pathlib import Path
import shutil
from asyncio import Lock

thread_executor = ThreadPoolExecutor(max_workers=4)

def timer(func):
    """Decorator that supports both sync and async functions.

    For coroutine functions it returns an async wrapper that awaits the
    function and measures elapsed time. For regular functions it returns
    a sync wrapper.
    """
    @wraps(func)
    def _sync_wrapper(*args, **kwargs):
        start_time = perf_counter()
        result = func(*args, **kwargs)
        end_time = perf_counter()
        elapsed_time = end_time - start_time
        return result, elapsed_time

    @wraps(func)
    async def _async_wrapper(*args, **kwargs):
        start_time = perf_counter()
        result = await func(*args, **kwargs)
        end_time = perf_counter()
        elapsed_time = end_time - start_time
        return result, elapsed_time

    if asyncio.iscoroutinefunction(func):
        return _async_wrapper
    return _sync_wrapper

@timer
def cpu_task(n: int) -> tuple[int, float]:
    def is_prime(n: int) -> bool:
        if n <= 1:
            return False
        for i in range(2, int(n**0.5) + 1):
            if n % i == 0:
                return False
        return True

    count = 0
    for num in range(2, n + 1):
        if is_prime(num):
            count += 1

    print(f"[CPU] Found {count} prime numbers up to {n}")
    return count

@timer
def mem_task(size_mb: int) -> tuple[int, float]:
    import gc

    size = size_mb * 1024 * 1024
    print(f"[MEM] Allocating {size_mb} MB ...")
    data = bytearray(size)
    print(f"[MEM] Allocation down")
	
    for i in range(0, len(data), 4096):
        data[i] = 1
    print(f"[MEM] Accessed memory")

    del data
    gc.collect()
    print(f"[MEM] Freed memory")

    return size_mb


@timer
async def hdd_write_load(path, size_mb=1000) -> tuple[int, float]:
    _path = Path(path)
    chunk = b"\0" * (1024 * 1024) # 1MB, 全部零字节，CPU 不参与计算
    async with HDD_LOCK:
        print(f"[HDD] Writing {size_mb} MB to {path} ...")
        with open(path, "wb", buffering=0) as f:
            for _ in range(size_mb):
                f.write(chunk) 
    
    print(f"[HDD] Write down. File size: { _path.stat().st_size / (1024 * 1024)} MB")
    
    _path.unlink()
    print(f"[HDD] File deleted")

    return size_mb


async def handle_request_cpu(request: web.Request) -> web.Response:
    task_index = request.headers.get("task_index", "unknown")
    data: dict = await request.json()
    print(f"[CPU] Received request with task_index={task_index} number={data.get('number')}")
    number = data.get("number", 0)

    # get loop from asyncio
    loop = asyncio.get_event_loop()
    
    # calculate the processed time in thread pool
    prime_count, computation_time = await loop.run_in_executor(thread_executor, cpu_task, number)
    processing_time = computation_time
    return web.json_response({"result": prime_count, "task_index": task_index, "processing_time": processing_time})


async def handle_request_mem(request: web.Request) -> web.Response:
    def free_memory() -> int:
        with open('/sys/fs/cgroup/memory.max', 'r') as f:
            max_memory_b = int(f.read().strip())
        with open('/sys/fs/cgroup/memory.current', 'r') as f:
            used_memory_b = int(f.read().strip())
        return int(max_memory_b - used_memory_b) // (1024 * 1024)
    task_index = request.headers.get("task_index", "unknown")
    data: dict = await request.json()
    print(f"[MEM] Received request with task_index={task_index} size_mb={data.get('size_mb')}")
    size_mb = data.get("size_mb", 100)

    # check free memory before allocation
    if free_memory() <= size_mb:
        print(f"[MEM] Not enough free memory for task_index={task_index}, required={size_mb} MB")
        return web.json_response({"error": "not enough free memory", "task_index": task_index}, status=507)

    # record mem_free before allocation
    mem_free_before = free_memory()

    # get loop from asyncio
    loop = asyncio.get_event_loop()
    
    # calculate the processed time in thread pool
    allocated_size, computation_time = await loop.run_in_executor(thread_executor, mem_task, size_mb)
    processing_time = computation_time
    return web.json_response({"result": allocated_size, "task_index": task_index, "processing_time": processing_time, "mem_free_before": mem_free_before})


async def handle_request_hdd(request: web.Request) -> web.Response:
    def is_free_space(path: str, required_mb: int) -> bool:
        total, used, free = shutil.disk_usage(path)
        free_mb = free // (1024 * 1024)
        return free_mb >= required_mb

    task_index = request.headers.get("task_index", "unknown")
    data: dict = await request.json()
    print(f"[HDD] Received request with task_index={task_index} size_mb={data.get('size_mb')}")
    size_mb = data.get("size_mb", 100)

    path =  "/app/data/hdd_test_file.dat"

    # check disk space before writing
    if not is_free_space(Path(path).parent.as_posix(), size_mb):
        print(f"[HDD] Not enough disk space for task_index={task_index}, required={size_mb} MB")
        return web.json_response({"error": "not enough disk space", "task_index": task_index}, status=507)
    
    # record hdd_free
    hdd_free_before = shutil.disk_usage(Path(path).parent.as_posix()).free // (1024 * 1024)
    
    # calculate the processed time in thread pool
    written_size, computation_time = await hdd_write_load(path, size_mb)
    processing_time = computation_time
    return web.json_response({"result": written_size, "task_index": task_index, "processing_time": processing_time, "hdd_free_before": hdd_free_before})

HDD_LOCK = Lock()

def init_app():
    app = web.Application()
    app.router.add_post('/cpu/', handle_request_cpu)
    app.router.add_post('/mem/', handle_request_mem)
    app.router.add_post('/hdd/', handle_request_hdd)
    return app
if __name__ == '__main__':
    with open('/sys/fs/cgroup/memory.max', 'r') as f:
        free_memory_mb = int(f.read().strip()) // (1024 * 1024)
        print(f"[MEM] Free memory: {free_memory_mb} MB")
    
    app = init_app()
    web.run_app(app, host='0.0.0.0', port=8080)