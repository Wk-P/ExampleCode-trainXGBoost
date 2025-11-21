import trace
from typing import TypedDict, Any
import asyncio
import traceback
import concurrent.futures
from webbrowser import get
from xml.etree.ElementInclude import include
import paramiko             # type: ignore
from aiohttp import ClientTimeout
import aiohttp
from docker_utils.utils import get_stack_services, get_stack_services_resources_limits, get_docker_node_list, get_service_port_list

class ManagerResponse(TypedDict):
    task_index: str
    task_class: str
    error: int
    message: str
    success: Any
    response_data: Any


class WorkerSSHClient:
    def __init__(self, server_url: str, ssh_client: paramiko.SSHClient, hostname: str, port: int, username: str, password: str):
        self.server_url = server_url
        self.ssh_client = ssh_client
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.mem_usage = 0
        self.max_mem_usage = 0

        self.hdd_usage = 0  
        self.max_hdd_usage = 0

        self.hdd_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.mem_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    
        print(f"Connecting SSH to {self.hostname}:{self.port}")

        try:
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(hostname=self.hostname, port=self.port, username=self.username, password=self.password)

            print(f"SSH connected to {self.hostname}:{self.port}")
            print(f"SSH client: {self.ssh_client}")
        except Exception as e:
            print(f"Failed to connect SSH to {self.hostname}:{self.port} - {e}")
            traceback.print_exc()
            exit(1)
    # for get worker node resource usage
    async def hdd_usage_handle(self):
        def _sync_hdd_check():
            cmd = """df -B1"""
            try:
                stdin, stdout, stderr = self.ssh_client.exec_command(command=cmd)
                output = stdout.read().decode()
                
                total_blocks = 0
                current_blocks = 0
                for line in output.splitlines():
                    parts = line.split()
                    if len(parts) > 0 and parts[0].startswith('/dev'):
                        total_blocks += int(parts[1])
                        current_blocks += int(parts[2])
                return current_blocks, total_blocks
            except Exception as e:
                traceback.print_exc()
                print(f"HDD monitor error: {e}")
                return 0, 0
        
        while True:
            try:
                loop = asyncio.get_event_loop()
                current_blocks, total_blocks = await loop.run_in_executor(self.hdd_executor, _sync_hdd_check)
                self.hdd_usage = current_blocks
                self.max_hdd_usage = total_blocks
                
                await asyncio.sleep(1)  # delay interval to 1 second
            
            # Stop with error or interrupt # 오류 또는 중단 발생 시 종료
            except Exception as e:
                error_message = traceback.format_exc()
                print(error_message)
                exit(1)
            
    # for get worker node resource usage
    async def mem_usage_handle(self):
        def _sync_mem_check():
            cmd = """cat /proc/meminfo"""
            try:
                stdin, stdout, stderr = self.ssh_client.exec_command(command=cmd)
                output = stdout.read().decode()
                
                mem_info = {}
                for line in output.splitlines():
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip().split()[0]
                        mem_info[key] = int(value) * 1024

                mem_total = mem_info.get('MemTotal', 0)
                mem_free = mem_info.get('MemFree', 0)
                mem_used = mem_total - mem_free
                return mem_used, mem_total
            except Exception as e:
                traceback.print_exc()
                print(f"Memory monitor error: {e}")
                return 0, 0
        
        while True:
            try:
                loop = asyncio.get_event_loop()
                mem_used, mem_total = await loop.run_in_executor(self.mem_executor, _sync_mem_check)
                self.mem_usage = mem_used
                self.max_mem_usage = mem_total
                
                await asyncio.sleep(1)  # delay interval to 1 second

            except Exception as e:
                error_message = traceback.format_exc()
                print(f"Error occurred: {error_message}")
                exit(1)



# Round-robin server manager using itertools.cycle (no async lock needed)
class ServerManager:
    def __init__(self, **kwargs):
        # get worker resource limits
        # return (cpus_limit_cores, memory_limit_mb)
        if kwargs.get("stack_name") is None:
            with open('stack_name', 'r', encoding='utf-8') as f:
                self.stack_name = f.read().strip()
        else:
            self.stack_name = kwargs.get("stack_name")


        print(f"Load Stack Name: {self.stack_name}")

        self.worker_resources_limits_list = get_stack_services_resources_limits(stack_name=self.stack_name)
        self.node_list = get_docker_node_list()

        print(f"Worker Resources Limits for Workers...")
        print(f"Worker Resources Limits: {[res for res in self.worker_resources_limits_list]}")

        print(f"Get Docker Nodes...")
        print(f"Docker Nodes: {[node for node in self.node_list]}")

        self.post_list = []
        self.services_list = get_stack_services(stack_name=self.stack_name)
        print(f"Stack Services: {[service for service in self.services_list]}")

        for service in self.services_list:
            self.post_list.append(get_service_port_list(service_name=service['name']))

        print(f"Get Services Ports...")
        print(f"Services Ports: {self.post_list}")

        self.worker_urls = [
            {
                "url": f"http://{node['addr']}:{port[0]['PublishedPort']}",
                "service_name": port[0]['ServiceName']
            } for node, port in zip(self.node_list, self.post_list)
        ]
        
        print(f"Initialized ServerManager with {len(self.worker_urls)} worker nodes.")
        print(f"Worker URLs: {self.worker_urls}")

        self.http_clients = [
            aiohttp.ClientSession(
                timeout=ClientTimeout(total=0),
                connector=aiohttp.TCPConnector(limit=5000),
            ) for _ in self.worker_urls
        ]
        self.task_urls = {
            "HDD": "/hdd/",
            "MEM": "/mem/",
            "CPU": "/cpu/",
        }

        
        print(f"Load Stack Name: {self.stack_name}")
        print(f"Get Services Resources Limits for Workers...")
        print(f"Worker Services Names: {[res['service_name'] for res in self.worker_resources_limits_list]}")

        # cpu task round-robin index
        self.round_robin_index = 0
        self.round_robin_index_lock = asyncio.Lock()
        

        self.worker_ssh_clients: list[WorkerSSHClient] = [
            WorkerSSHClient(
                url, 
                ssh_client=paramiko.SSHClient(),
                hostname=self.find_hostip_by_servicename(url['service_name']),
                port=22,
                username="pi", 
                password="raspberrypi"
            ) for url in self.worker_urls
        ]

    def find_hostip_by_servicename(self, service_name: str) -> str:
        for limits in self.worker_resources_limits_list:
            if limits['service_name'] == service_name:
                # by limits list find node and service
                limits = limits['limits']
                
                print(f"[find_hostip_by_servicename] Found host IP: {limits[0]['node_host_ip']} for service: {service_name}")
                return limits[0]['node_host_ip']

        return self.node_list[0]['addr']    # default to first node addr if not found

    async def get_and_update_round_robin_index(self) -> int:
        index = self.round_robin_index
        async with self.round_robin_index_lock:
            self.round_robin_index = (self.round_robin_index + 1) % len(self.worker_urls)
        return index
    

    def check_worker_resources(self, worker_index: int, task_class: str) -> bool:
        worker_ssh_client = self.worker_ssh_clients[worker_index]
        if task_class == "HDD":
            w_usage = worker_ssh_client.hdd_usage
            w_max_usage = worker_ssh_client.max_hdd_usage
        elif task_class == "MEM":
            w_usage = worker_ssh_client.mem_usage
            w_max_usage = worker_ssh_client.max_mem_usage
        else:
            return True  # For CPU or other tasks, assume always enough resources

        usage_ratio = w_usage / w_max_usage if w_max_usage > 0 else 1.0
        # Define threshold for resource usage (e.g., 80%)
        threshold = 0.8
        return usage_ratio < threshold