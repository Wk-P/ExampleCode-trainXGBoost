from typing import TypedDict, Any
import asyncio
import traceback
import concurrent.futures
import paramiko             # type: ignore
from aiohttp import ClientTimeout
import aiohttp
from docker_utils.utils import get_stack_services, get_stack_services_resources_limits, get_docker_node_list, get_service_port_list
from pathlib import Path

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
            with open(Path(__file__).parent / 'stack_name', 'r', encoding='utf-8') as f:
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

        # Build a unified workers list from discovered resource limits and ports.
        # Each entry contains node/service info and the request URL to use.
        self.workers = []

        # map service_name -> published port (first seen)
        port_map: dict[str, int] = {}
        for ports in self.post_list:
            if ports and isinstance(ports, list) and ports[0].get('ServiceName'):
                svc = ports[0].get('ServiceName')
                port_map.setdefault(svc, ports[0].get('PublishedPort'))

        # prefer using worker_resources_limits_list as authoritative mapping of service -> node
        for limits_info in self.worker_resources_limits_list:
            svc_name = limits_info.get('service_name')
            for limit in limits_info.get('limits', []):
                node_ip = limit.get('node_host_ip', '')
                pub_port = port_map.get(svc_name)
                if pub_port:
                    url = f"http://{node_ip}:{pub_port}"
                else:
                    # if no published port known, fallback to bare node ip
                    url = f"http://{node_ip}"

                self.workers.append({
                    'service_name': svc_name,
                    'node_id': limit.get('node_id'),
                    'node_hostname': limit.get('node_hostname'),
                    'node_host_ip': node_ip,
                    'published_port': pub_port,
                    'url': url,
                    'cpus_limit_cores': limit.get('cpus_limit_cores'),
                    'memory_limit_mb': limit.get('memory_limit_mb'),
                    'hdd_limit_mb': limit.get('hdd_limit_mb'),
                })

        # If no workers discovered from limits, fallback to zipping node_list and post_list
        if not self.workers:
            for node, port in zip(self.node_list, self.post_list):
                if not port:
                    continue
                svc = port[0].get('ServiceName')
                pub = port[0].get('PublishedPort')
                node_addr = node.get('addr')
                self.workers.append({
                    'service_name': svc,
                    'node_id': node.get('id'),
                    'node_hostname': node.get('hostname'),
                    'node_host_ip': node_addr,
                    'published_port': pub,
                    'url': f"http://{node_addr}:{pub}",
                    'cpus_limit_cores': 0,
                    'memory_limit_mb': 0,
                    'hdd_limit_mb': None,
                })

        # Build backward-compatible structures used elsewhere
        self.worker_urls = [{ 'url': w['url'], 'service_name': w['service_name'] } for w in self.workers]

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
        

        # create WorkerSSHClient instances based on unified workers list
        self.worker_ssh_clients: list[WorkerSSHClient] = []
        for w in self.workers:
            hostname = w.get('node_host_ip') or self.find_hostip_by_servicename(w.get('service_name'))
            try:
                self.worker_ssh_clients.append(
                    WorkerSSHClient(
                        w.get('url'),
                        ssh_client=paramiko.SSHClient(),
                        hostname=hostname,
                        port=22,
                        username="pi",
                        password="raspberrypi"
                    )
                )
            except Exception as exc:
                print(f"Failed to create WorkerSSHClient for {w.get('service_name')}@{hostname}: {exc}")

    def find_hostip_by_servicename(self, service_name: str) -> str:
        # First try unified workers list
        if hasattr(self, 'workers'):
            for w in self.workers:
                if w.get('service_name') == service_name:
                    ip = w.get('node_host_ip')
                    if ip:
                        print(f"[find_hostip_by_servicename] Found host IP from workers: {ip} for service: {service_name}")
                        return ip

        # Fallback to original resource limits list
        for limits in self.worker_resources_limits_list:
            if limits['service_name'] == service_name:
                limits_inner = limits.get('limits', [])
                if limits_inner:
                    print(f"[find_hostip_by_servicename] Found host IP from limits: {limits_inner[0].get('node_host_ip')} for service: {service_name}")
                    return limits_inner[0].get('node_host_ip')

        # Final fallback to first node address
        if self.node_list:
            return self.node_list[0].get('addr', '')
        return ''

    async def get_and_update_round_robin_index(self) -> int:
        async with self.round_robin_index_lock:
            index = self.round_robin_index
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
    
if __name__ == "__main__":
    # test code
    manager = ServerManager()
    for i in range(len(manager.worker_ssh_clients)):
        client = manager.worker_ssh_clients[i]
        print(f"Worker {i}: {client.hostname}, Mem Usage: {client.mem_usage}/{client.max_mem_usage}, HDD Usage: {client.hdd_usage}/{client.max_hdd_usage}")