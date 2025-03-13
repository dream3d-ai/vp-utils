import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Literal

import invoke
import requests
from typer import Typer

from vp.core.torchrun import REMOTE_WORKING_DIR, SSHConnection, SSHNode, get_result

cli = Typer(
    pretty_exceptions_show_locals=False,
    add_completion=False,
    chain=True,
)


class MachineType(Enum):
    BARE_METAL = "bare-metal"
    VIRTUAL = "virtual"

    def endpoint(self) -> str:
        match self:
            case MachineType.BARE_METAL:
                return "bare-metal"
            case MachineType.VIRTUAL:
                return "virtual-machines"
            case _:
                raise ValueError(f"Invalid machine type: {self}")

    def pretty(self) -> str:
        match self:
            case MachineType.BARE_METAL:
                return "Bare Metal"
            case MachineType.VIRTUAL:
                return "Virtual Machine"
            case _:
                raise ValueError(f"Invalid machine type: {self}")


@dataclass
class VPClusterStatus:
    cluster_id: str
    name: str
    public_ips: list[str]
    private_ips: list[str]
    creation_timestamp: datetime
    status: Literal["Running", "Pending", "Failed"]
    power_status: Literal["Running"]
    node_count: int
    gpu_model: str
    gpu_count: int
    cpu_model: str
    cpu_count: int
    ram_gb: int
    storage_gb: int
    rate_hourly: float
    network_type: Literal["ethernet", "infiniband"]
    machine_type: MachineType

    def __post_init__(self):
        assert (len(self.public_ips) == len(self.private_ips) == 0) or (
            len(self.public_ips) == len(self.private_ips) == self.node_count
        )


@dataclass
class VPJob:
    job_id: str
    job_name: str
    command: str
    pid: int
    running: bool
    creation_timestamp: datetime


def temporary_property_cache(cache_time: float = 15):
    """
    Calls to a property will be cached for the given amount of time before being recalculated.
    Reduces the number of network requests to the VP API.
    """

    def wrapper(func) -> property:
        def wrapped(self):
            if not hasattr(self, "_temp_prop_cache"):
                self._temp_prop_cache = {}

            if (
                func.__name__ not in self._temp_prop_cache
                or time.perf_counter() - self._temp_prop_cache[func.__name__][1]
                > cache_time
            ):
                self._temp_prop_cache[func.__name__] = (func(self), time.perf_counter())

            return self._temp_prop_cache[func.__name__][0]

        return property(wrapped)

    return wrapper


class VPCloud:
    def __init__(
        self,
        api_key: str | None = None,
        ssh_key_path: str | None = os.path.join(
            os.path.expanduser("~"), ".ssh", "vp_key.pub"
        ),
    ):
        self.base_url = "https://cloud-api.voltagepark.com/api/v1"
        self.api_key = api_key or os.environ.get("VP_API_KEY")
        self.ssh_key_path = ssh_key_path

        if self.api_key is None:
            raise ValueError("VP_API_KEY is not set")

    def get_all(
        self,
        endpoint: str,
        limit: int = 15,
        max_entries: int = 120,
        authorize: bool = True,
    ) -> list:
        def _get_all_rec(limit: int = limit, offset: int = 0) -> list:
            if offset > max_entries:
                raise Exception("Maximum limit reached")

            request = self._submit_request(
                "GET", endpoint, limit=limit, offset=offset, authorize=authorize
            )
            assert isinstance(request, dict)

            if not request["has_next"]:
                return request["results"]

            return request["results"] + _get_all_rec(limit, offset + limit)

        return _get_all_rec()

    @property
    def ssh_key(self) -> str | None:
        if self.ssh_key_path is None:
            return None

        with open(self.ssh_key_path, "r") as f:
            return f.read().strip()

    @temporary_property_cache(cache_time=15)
    def clusters(self) -> dict[str, VPClusterStatus]:
        # TODO: Uncomment when virtual machine
        bare_metal = self.get_all("bare-metal/")
        # virtual = self.get_all("virtual-machines/")

        for machine in bare_metal:
            machine["machine_type"] = MachineType.BARE_METAL
        # for machine in virtual:
        #     machine["machine_type"] = MachineType.VIRTUAL

        clusters = {
            cluster["id"]: VPClusterStatus(
                cluster_id=cluster["id"],
                public_ips=[
                    node.get("public_ip", "")
                    for node in cluster.get("node_networking", [])
                ],
                private_ips=[
                    node.get("private_ip", "")
                    for node in cluster.get("node_networking", [])
                ],
                name=cluster["name"],
                creation_timestamp=datetime.fromisoformat(
                    cluster["creation_timestamp"]
                ).astimezone(timezone(timedelta(hours=-5), "EST")),
                status=cluster.get("status", "Unknown"),
                power_status=cluster.get("power_status", "Unknown"),
                node_count=cluster.get("node_count", 1),
                gpu_model=cluster.get("specs_per_node", {}).get("gpu_model", "Unknown"),
                gpu_count=cluster.get("specs_per_node", {}).get("gpu_count", 1),
                cpu_model=cluster.get("specs_per_node", {}).get("cpu_model", "Unknown"),
                cpu_count=cluster.get("specs_per_node", {}).get("cpu_count", 0),
                ram_gb=cluster.get("specs_per_node", {}).get("ram_gb", 0),
                storage_gb=cluster.get("specs_per_node", {}).get("storage_gb", 0),
                rate_hourly=cluster.get("rate_hourly", 0),
                network_type=cluster.get("network_type", "Unknown"),
                machine_type=MachineType.BARE_METAL,  # TODO: Add virtual
            )
            for cluster in bare_metal  # + virtual
        }
        return dict(
            sorted(clusters.items(), key=lambda item: item[1].creation_timestamp)
        )

    @temporary_property_cache(cache_time=600)
    def locations(self) -> dict[str, dict]:
        return {
            location["id"]: location
            for location in self.get_all("bare-metal/locations/", authorize=False)
        }

    @temporary_property_cache(cache_time=15)
    def jobs(self) -> dict[str, list[VPJob]]:
        # Get job info for a single node
        def get_jobs(cluster_id: str):
            delimiter = "_delimiter_"
            # Jobs should be equivalent across all nodes within a cluster
            with SSHConnection(
                self.ssh_cluster(cluster_id, self.ssh_key_path)[0]
            ) as connection:
                try:
                    job_ids = get_result(
                        connection, f"ls {REMOTE_WORKING_DIR}"
                    ).splitlines()
                except invoke.exceptions.UnexpectedExit:
                    job_ids = []

                def get_job_data(job_id):
                    # Get pid, status, and metadata for a single job
                    try:
                        return get_result(
                            connection,
                            f"pid=$(cat {REMOTE_WORKING_DIR}/{job_id}/job.pid) && "
                            f'status=$(ps -p $pid > /dev/null 2>&1 && echo "1" || echo "0") && '
                            f"metadata=$(cat {REMOTE_WORKING_DIR}/{job_id}/job.json) && "
                            f"echo $pid {delimiter} $status {delimiter} $metadata",
                        ).split(f" {delimiter} ")
                    except invoke.exceptions.UnexpectedExit:
                        return None

                with ThreadPoolExecutor(max_workers=4) as executor:
                    # Get pid, status, and metadata for all jobs
                    return cluster_id, zip(job_ids, executor.map(get_job_data, job_ids))

        with ThreadPoolExecutor(max_workers=4) as executor:
            # Get pid, status, and metadata for all jobs
            cluster_jobs = executor.map(
                get_jobs,
                (
                    cluster_id
                    for cluster_id, cluster in self.clusters.items()
                    if cluster.status == "Running"
                ),
            )

        def parse_job(job_info: tuple[str, list[str] | None]):
            job_id, info = job_info
            if info is None:
                return VPJob(
                    job_id=job_info[0],
                    job_name="",
                    command="",
                    pid=0,
                    running=False,
                    creation_timestamp=datetime(
                        1970, 1, 1, tzinfo=timezone(timedelta(hours=-5), "EST")
                    ),
                )

            # Process each job into a VPJob
            pid, status, metadata = info
            pid, status, metadata = int(pid), status == "1", json.loads(metadata)

            job_name = metadata["job_name"]
            command = metadata["command"]
            creation_timestamp = datetime.fromisoformat(
                metadata["creation_timestamp"]
            ).astimezone(timezone(timedelta(hours=-5), "EST"))

            return VPJob(
                job_id=job_id,
                job_name=job_name,
                command=command,
                pid=pid,
                running=status,
                creation_timestamp=creation_timestamp,
            )

        jobs = {
            cluster_id: [parse_job(job) for job in jobs]
            for cluster_id, jobs in cluster_jobs
        }

        jobs = {
            cluster_id: sorted(jobs, key=lambda job: job.creation_timestamp)
            for cluster_id, jobs in jobs.items()
        }

        return dict(
            sorted(
                jobs.items(), key=lambda item: self.clusters[item[0]].creation_timestamp
            )
        )

    def status_by_ids(self, ids: list[str] | None = None) -> list[str]:
        if ids is None:
            return list(self.clusters.keys())

        # Find all clusters starting with the provided ids
        ids_ = [
            [key for key in self.clusters.keys() if key.startswith(id_)] for id_ in ids
        ]

        for i, i_ in zip(ids, ids_):
            if len(i_) == 0:
                raise ValueError(f"No cluster found with id {i}")
            if len(i_) > 1:
                raise ValueError(f"Multiple clusters found starting with {i}")

        return [i[0] for i in ids_]

    def get_cluster_id_by_job_id(self, job_id: str | None = None) -> tuple[str, VPJob]:
        if job_id is None:
            jobs = [
                (cluster_id, job)
                for cluster_id, jobs in self.jobs.items()
                for job in jobs
            ]
            jobs = sorted(jobs, key=lambda job: job[1].creation_timestamp)
            return jobs[-1]

        cluster_ids = [
            cluster_id
            for cluster_id, jobs in self.jobs.items()
            if any(job.job_id.startswith(job_id) for job in jobs)
        ]
        if len(cluster_ids) == 0:
            raise ValueError(f"No clusters found starting with job id {job_id}")
        if len(cluster_ids) > 1:
            raise ValueError(
                f"Multiple clusters found starting with job id {job_id}. Please specify full job id"
            )

        job = next(
            job for job in self.jobs[cluster_ids[0]] if job.job_id.startswith(job_id)
        )
        return cluster_ids[0], job

    def ssh_cluster(
        self, cluster_id: str, ssh_key_path: str | None = None
    ) -> list[SSHNode]:
        return [
            SSHNode(
                public_ip=public_ip,
                private_ip=private_ip,
                num_gpus=self.clusters[cluster_id].gpu_count,
                nproc=self.clusters[cluster_id].cpu_count,
                ssh_pub_key_path=ssh_key_path,
                network_type=self.clusters[cluster_id].network_type,
            )
            for public_ip, private_ip in zip(
                self.clusters[cluster_id].public_ips,
                self.clusters[cluster_id].private_ips,
            )
        ]

    def terminate(self, ids: list[str]):
        for id_ in ids:
            if id_ not in self.clusters:
                raise ValueError(f"No cluster found with id {id_}")

        for id_ in ids:
            endpoint = self.clusters[id_].machine_type.endpoint()
            self._submit_request("DELETE", f"{endpoint}/{id_}")

    def deploy(
        self,
        machine_type: MachineType,
        location: str,
        gpu_count: int,
        name: str,
        network_type: Literal["ethernet", "infiniband"] = "ethernet",
    ):
        if machine_type == MachineType.VIRTUAL:
            raise NotImplementedError("Virtual machine deploying is not yet supported")

        response = self._submit_request(
            "POST",
            "bare-metal/",
            headers={"Content-Type": "application/json"},
            json={
                "location_id": location,
                "gpu_count": gpu_count,
                "name": name,
                "organization_ssh_keys": {"mode": "none"},
                "ssh_keys": [self.ssh_key],
                "network_type": network_type,
            },
        )

        assert isinstance(response, dict)
        return response

    def clear_stale_jobs(self, cluster_id: str | None = None):
        for _cluster_id, job in self.jobs.items():
            if cluster_id is not None and _cluster_id != cluster_id:
                continue

            def clean_device(node: SSHNode):
                with SSHConnection(node) as connection:
                    for job_ in job:
                        if job_.running:
                            continue

                        connection.run(f"rm -rf {REMOTE_WORKING_DIR}/{job_.job_id}")

            with ThreadPoolExecutor(max_workers=4) as executor:
                executor.map(
                    clean_device, self.ssh_cluster(_cluster_id, self.ssh_key_path)
                )

    def _submit_request(
        self,
        method: Literal["GET", "DELETE", "POST"],
        endpoint: str,
        headers: dict[str, str] = {},
        authorize: bool = True,
        json: dict | None = None,
        **params,
    ):
        if authorize:
            headers["Authorization"] = f"Bearer {self.api_key}"

        response = requests.request(
            method,
            f"{self.base_url}/{endpoint}",
            headers=headers,
            params=params,
            json=json,
        )

        match response.status_code:
            case 422:
                raise Exception(
                    f"Validation error to {endpoint}: {response.json().get('detail', 'No detail available')}"
                )
            case 400:
                raise Exception(
                    f"Bad request to {endpoint}: {response.json().get('detail', 'No detail available')}"
                )
            case 200 | 201 | 202:
                return response.json()
            case _:
                raise Exception(
                    f"Unknown error occurred: {response.status_code}. {response}"
                )
