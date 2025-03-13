"""
Utility file submits a torchrun job to a cluster through SSH.
"""

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Literal

from fabric import Connection
from loguru import logger

from vp.utils.archive import WorkingDirectoryArchiver

REMOTE_WORKING_DIR = "/tmp/torchrun/jobs"
LOCAL_CACHE_DIR = os.path.expanduser("~/.cache/vp/jobs")


@dataclass(kw_only=True)
class SSHNode:
    public_ip: str
    private_ip: str | None = None
    num_gpus: int
    nproc: int
    ssh_pub_key_path: str | None = None
    network_type: Literal["infiniband", "ethernet"] = "ethernet"


class SSHConnection:
    def __init__(self, node: SSHNode):
        self.node = node

    def __enter__(self):
        self.connection = Connection(
            host=self.node.public_ip,
            connect_kwargs={
                "key_filename": self.node.ssh_pub_key_path,
            },
        )
        self.connection.open()
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()


def torchrun_single_node_command(
    node: SSHNode,
    max_restarts: int = 0,
    runtime_env: str | None = None,
):
    import yaml

    if runtime_env is not None:
        with open(runtime_env, "r") as file:
            env_vars = yaml.safe_load(file)
    else:
        env_vars = {}

    return (
        f"OMP_NUM_THREADS={node.nproc // node.num_gpus} "
        f"{' '.join(f'{k}={v}' for k, v in env_vars.items())} "
        f"nohup torchrun "
        f"--nproc_per_node={node.num_gpus} "
        f"--nnodes=1 "
        f"--rdzv-backend=c10d "
        f"--max-restarts={max_restarts} "
        f"--no-python "
    )


def torchrun_multi_node_command(
    nodes: list[SSHNode],
    job_id: str,
    max_restarts: int = 0,
    runtime_env: str | None = None,
) -> list[str]:
    import yaml

    if runtime_env is not None:
        with open(runtime_env, "r") as file:
            env_vars = yaml.safe_load(file)
    else:
        env_vars = {}

    master_node = nodes[0]
    port = random.randint(29400, 29499)

    formatted_runtime_env = " ".join(f"{k}={v}" for k, v in env_vars.items())
    ib_devices = [
        "mlx5_0",
        "mlx5_3",
        "mlx5_4",
        "mlx5_5",
        "mlx5_6",
        "mlx5_9",
        "mlx5_10",
        "mlx5_11",
    ]
    infiniband_nccl_options = (
        "NCCL_IB_HCA=" + ",".join(ib_devices) + " "
        "UCX_NET_DEVICES=" + ",".join([f"{d}:1" for d in ib_devices]) + " "
        "SHARP_COLL_ENABLE_PCI_RELAXED_ORDERING=1 "
        "NCCL_COLLNET_ENABLE=0 "
    )

    return [
        (
            f"OMP_NUM_THREADS={node.nproc // node.num_gpus} "
            f"{formatted_runtime_env} "
            f"{infiniband_nccl_options} "
            f"nohup torchrun "
            f"--nnodes={len(nodes)} "
            f"--node_rank={rank} "
            f"--nproc-per-node={node.num_gpus} "
            f"--max-restarts={max_restarts} "
            f"--local-addr={node.private_ip} "
            f"--rdzv_endpoint={'localhost' if rank == 0 else master_node.private_ip}:{port} "
            "--rdzv_backend=c10d "
            f"--rdzv-id={job_id} "
            "--no-python"
        )
        for rank, node in enumerate(nodes)
    ]


def get_result(connection: Connection, command: str) -> str:
    result = connection.run(command, hide=True)
    return result.stdout.strip()  # type: ignore


def submit(
    job_id: str,
    job_name: str,
    nodes: list[SSHNode],
    command: str,
    max_restarts: int = 0,
    runtime_env: str | None = None,
):
    # Archive the current working directory
    archiver = WorkingDirectoryArchiver(
        job_id, LOCAL_CACHE_DIR, job_name=job_name, command=command
    )
    archived_dir = archiver.archive(os.getcwd())
    logger.opt(colors=True).info(
        f"Archived working directory to <blue>{archived_dir}</blue>"
    )

    def submit_node(node: SSHNode, torchrun_command: str):
        remote_working_dir = f"{REMOTE_WORKING_DIR}/{job_id}"
        with SSHConnection(node) as connection:
            # Setup remote environment
            connection.run(f"mkdir -p {remote_working_dir}")
            connection.put(archived_dir, f"{remote_working_dir}/archived_dir.zip")
            logger.opt(colors=True).info(
                f"Uploaded working directory to <blue>{node.public_ip}</blue>"
            )

            connection.run(f"cd {remote_working_dir} && unzip -q -o archived_dir.zip")
            connection.run(f"touch {remote_working_dir}/output.log")
            logger.info("Unpacked working directory")

            # Execute job using torchrun
            connection.run(
                # Setup shell
                f"source ~/.profile && "
                f"cd {remote_working_dir} && "
                # Submit job
                f"LOGURU_FILE={remote_working_dir}/output.log "
                f"EXPERIMENT_NAME={job_name} "
                f"{torchrun_command} {command} > "
                # Pipe output to log file (filtering out annoying logs)
                f"{remote_working_dir}/output.log 2>&1 & "
                # Write pid to file and wait for job to finish
                f"pid=$!; "
                f"echo $pid > {remote_working_dir}/job.pid; "
                f"wait $pid; "
                f"echo $? > {remote_working_dir}/exit_code",
                disown=True,
            )

            # Get the job pid
            pid = int(get_result(connection, f"cat {remote_working_dir}/job.pid"))
            logger.opt(colors=True).info(
                f"Submitted job <blue>{job_name}</blue> to <blue>{node.public_ip}</blue> with pid <blue>{pid}</blue>"
            )
            return pid

    if len(nodes) == 1:
        commands = [torchrun_single_node_command(nodes[0], max_restarts, runtime_env)]
    else:
        commands = torchrun_multi_node_command(nodes, job_id, max_restarts, runtime_env)

    pids = [submit_node(node, cmd) for node, cmd in zip(nodes, commands)]
    return pids


def tail_job(
    job_id: str, filter_lines: str | None, node: SSHNode, sync_interval: int = 600
):
    remote_working_dir = f"/tmp/torchrun/jobs/{job_id}"

    with SSHConnection(node) as connection:

        def tail():
            connection.run(
                f"tail --lines=+0 -f {remote_working_dir}/output.log"
                + (" | grep -v '{filter_lines}'" if filter_lines is not None else "")
            )

        def sync():
            while True:
                connection.get(
                    f"{remote_working_dir}/output.log",
                    f"{LOCAL_CACHE_DIR}/{job_id}/output.log",
                )
                time.sleep(sync_interval)

        with ThreadPoolExecutor(max_workers=2) as executor:
            tail_future = executor.submit(tail)
            sync_future = executor.submit(sync)

            tail_future.result()
            sync_future.result()
