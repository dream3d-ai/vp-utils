import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated, Optional

import invoke
import typer
from loguru import logger
from rich.console import Console
from rich.table import Table
from typer import Argument, Option, Typer

from vp.api import VPCloud
from vp.torchrun import SSHConnection, SSHNode, tail_job
from vp.torchrun import submit as torchrun_submit
from vp.utils.name import generate_friendly_name

cli = Typer(
    pretty_exceptions_show_locals=False,
    add_completion=True,
)


@cli.command(rich_help_panel="Launch")
def run(
    command: Annotated[list[str], Argument(help="The command to run on the cluster")],
    cluster_id: Annotated[
        str | None,
        Option("--cluster-id", help="The id of the cluster to run the job on"),
    ] = None,
    job_id: Annotated[
        Optional[str], Option("--job-id", help="The id of the job to run")
    ] = None,
    job_name: Annotated[
        Optional[str], Option("--job-name", help="The name of the job to run")
    ] = None,
    max_restarts: Annotated[
        int,
        Option("--max-restarts", help="The maximum number of times to restart the job"),
    ] = 0,
    ssh_key_path: Annotated[
        str,
        Option(
            "--ssh-key-path", help="The path to a public SSH key to use for the machine"
        ),
    ] = os.path.join(os.path.expanduser("~"), ".ssh", "vp_key.pub"),
    runtime_env: Annotated[
        Optional[str],
        Option(
            "--runtime-env",
            help="The filepath to a yaml file containing the runtime environment variables",
        ),
    ] = "runtime_env.yaml",
    filter_lines: Annotated[
        str | None,
        Option(
            "--filter-lines",
            help="A regex pattern that filters lines out from the output log",
        ),
    ] = None,  # r"\[swscaler @ 0x[\d|a-f]+\] deprecated pixel format used, make sure you did set range correctly",
):
    """Run a command on an existing VP cluster."""

    vp = VPCloud(runtime_env=runtime_env, ssh_key_path=ssh_key_path)

    if cluster_id is None:
        clusters = [
            cluster
            for cluster in vp.clusters.values()
            if (
                cluster.status == "Running"
                and not any(job.running for job in vp.jobs[cluster.cluster_id])
            )
        ]

        assert len(clusters) != 0, (
            "No free clusters found, please deploy a new cluster or kill an existing job"
        )
        cluster_id = str(clusters[0].cluster_id)
    else:
        cluster_id = vp.status_by_ids([cluster_id])[0]

    logger.opt(colors=True).info(
        f"Running job on cluster <blue>{vp.clusters[cluster_id].name}</blue>"
    )

    job_id = job_id or uuid.uuid4().hex

    pids = torchrun_submit(
        job_id=job_id,
        job_name=job_name or generate_friendly_name(),
        nodes=vp.ssh_cluster(cluster_id, ssh_key_path),
        command=" ".join(command),
        max_restarts=max_restarts,
        runtime_env=runtime_env,
    )

    print()
    logs(job_id, filter_lines, ssh_key_path, runtime_env, tail=True)
    return pids


@cli.command(rich_help_panel="Launch")
def resume(
    job_id: Annotated[
        Optional[str], Option("--job-id", help="The id of the job to run")
    ] = None,
    max_restarts: Annotated[
        int,
        Option("--max-restarts", help="The maximum number of times to restart the job"),
    ] = 0,
    ssh_key_path: Annotated[
        str,
        Option(
            "--ssh-key-path", help="The path to a public SSH key to use for the machine"
        ),
    ] = os.path.join(os.path.expanduser("~"), ".ssh", "vp_key.pub"),
    runtime_env: Annotated[
        Optional[str],
        Option(
            "--runtime-env",
            help="The filepath to a yaml file containing the runtime environment variables",
        ),
    ] = "runtime_env.yaml",
    filter_lines: Annotated[
        str | None,
        Option(
            "--filter-lines",
            help="A regex pattern that filters lines out from the output log",
        ),
    ] = None,  # r"\[swscaler @ 0x[\d|a-f]+\] deprecated pixel format used, make sure you did set range correctly",
):
    """Run a command on an existing VP cluster."""

    vp = VPCloud(runtime_env=runtime_env, ssh_key_path=ssh_key_path)
    cluster_id, job = vp.get_cluster_id_by_job_id(job_id)

    new_job_id = uuid.uuid4().hex

    pids = torchrun_submit(
        job_id=new_job_id,
        job_name=job.job_name,
        nodes=vp.ssh_cluster(cluster_id, ssh_key_path),
        command=job.command,
        max_restarts=max_restarts,
        runtime_env=runtime_env,
    )

    print()
    logs(new_job_id, filter_lines, ssh_key_path, runtime_env, tail=True)
    return pids


@cli.command(rich_help_panel="Terminate")
def kill(
    job_ids: Annotated[list[str], Argument(help="The id of the job to kill")],
    clean: Annotated[
        bool, Option("--clean", help="Clean up the job directory after killing the job")
    ] = False,
    quiet: Annotated[
        bool, Option("--quiet", help="Do not show more information about the cluster")
    ] = False,
    ssh_key_path: Annotated[
        str,
        Option(
            "--ssh-key-path", help="The path to a public SSH key to use for the machine"
        ),
    ] = os.path.join(os.path.expanduser("~"), ".ssh", "vp_key.pub"),
    runtime_env: Annotated[
        Optional[str],
        Option(
            "--runtime-env",
            help="The filepath to a yaml file containing the runtime environment variables",
        ),
    ] = "runtime_env.yaml",
):
    """Kill a job on an existing VP cluster."""
    for job_id in job_ids:
        vp = VPCloud(runtime_env=runtime_env, ssh_key_path=ssh_key_path)
        cluster_id = next(
            (
                cluster_id
                for cluster_id, jobs in vp.jobs.items()
                if any(job.job_id.startswith(job_id) for job in jobs)
            ),
            None,
        )
        if cluster_id is None:
            raise typer.BadParameter(f"No job found with id {job_id}")

        job = next(
            (job for job in vp.jobs[cluster_id] if job.job_id.startswith(job_id))
        )

        if not typer.confirm(f"Are you sure you want to terminate {job.job_id}?"):
            typer.echo("Aborted.")
            raise typer.Exit()

        def kill_job(node: SSHNode):
            assert job is not None  # Typing
            with SSHConnection(node) as connection:
                try:
                    connection.run(f'pkill -f "{job.command}"', hide=True)
                    connection.run(f"kill {job.pid}", hide=True)
                except invoke.exceptions.UnexpectedExit:
                    pass

                logger.opt(colors=True).info(
                    f"Killed job <blue>{job.job_id}</blue> on node <blue>{cluster_id}</blue>: <green>{node.public_ip}</green> with pid <blue>{job.pid}</blue>"
                )

        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(kill_job, vp.ssh_cluster(cluster_id, ssh_key_path))

    if clean:
        vp.clear_stale_jobs(cluster_id)

    if not quiet:
        jobs(ssh_key_path=ssh_key_path, runtime_env=runtime_env)


@cli.command(name="list", rich_help_panel="Monitor")
def jobs(
    cluster_id: Annotated[
        Optional[str], Option(help="The id of the cluster to get the status of")
    ] = None,
    ssh_key_path: Annotated[
        str,
        Option(
            "--ssh-key-path", help="The path to a public SSH key to use for the machine"
        ),
    ] = os.path.join(os.path.expanduser("~"), ".ssh", "vp_key.pub"),
    runtime_env: Annotated[
        Optional[str],
        Option(
            "--runtime-env",
            help="The filepath to a yaml file containing the runtime environment variables",
        ),
    ] = "runtime_env.yaml",
):
    """Get the status of a VP node."""

    vp = VPCloud(runtime_env=runtime_env, ssh_key_path=ssh_key_path)
    if cluster_id is None:
        cluster_ids = list(vp.clusters.keys())
    else:
        cluster_ids = vp.status_by_ids([cluster_id])

    jobs = vp.jobs
    table = Table(title="VP Node Jobs")

    table.add_column("Node ID", style="yellow", no_wrap=True)
    table.add_column("Node name", style="white")
    table.add_column("Job ID", style="magenta")
    table.add_column("Job Name", style="white")
    table.add_column("Created", style="bright_blue")
    table.add_column("PID", style="grey74", justify="right")
    table.add_column("Status", style="green")
    table.add_column("Command", style="white")

    for cluster_id in cluster_ids:
        if cluster_id not in jobs:
            continue

        for job in jobs[cluster_id]:
            table.add_row(
                cluster_id,
                vp.clusters[cluster_id].name,
                job.job_id,
                f"[link=https://wandb.ai/dream3d/dream-vae/runs/{job.job_name}]{job.job_name}[/link]",
                job.creation_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                str(job.pid),
                "[green]Running[/green]" if job.running else "[red]Stopped[/red]",
                job.command,
            )
    console = Console()
    console.print(table)


@cli.command(rich_help_panel="Monitor")
def logs(
    job_id: Annotated[str | None, Argument(help="The id of the job to tail")] = None,
    tail: Annotated[bool, Option("--tail", help="Tail the logs")] = False,
    filter_lines: Annotated[
        str | None,
        Option(
            "--filter-lines",
            help="A regex pattern that filters lines out from the output log",
        ),
    ] = None,
    ssh_key_path: Annotated[
        str,
        Option(
            "--ssh-key-path", help="The path to a public SSH key to use for the machine"
        ),
    ] = os.path.join(os.path.expanduser("~"), ".ssh", "vp_key.pub"),
    runtime_env: Annotated[
        Optional[str],
        Option(
            "--runtime-env",
            help="The filepath to a yaml file containing the runtime environment variables",
        ),
    ] = "runtime_env.yaml",
):
    """Tail the logs of a job on an existing VP cluster."""

    vp = VPCloud(runtime_env=runtime_env)
    cluster_id, job = vp.get_cluster_id_by_job_id(job_id)
    logger.info(f"Tailing job {job.job_id} on cluster {cluster_id}")
    tail_job(job.job_id, filter_lines, vp.ssh_cluster(cluster_id, ssh_key_path)[0])
