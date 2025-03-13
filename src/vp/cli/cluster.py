import os
import time
from datetime import datetime, timedelta, timezone
from typing import Annotated, Optional

import typer
from loguru import logger
from rich.console import Console
from rich.table import Table
from typer import Argument, Option, Typer

from vp.core.api import MachineType, VPCloud
from vp.core.setup import setup_cluster
from vp.utils.logging import log_progress, slack_message
from vp.utils.name import generate_friendly_name

cli = Typer(
    pretty_exceptions_show_locals=False,
    add_completion=True,
)


@cli.command(name="up")
def deploy(
    machine_type: Annotated[
        MachineType, Argument(help="The type of machine to deploy")
    ] = MachineType.BARE_METAL.value,  # type: ignore
    gpu_count: Annotated[int, Argument(help="The number of GPUs to deploy")] = 8,
    location_id: Annotated[
        Optional[str],
        Option("--location", help="The id of the location to deploy the machine in"),
    ] = None,
    name: Annotated[
        Optional[str], Option("--name", help="The name of the machine to deploy")
    ] = None,
    ssh_key_path: Annotated[
        str,
        Option(
            "--ssh-key-path", help="The path to a public SSH key to use for the machine"
        ),
    ] = os.path.join(os.path.expanduser("~"), ".ssh", "vp_key.pub"),
    infiniband: Annotated[
        bool, Option("--infiniband", help="Use Infiniband instead of Ethernet")
    ] = False,
    runtime_env: Annotated[
        Optional[str],
        Option(
            "--runtime-env",
            help="The filepath to a yaml file containing the runtime environment variables",
        ),
    ] = "runtime_env.yaml",
    no_setup: Annotated[
        bool,
        Option(
            "--no-setup",
            help="Do not setup the cluster after deployment",
        ),
    ] = False,
) -> str:
    """Deploy a new VP cluster."""
    vp = VPCloud(runtime_env=runtime_env, ssh_key_path=ssh_key_path)

    if machine_type == MachineType.BARE_METAL:
        assert gpu_count % 8 == 0, (
            "GPU count must be divisible by 8 for bare metal machines"
        )
    if infiniband:
        assert machine_type == MachineType.BARE_METAL, (
            "Infiniband is only supported for bare metal machines. "
        )

    if location_id is None:
        if len(vp.locations) == 0:
            raise ConnectionError("No locations found")
        elif len(vp.locations) > 1:
            logger.warning("Multiple locations found, please specify a location")
            locations()
            raise typer.Exit()

        location_id = next(iter(vp.locations))

    if name is None:
        # Generate a random friendly name for the cluster
        name = generate_friendly_name()

    response = vp.deploy(
        machine_type=machine_type,
        location=location_id,
        gpu_count=gpu_count,
        name=name,
        network_type="infiniband" if infiniband else "ethernet",
    )

    cluster_id = response["rental_id"]

    with log_progress(
        logger,
        in_progress=f"Deploying cluster [blue][link=https://dashboard.voltagepark.com/instances?machineId={cluster_id}]{cluster_id}[/link][/blue]",
        complete=f"Deployed cluster [blue]{cluster_id}[/blue]",
    ):
        status = vp.clusters[cluster_id].status
        while status == "Pending":
            time.sleep(15)
            status = vp.clusters[cluster_id].status

        if status != "Running":
            logger.error(f"Deployment of cluster {cluster_id} {status}")
            return cluster_id

    if not no_setup:
        setup(cluster_id=cluster_id, password=None, runtime_env=runtime_env)

    return cluster_id


@cli.command()
def setup(
    cluster_id: str,
    ssh_key_path: Optional[str] = None,
    default_playbooks: bool = True,
    extra_playbooks: list[str] = [],
    vault_password: Optional[str] = None,
):
    """Setup an existing VP cluster."""

    vp = VPCloud(ssh_key_path=ssh_key_path)
    cluster_id = vp.status_by_ids([cluster_id])[0]

    ssh_key_path = ssh_key_path or os.environ.get("VP_SSH_KEY_PATH")

    playbooks = []
    if default_playbooks:
        playbooks += [
            os.path.join(os.path.dirname(__file__), "../..", "ansible", "ubuntu.yaml"),
            os.path.join(os.path.dirname(__file__), "../..", "ansible", "python.yaml"),
        ]
    playbooks += extra_playbooks

    for playbook in playbooks:
        if not os.path.exists(playbook):
            logger.error(f"Playbook {playbook} does not exist")
            raise typer.Exit(1)

    for playbook in playbooks:
        setup_cluster(
            *[
                (public_ip, private_ip)
                for public_ip, private_ip in zip(
                    vp.clusters[cluster_id].public_ips,
                    vp.clusters[cluster_id].private_ips,
                )
            ],
            password=vault_password,
            ssh_key_path=ssh_key_path,
            infiniband=vp.clusters[cluster_id].network_type == "infiniband",
            playbook_path=playbook,
        )


@cli.command(name="down")
def terminate(
    ids: Annotated[
        Optional[list[str]], Argument(help="The IDs of the VP clusters to close")
    ] = None,
    all: Annotated[bool, Option("--all", help="Terminate all VP clusters")] = False,
    quiet: Annotated[
        bool, Option("--quiet", help="Do not show more information about the cluster")
    ] = False,
    runtime_env: Annotated[
        Optional[str],
        Option(
            "--runtime-env",
            help="The filepath to a yaml file containing the runtime environment variables",
        ),
    ] = "runtime_env.yaml",
):
    """Terminate the given VP clusters."""
    if ids is None and not all:
        raise typer.BadParameter("Please provide a list of cluster IDs or use --all")

    vp = VPCloud(runtime_env=runtime_env)
    ids_ = vp.status_by_ids(ids)
    if not typer.confirm(f"Are you sure you want to terminate {', '.join(ids_)}?"):
        typer.echo("Aborted.")
        raise typer.Exit()

    vp.terminate(ids_)

    if not quiet:
        clusters(runtime_env=runtime_env)


@cli.command(name="list")
def clusters(
    runtime_env: Annotated[
        Optional[str],
        Option(
            "--runtime-env",
            help="The filepath to a yaml file containing the runtime environment variables",
        ),
    ] = "runtime_env.yaml",
):
    """View the status of all currently active VP clusters."""

    vp = VPCloud(runtime_env=runtime_env)

    table = Table(title="VP Bare Metal Clusters")

    table.add_column("Machine Type", style="grey74")
    table.add_column("ID", style="yellow", no_wrap=True)
    table.add_column("Name", style="magenta")
    table.add_column("Created", style="bright_blue")
    table.add_column("Status")
    table.add_column("Node Count")
    table.add_column("Hourly Rate")
    table.add_column("Network Type")
    table.add_column("Public IP")
    table.add_column("Private IP")

    for id_, cluster in vp.clusters.items():
        status = cluster.status
        color = lambda msg, colour: f"[{colour}]{msg}[/{colour}]"
        status = (
            color(status, "green")
            if status == "Running"
            else color(status, "yellow")
            if status == "Pending"
            else color(status, "red")
            if status == "Failed"
            else color(status, "grey")
        )

        table.add_row(
            cluster.machine_type.value,
            id_,
            cluster.name,
            cluster.creation_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            status,
            str(cluster.node_count),
            f"${float(cluster.rate_hourly):.2f}",
            cluster.network_type,
            ", ".join(cluster.public_ips),
            ", ".join(cluster.private_ips),
        )

    console = Console()
    console.print()
    console.print(table)


@cli.command()
def locations():
    vp = VPCloud()

    table = Table(title="Available Machine Configurations", show_lines=True)
    table.add_column("ID", style="yellow")
    table.add_column("Network Type", style="magenta")
    table.add_column("Available GPUs", style="magenta", justify="right")
    table.add_column("GPU Price", style="green", justify="right")
    table.add_column("Specs", style="cyan")

    for id_, stats in vp.locations.items():
        specs = stats["specs_per_node"]
        for network_type in ("ethernet", "infiniband"):
            table.add_row(
                id_,
                network_type,
                str(stats[f"gpu_count_{network_type}"]),
                f"${float(stats[f'gpu_price_{network_type}']):.2f}",
                f"GPU Model: {specs['gpu_model']} ({specs['gpu_count']})\nCPU Model: {specs['cpu_model']} ({specs['cpu_count']})\nRAM: {specs['ram_gb']}GB\nStorage: {specs['storage_gb']}GB",
            )

    console = Console()
    console.print()
    console.print(table)
    raise typer.Exit()


@cli.command()
def clean(
    cluster_id: Annotated[
        Optional[str], Option("--cluster-id", help="The id of the cluster to clean")
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
    """Clean up stale jobs on all VP nodes."""

    vp = VPCloud(runtime_env=runtime_env, ssh_key_path=ssh_key_path)
    vp.clear_stale_jobs(cluster_id)


@cli.command()
def watch(
    interval: Annotated[
        int,
        Option(
            "--interval",
            help="The time interval in seconds to check the status of the clusters",
        ),
    ] = 600,  # 10 minutes
    kill_idle: Annotated[
        bool,
        Option("--kill-idle", help="Kill idle clusters"),
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
    """Watches all VP clusters and warns if no jobs are running."""

    vp = VPCloud(runtime_env=runtime_env, ssh_key_path=ssh_key_path)

    with log_progress(in_progress="Watching clusters", complete=""):
        while True:
            for cluster_id, jobs in vp.jobs.items():
                if any(job.running for job in jobs):
                    continue

                timestamp = (
                    vp.clusters[cluster_id].creation_timestamp
                    if len(jobs) == 0
                    else jobs[-1].creation_timestamp
                )

                max_delta = timedelta(minutes=30)
                if (
                    kill_idle
                    and datetime.now(timezone(timedelta(hours=-5), "EST")) - timestamp
                    > max_delta
                ):
                    warning = f"Cluster {vp.clusters[cluster_id].name} has been idle for {max_delta.seconds // 60} minutes. Killing cluster..."
                    logger.warning("\n" + warning)
                    slack_message(warning, runtime_env=runtime_env)
                    vp.terminate([cluster_id])
                    continue

                warning = f"No jobs running on cluster `{vp.clusters[cluster_id].name}`. Run `python -m dream submit vp terminate {cluster_id}` to kill the cluster."
                logger.warning("\n" + warning)
                slack_message(warning, runtime_env=runtime_env)

            time.sleep(interval)
