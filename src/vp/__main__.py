from typer import Typer

from vp.cli.cluster import cli as cluster_cli
from vp.cli.job import cli as job_cli

cli = Typer(pretty_exceptions_show_locals=False, add_completion=False)
cli.add_typer(cluster_cli, name="cluster")
cli.add_typer(job_cli, name="job")

if __name__ == "__main__":
    cli()
