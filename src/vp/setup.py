"""
Utility file that sets up a node through SSH (and a predefined ansible playbook).
"""

import os
import shutil

import ansible_runner
from loguru import logger
from tenacity import retry, retry_if_result, stop_after_attempt, wait_fixed

PRIVATE_ANSIBLE_DIR = os.path.join("/tmp/ansible", os.getenv("USER", "default"))


def event_handler(event: dict):
    match event.get("event"):
        case "playbook_on_play_start":
            logger.info(f"PLAY: {event['event_data']['play']}")
        case "playbook_on_task_start":
            logger.info(f"TASK: {event['event_data']['task']}")
        case "playbook_on_stats":
            msg = event["stdout"].split("*")[-1].strip()
            msg = " ".join(word for word in msg.split(" ") if word)
            logger.info(f"RECAP: {msg}")
        case (
            "verbose"
            | "playbook_on_start"
            | "runner_on_start"
            | "runner_on_ok"
            | "runner_on_skipped"
        ):
            pass  # Skip logging these events
        case _:
            error_msg = event.get("event_data", {}).get("res", {}).get("msg", None)
            logger.warning(event) if error_msg is None else logger.error(f"{error_msg}")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_result(lambda x: x != "successful"),
)
def _setup_node(
    *ips: tuple[str, str],
    password: str,
    ssh_key: str | None = None,
    playbook_path: str = os.path.join("clusters", "setup.yaml"),
    infiniband: bool = False,
) -> str:
    # Extract group IP from private IPs (assuming they're in the same subnet)
    group_ips = []
    for _, private_ip in ips:
        ip_parts = private_ip.split(".")
        if len(ip_parts) == 4:  # Ensure it's a valid IPv4
            # Create network address by taking the first two octets
            group_ip = f"{ip_parts[0]}.{ip_parts[1]}.0.0"
            group_ips.append(group_ip)

    inventory = "\n".join(
        (
            "[nodes]",
            *(
                f"{public_ip} ansible_user=ubuntu private_ip={private_ip} group_ip={group_ip}"
                for ((public_ip, private_ip), group_ip) in zip(ips, group_ips)
            ),
        )
    )

    os.makedirs(PRIVATE_ANSIBLE_DIR, exist_ok=True)
    playbook_path = os.path.join(os.getcwd(), playbook_path)

    run = ansible_runner.run(
        private_data_dir=PRIVATE_ANSIBLE_DIR,
        playbook=playbook_path,
        inventory=inventory,
        ssh_key=ssh_key,
        passwords={"^Vault password:\\s*?$": password},
        cmdline="--ask-vault-pass",
        event_handler=event_handler,
        quiet=False,
        extravars={"enable_infiniband": infiniband},
    )

    return run.status


def setup_cluster(
    *ips: tuple[str, str],
    password: str,
    ssh_key_path: str | None = None,
    playbook_path: str = os.path.join("clusters", "setup.yaml"),
    infiniband: bool = False,
) -> bool:
    if ssh_key_path:
        with open(ssh_key_path, "r") as f:
            ssh_key = f.read()
    else:
        ssh_key = None

    try:
        return _setup_node(
            *ips,
            password=password,
            ssh_key=ssh_key,
            playbook_path=playbook_path,
            infiniband=infiniband,
        )
    finally:
        if os.path.exists(PRIVATE_ANSIBLE_DIR):
            shutil.rmtree(PRIVATE_ANSIBLE_DIR)
