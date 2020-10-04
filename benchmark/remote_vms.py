#!/usr/bin/env python3
import json
import sys
import subprocess
from tempfile import NamedTemporaryFile
import yaml

if len(sys.argv) != 2:
    raise Exception("exactly 1 argument is expected")

terraform_run = subprocess.run(["terraform", "output", "--json"], capture_output=True)
terraform_run.check_returncode()
terraform_output = json.loads(terraform_run.stdout)
manager_public_ip = terraform_output['manager_public_ip']['value'].strip()
manager_private_ip = terraform_output['manager_private_ip']['value'].strip()
worker_private_ips = [ip.strip() for ip in terraform_output['worker_private_ips']['value']]

with NamedTemporaryFile() as inventory_temporary_file:
    with open(inventory_temporary_file.name, 'w') as inventory:
        inventory.write(yaml.dump(dict(groups=[
            dict(name='managers', targets=[dict(
                uri='flamestream-benchmarks-manager',
                name='manager',
                vars=dict(public_ip=manager_public_ip, private_ip=manager_private_ip),
            )]),
            dict(
                name='workers',
                targets=[
                    dict(
                        uri=f"flamestream-benchmarks-worker-{index}",
                        name=f"worker-{index}",
                        vars=dict(private_ip=private_ip),
                    )
                    for index, private_ip in enumerate(worker_private_ips)
                ],
                # config=dict(transport='ssh', ssh={'run-as-command': [
                #     'sudo',
                #     f'http_proxy=http://{manager_private_ip}:3128',
                #     f'https_proxy=http://{manager_private_ip}:3128',
                #     "-nkSEu",
                # ]}),
            ),
        ])))


    def bolt_plan_run(*args):
        subprocess.run((
            "bolt", "plan", "run", f'--inventoryfile={inventory_temporary_file.name}', "--concurrency=10", *args
        )).check_returncode()


    if sys.argv[1] == "apply":
        bolt_plan_run("my::localhost_ssh_config")
        bolt_plan_run("my::install_agent", "--run-as=root")
        bolt_plan_run("my::localhost_ssh_known_hosts")
        # bolt_plan_run("my::http_proxy", "--run-as=root")
        bolt_plan_run("my::hosts", "--run-as=root")
        bolt_plan_run("my::install_deadsnakes", "--run-as=root")
        bolt_plan_run("my::install", "--run-as=root")
        bolt_plan_run("my::ssh")
        bolt_plan_run("my::bench")
    elif sys.argv[1] == "destroy":
        bolt_plan_run("my::destroy")
    else:
        raise Exception("unknown argument")
