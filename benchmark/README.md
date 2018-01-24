# Benchmarks

#### Measurements

1. Inverted index of 1, 2, 4, 8, 10 workers
2. Single filter
3. Single grouping test (suffling) `// there shouldn't be any replay with window 1`

#### Benchmarks structure

Contents of `ansible` directory:

- `aws.yml` - inventory for aws hosts
- `docker-compose.yml` - the setup for local testing
- `local.yml` - inventory for local docker hosts
- `flamestrem.yml` - playbook for flamestream benchmarkink (does all the job)
- `flink.yml` - same for flink
- `roles/` - roles, they seems to be self-explanatory

To run remote benchmarks you need to:

0. Copy wiki.xml to `flamestream-bench` job
1. Package the project
2. Copy artifacts to appropriate directories
3. Fill aws inventory with hosts. Hostname is a local address of the host, `ansible_hostname` is the public
3. Run playbook with inventory of your choise

To run locally:

0. Copy wiki.xml
1. Package
2. Copy
3. `docker-compose up`
4. In `flamestream-common` job change _synchonize_ with _copy_, as sync doesn't work with docker connection
5. Run playbook

All this steps are automated with script in the root of the project. On the last line there is a default funciton call, replace it with local/remote, flink/flamestream function.

Caveats:

- If something goes wrong you need to manually kill bench. I ssh into a host and `sudo pkill -f spbsu` (To run on every host: `sudo ansible -v -i aws.yml all -a 'sudo pkill -f spb'`
- You need to manually check resources on each host. So I suggest to ssh into _bench_ and _input_ hosts and random worker. Run `vmstat -Sm 1`. To monitor memory and cpu usage.
- When you are copying hosts to inventory from aws console check twice
