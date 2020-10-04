plan my::bench() {
  get_targets('managers').apply_prep
  $manager_host = get_target('managers').name
  $worker_hosts = get_targets('workers').map |Target $worker| { $worker.name }
  apply(get_targets('managers')) {
    vcsrepo { "${facts['home']}/FlameStream":
      ensure   => present,
      provider => git,
      source   => 'https://github.com/flame-stream/FlameStream',
      revision => 'feature/labels-wip',
    }
    file { "${facts['home']}/FlameStream/benchmark/ansible/remote.yml":
      content => inline_template(@(ERB))
<%= { "all" => { "children" => {
  "bench" => { "hosts" => { @manager_host => {} } },
  "manager" => { "hosts" => { @manager_host => {} } },
  "workers" => { "hosts" => @worker_hosts.map { |host| [host, {}] }.to_h },
} } }.to_yaml %>
ERB
    }
  }
}
