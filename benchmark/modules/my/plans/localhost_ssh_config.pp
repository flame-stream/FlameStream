plan my::localhost_ssh_config() {
  $all = get_targets("all")
  $manager = get_target("managers")
  'localhost'.apply_prep
  apply('localhost') {
    file { "${facts['home']}/.ssh/config":
      ensure => present,
    }
    fm_prepend { "Include flamestream-benchmarks.config":
      ensure => present,
      data   => "Include flamestream-benchmarks.config",
      path   => "${facts['home']}/.ssh/config",
    }
    ::ssh::client::config::user { system::env('USER'):
      target => "${facts['home']}/.ssh/flamestream-benchmarks.config",
      ensure => present,
      options => Hash($all.map |Target $target| {
        [
          "Host ${target.uri}", if ($target.vars['public_ip']) {
            {
              'HostName' => $target.vars['public_ip'],
              'User' => 'ubuntu',
              'StrictHostKeyChecking' => no,
            }
          } else {
            {
              'HostName' => $target.vars['private_ip'],
              'User' => 'ubuntu',
              'ProxyJump' => $manager.uri,
              'StrictHostKeyChecking' => no,
            }
          },
        ]
      }),
    }
  }
}
