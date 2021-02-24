plan my::destroy() {
  $all = get_targets("all")
  $manager = get_target("managers")
  $all.apply_prep
  'localhost'.apply_prep
  apply('localhost') {
    $all.each |Target $target| {
      $pub = $target.facts['/etc/ssh/ssh_host_ecdsa_key_pub'].split(' ')
      sshkey { $target.uri:
        ensure => absent,
        type => $pub[0],
        key => $pub[1],
        host_aliases => if $target.vars['public_ip'] { $target.vars['public_ip'] } else { $target.vars['private_ip'] },
        target => "${facts['home']}/.ssh/known_hosts",
      }
    }
  }

  apply('localhost') {
    file_line { "~/.ssh/config:Include flamestream-benchmarks.config":
      path   => "${facts['home']}/.ssh/config",
      line   => "Include flamestream-benchmarks.config",
      ensure => absent,
    }
    file { "${facts['home']}/.ssh/flamestream-benchmarks.config":
      ensure => absent,
    }
  }
}
