plan my::localhost_ssh_known_hosts() {
  $all = get_targets("all")
  $all.apply_prep
  'localhost'.apply_prep
  apply('localhost') {
    $all.each |Target $target| {
      $pub = $target.facts['/etc/ssh/ssh_host_ecdsa_key_pub'].split(' ')
      sshkey { $target.uri:
        type => $pub[0],
        key => $pub[1],
        host_aliases => if $target.vars['public_ip'] { $target.vars['public_ip'] } else { $target.vars['private_ip'] },
        target => "${facts['home']}/.ssh/known_hosts",
      }
    }
  }
}
