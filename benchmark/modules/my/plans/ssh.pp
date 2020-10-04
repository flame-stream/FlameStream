plan my::ssh() {
  $all = get_targets('all')
  apply($all) {
    exec { "/usr/bin/test -e /home/ubuntu/.ssh/id_rsa.pub || ssh-keygen -N '' -f /home/ubuntu/.ssh/id_rsa": }
  }
  $all.apply_prep
  apply($all) {
    $all.each |Target $target| {
      $client_public_key = split($target.facts['~/.ssh/id_rsa.pub'], ' ')
      ssh_authorized_key { $client_public_key[2]:
        user   => 'ubuntu',
        type   => $client_public_key[0],
        key    => $client_public_key[1],
      }
      $server_public_key = $target.facts['/etc/ssh/ssh_host_ecdsa_key.pub'].split(' ')
      sshkey { $target.name:
        type => $server_public_key[0],
        key => $server_public_key[1],
        host_aliases => $target.vars['private_ip'],
        target => "${facts['home']}/.ssh/known_hosts",
      }
    }
  }
}
