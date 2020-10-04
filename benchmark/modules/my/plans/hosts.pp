plan my::hosts() {
  $all = get_targets('all')
  $all.apply_prep
  apply($all) {
    $all.each |Target $target| { host { $target.name: ip => $target.vars['private_ip'] } }
  }
}
