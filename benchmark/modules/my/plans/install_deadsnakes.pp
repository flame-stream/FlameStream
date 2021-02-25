plan my::install_deadsnakes() {
  $manager = get_targets('managers')
  $manager.apply_prep
  apply($manager) {
    package { 'lsb-core': }
    include apt
    apt::ppa { 'ppa:deadsnakes/ppa': }
  }
}
