plan my::install() {
  $manager = get_targets('managers')
  $manager.apply_prep
  apply($manager) {
    package { 'git': }
    package { 'libffi-dev': }
    package { 'libzmq3-dev': }
    package { 'maven': }
    package { 'python3': }
    package { 'python3.9': }
    package { 'python3.9-dev': }
    package { 'python3.9-distutils': }
    class { 'python::pip::bootstrap': version => 'pip3' }
    python::pip { 'ansible': }
  }
  $workers = get_targets('workers')
  $workers.apply_prep
  apply($workers) {
    include apt
    package { 'openjdk-11-jdk': }
    package { 'rsync': }
    package { 'vim': }
    package { 'less': }
    package { 'htop': }
    package { 'procps': }
    package { 'unzip': }
    package { 'sysstat': }
    package { 'make': }
    package { 'gcc': }
    alternatives { java: path => '/usr/lib/jvm/java-11-openjdk-amd64/bin/java' }
  }
}
