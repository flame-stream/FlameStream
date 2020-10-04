Facter.add('ssh_id_rsa_pub') do
  setcode do
    Facter::Util::Resolution.exec('cat ~/.ssh/id_rsa.pub')
  end
end
