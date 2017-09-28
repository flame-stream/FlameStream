package com.spbsu.flamestream.benchmark;

import com.spbsu.flamestream.benchmark.config.EnvironmentFromTypesafeProvider;
import com.spbsu.flamestream.benchmark.config.EnvironmentRunnerCfg;
import com.spbsu.flamestream.benchmark.config.TypesafeEnvironmentRunnerCfg;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class BenchmarkLauncher {
  public static void main(String[] args) throws Exception {
    final Config load;
    if (args.length == 1) {
      final Path filename = Paths.get(args[0]);
      load = ConfigFactory.parseReader(Files.newBufferedReader(filename));
    } else {
      load = ConfigFactory.load("bench.conf").getConfig("benchmark");
    }

    final EnvironmentRunnerCfg environmentRunnerCfg = new TypesafeEnvironmentRunnerCfg(load);
    final EnvironmentRunner runner = environmentRunnerCfg.runner().newInstance();

    try (final Environment environment = new EnvironmentFromTypesafeProvider(load).environment()) {
      runner.run(environment);
    }
  }
}
