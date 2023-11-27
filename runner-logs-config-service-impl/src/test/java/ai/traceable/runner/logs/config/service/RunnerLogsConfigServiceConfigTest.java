package ai.traceable.runner.logs.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.runner.logs.config.service.v1.AstScanLogsPersistenceConfig;
import ai.traceable.runner.logs.config.service.v1.AstScanLogsPurgeConfig;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPersistenceConfig;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPurgeConfig;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RunnerLogsConfigServiceConfigTest {

  private RunnerLogsConfigServiceConfig runnerLogsConfigServiceConfig;

  @BeforeEach
  void setup() throws InvalidProtocolBufferException {
    runnerLogsConfigServiceConfig = new RunnerLogsConfigServiceConfig(buildConfig());
  }

  @Test
  void testConfig() {
    assertEquals(
        RunnerLogsPurgeConfig.newBuilder()
            .setAstScanLogsPurgeConfig(
                AstScanLogsPurgeConfig.newBuilder()
                    .setEnabled(true)
                    .setRetentionDuration(Duration.newBuilder().setSeconds(259200)))
            .build(),
        runnerLogsConfigServiceConfig.getDefaultRunnerLogsPurgeConfig());
    assertEquals(
        RunnerLogsPersistenceConfig.newBuilder()
            .setAstScanLogsPersistenceConfig(
                AstScanLogsPersistenceConfig.newBuilder().setMaxPersistLogLines(100000))
            .build(),
        runnerLogsConfigServiceConfig.getDefaultRunnerLogsPersistenceConfig());
  }

  private Config buildConfig() {
    return ConfigFactory.parseString(
        "runner.logs.config {\n"
            + "  defaultRunnerLogsPurgeConfig = {\n"
            + "    \"astScanLogsPurgeConfig\" : {\n"
            + "      \"enabled\": true\n"
            + "      \"retentionDuration\": \"259200s\"\n"
            + "    }\n"
            + "  }\n"
            + "  defaultRunnerLogsPersistenceConfig = {\n"
            + "    \"astScanLogsPersistenceConfig\" : {\n"
            + "      \"maxPersistLogLines\": 100000\n"
            + "    }\n"
            + "  }\n"
            + "}");
  }
}
