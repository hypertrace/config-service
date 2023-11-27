package ai.traceable.runner.logs.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RunnerLogsConfigServiceModuleTest {

  @Mock Config mockConfig;
  @Mock Channel mockChannel;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;

  @Test
  void testResolveBindings() {
    when(mockConfig.getConfig("runner.logs.config")).thenReturn(buildConfig());
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new RunnerLogsConfigServiceModule(
                        mockConfig, mockChannel, mockConfigChangeEventGenerator))
                .getAllBindings());
  }

  private Config buildConfig() {
    return ConfigFactory.parseString(
        "defaultRunnerLogsPurgeConfig = {\n"
            + "    \"astScanLogsPurgeConfig\" : {\n"
            + "      \"enabled\": true\n"
            + "      \"retentionDuration\": \"259200s\"\n"
            + "    }\n"
            + "  }\n"
            + "  defaultRunnerLogsPersistenceConfig = {\n"
            + "    \"astScanLogsPersistenceConfig\" : {\n"
            + "      \"maxPersistLogLines\": 100000\n"
            + "    }\n"
            + "  }");
  }
}
