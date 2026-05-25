package ai.traceable.edge.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TraceableEdgeConfigTest {
  @Test
  public void testConfig() {
    TraceableEdgeConfig config = new TraceableEdgeConfig(getTestConfig());
    assertEquals(30, config.getAgentPollingFrequency("CaptchaSiteKeyConfig").getSeconds());
    assertEquals(15, config.getAgentPollingFrequency("Random").getSeconds());
    assertEquals(
        CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3,
        config.getCustomSignatureRuleVersion());
  }

  static Config getTestConfig() {
    return ConfigFactory.parseMap(
        Map.of(
            "traceable.edge.config.service",
            ImmutableMap.copyOf(ConfigFactory.parseResources("traceable-edge.conf").entrySet()),
            "actor.service.config",
            ImmutableMap.copyOf(
                ConfigFactory.parseString(
                        "host = localhost\n"
                            + "  port = 50888\n"
                            + "  request.timeout = 10s\n"
                            + "  cache = {\n"
                            + "    maxCacheSize = 10000\n"
                            + "    refreshAfterWriteDuration = 30s\n"
                            + "    expireAfterWriteDuration = 10m\n"
                            + "  }\n"
                            + "  maxNumberOfActors = 10000")
                    .entrySet()),
            "entity.fetcher.cache",
            ImmutableMap.copyOf(
                ConfigFactory.parseString(
                        "  service.mapping.cache = {\n"
                            + "    maxSize = 1000\n"
                            + "    refreshAfterWriteDuration = 10m\n"
                            + "    expireAfterWriteDuration = 1h\n"
                            + "  }\n"
                            + "  api.mapping.cache = {\n"
                            + "    maxSize = 1000\n"
                            + "    refreshAfterWriteDuration = 10m\n"
                            + "    expireAfterAccessDuration = 1h\n"
                            + "  }\n")
                    .entrySet()),
            "entity.service",
            ImmutableMap.copyOf(
                ConfigFactory.parseString(
                        "config {\n"
                            + "    host = localhost\n"
                            + "    port = 50061\n"
                            + "    timeout = 10s\n"
                            + "  }\n"
                            + "  attributeMap {\n"
                            + "    service.id = \"SERVICE.id\"\n"
                            + "    service.name = \"SERVICE.name\"\n"
                            + "    service.environment = \"SERVICE.environment\"\n"
                            + "    environment.id = \"ENVIRONMENT.id\"\n"
                            + "    environment.name = \"ENVIRONMENT.name\"\n"
                            + "  }")
                    .entrySet()),
            "rate.limiting.service.config",
            ImmutableMap.copyOf(
                ConfigFactory.parseString(
                        "host = localhost\n" + "  port = 50888\n" + "  request.timeout = 10s\n")
                    .entrySet()),
            "insights.service.config",
            ImmutableMap.copyOf(
                ConfigFactory.parseString("host = localhost\n" + "  port = 50061\n").entrySet())));
  }
}
