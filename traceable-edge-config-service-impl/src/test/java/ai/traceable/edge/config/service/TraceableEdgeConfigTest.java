package ai.traceable.edge.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
            "rate.limiting.service.config",
            ImmutableMap.copyOf(
                ConfigFactory.parseString(
                        "host = localhost\n" + "  port = 50888\n" + "  request.timeout = 10s\n")
                    .entrySet())));
  }
}
