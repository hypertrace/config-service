package ai.traceable.edge.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import com.google.protobuf.Duration;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractTraceableEdgeConfigSupplierTest {

  private UuidGenerator uuidGenerator;
  private TraceableEdgeConfig config;

  @BeforeEach
  void setup() {
    uuidGenerator = mock(UuidGenerator.class);
    config = mock(TraceableEdgeConfig.class);
    when(config.getAgentPollingFrequency(any()))
        .thenReturn(Duration.newBuilder().setSeconds(30).build());
  }

  @Test
  void buildConfigResponseElement_hashIncludesConfigType() {
    when(uuidGenerator.generateId(any(ConfigPayloads.class))).thenReturn("payload-hash");

    TestSupplier supplier = new TestSupplier(uuidGenerator, config, "TestConfigType");
    ConfigPayloads payload = ConfigPayloads.getDefaultInstance();
    AgentCapabilities caps = AgentCapabilities.getDefaultInstance();

    ConfigResponseElement response = supplier.buildConfigResponseElement(payload, caps);

    assertTrue(response.getHash().contains("TestConfigType"));
    assertEquals("payload-hash:TestConfigType", response.getHash());
  }

  /** Test implementation of AbstractTraceableEdgeConfigSupplier */
  private static class TestSupplier extends AbstractTraceableEdgeConfigSupplier {
    private final String configType;

    TestSupplier(UuidGenerator uuidGenerator, TraceableEdgeConfig config, String configType) {
      super(uuidGenerator, config);
      this.configType = configType;
    }

    @Override
    public String getConfigType() {
      return configType;
    }

    @Override
    public ConfigResponseElement getConfigs(
        RequestContext ctx, String env, ConfigRequestElement req, AgentCapabilities caps) {
      return buildConfigResponseElement(ConfigPayloads.getDefaultInstance(), caps);
    }

    // Expose protected method for testing
    @Override
    public ConfigResponseElement buildConfigResponseElement(
        ConfigPayloads configPayloads, AgentCapabilities agentCapabilities) {
      return super.buildConfigResponseElement(configPayloads, agentCapabilities);
    }
  }
}
