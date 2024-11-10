package ai.traceable.edge.config.service;

import static ai.traceable.edge.config.service.TraceableEdgeConfigServiceModule.TRACEABLE_EDGE_CONFIG_SERVICE_NAME;
import static ai.traceable.edge.config.service.TraceableEdgeConfigSupplier.DEFAULT_AGENT_POLLING_FREQUENCY_CONFIG_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.util.Collections;
import java.util.Map;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class TraceableEdgeConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config config =
        ConfigFactory.parseMap(
            Map.of(
                TRACEABLE_EDGE_CONFIG_SERVICE_NAME,
                Collections.singletonMap(DEFAULT_AGENT_POLLING_FREQUENCY_CONFIG_NAME, "30s")));

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new TraceableEdgeConfigServiceModule(
                        mockChannel, config, mock(GrpcChannelRegistry.class)))
                .getAllBindings());
  }
}
