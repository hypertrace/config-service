package ai.traceable.edge.config.service;

import static ai.traceable.edge.config.service.TraceableEdgeConfigTest.getTestConfig;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class TraceableEdgeConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(ManagedChannel.class);
    Config config = getTestConfig();
    GrpcChannelRegistry mockGrpcChannelRegistry = mock(GrpcChannelRegistry.class);
    doReturn(mockChannel).when(mockGrpcChannelRegistry).forPlaintextAddress(anyString(), anyInt());
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new TraceableEdgeConfigServiceModule(
                        mockChannel,
                        config,
                        mockGrpcChannelRegistry,
                        mock(FeatureCachingClient.class)))
                .getAllBindings());
  }
}
