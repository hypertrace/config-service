package ai.traceable.edge.decision.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import java.io.File;
import java.util.Objects;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class EdgeDecisionConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Config config =
        ConfigFactory.parseFile(
                new File(
                    Objects.requireNonNull(
                            Thread.currentThread()
                                .getContextClassLoader()
                                .getResource("test_application.conf"))
                        .getPath()))
            .resolve();
    Channel mockChannel = mock(ManagedChannel.class);
    ConfigChangeEventGenerator mockEventGenerator = mock(ConfigChangeEventGenerator.class);
    GrpcChannelRegistry mockGrpcChannelRegistry = mock(GrpcChannelRegistry.class);
    doReturn(mockChannel).when(mockGrpcChannelRegistry).forPlaintextAddress(anyString(), anyInt());
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new EdgeDecisionConfigServiceModule(config, mockChannel, mockEventGenerator))
                .getAllBindings());
  }
}
