package ai.traceable.saved.query.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class SavedQueryConfigServiceModuleTest {

  @Test
  void testResolveBindings() {
    Config config = ConfigFactory.parseResources("application.conf");
    Channel mockChannel = mock(Channel.class);
    GrpcChannelRegistry mockGrpcChannelRegistry = mock(GrpcChannelRegistry.class);
    ConfigChangeEventGenerator mockEventGenerator = mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new SavedQueryConfigServiceModule(
                        config, mockChannel, mockEventGenerator, mockGrpcChannelRegistry))
                .getAllBindings());
  }
}
