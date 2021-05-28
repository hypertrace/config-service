package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class SensitiveDataModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = mock(Config.class);
    GrpcChannelRegistry mockRegistry = mock(GrpcChannelRegistry.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(new SensitiveDataModule(mockChannel, mockConfig, mockRegistry))
                .getAllBindings());
  }
}
