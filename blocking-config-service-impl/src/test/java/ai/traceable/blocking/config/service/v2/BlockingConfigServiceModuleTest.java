package ai.traceable.blocking.config.service.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.util.Map;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceModuleTest {

  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = mock(Config.class);
    GrpcChannelRegistry mockGrpcChannelRegistry = mock(GrpcChannelRegistry.class);

    when(mockConfig.getConfig("blocking.config.service.iptype"))
        .thenReturn(
            ConfigFactory.parseMap(
                Map.of("mode", "RESOURCE_FILE", "resource.file", "placeholder/highrisk.csv")));

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new BlockingConfigServiceModule(
                        mockChannel, mockConfig, mockGrpcChannelRegistry))
                .getAllBindings());
  }
}
