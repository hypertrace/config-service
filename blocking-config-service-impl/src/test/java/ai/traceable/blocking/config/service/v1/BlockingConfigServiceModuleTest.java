package ai.traceable.blocking.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceModuleTest {

  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = mock(Config.class);
    GrpcChannelRegistry mockGrpcChannelRegistry = mock(GrpcChannelRegistry.class);

    when(mockConfig.getConfig("blocking.config.service"))
        .thenReturn(
            ConfigFactory.parseString(
                "agent.polling.frequency = 30s\n"
                    + "  iptype {\n"
                    + "    mode = RESOURCE_FILE\n"
                    + "    resource.file = iptype/empty_highrisk.csv\n"
                    + "  }\n"
                    + "  actor.fetcher.config = {\n"
                    + "    host = localhost\n"
                    + "    port = 50888\n"
                    + "    request.timeout = 10s\n"
                    + "    cache = {\n"
                    + "      maxCacheSize = 10000\n"
                    + "      refreshAfterWriteDuration = 30s\n"
                    + "      expireAfterWriteDuration = 10m\n"
                    + "    }\n"
                    + "    maxNumberOfActors = 10000\n"
                    + "  }"));

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new BlockingConfigServiceModule(
                        mockChannel, mockConfig, mockGrpcChannelRegistry))
                .getAllBindings());
  }
}
