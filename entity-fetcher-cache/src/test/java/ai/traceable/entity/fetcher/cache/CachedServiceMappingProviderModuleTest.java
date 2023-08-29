package ai.traceable.entity.fetcher.cache;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class CachedServiceMappingProviderModuleTest {
  @Test
  void testResolveBindings() {
    Config mockConfig = mock(Config.class);
    GrpcChannelRegistry mockGrpcChannelRegistry = mock(GrpcChannelRegistry.class);
    doReturn(mock(ManagedChannel.class))
        .when(mockGrpcChannelRegistry)
        .forPlaintextAddress("localhost", 50061);

    when(mockConfig.getConfig("entity.fetcher.cache"))
        .thenReturn(
            ConfigFactory.parseString(
                "service.mapping.cache = {\n"
                    + "    maxSize = 100\n"
                    + "    refreshAfterWriteDuration = 6h\n"
                    + "    expireAfterWriteDuration = 24h\n"
                    + "  }"));
    when(mockConfig.getConfig("entity.service"))
        .thenReturn(
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
                    + "  }"));
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new CachedServiceMappingProviderModule(
                        mockGrpcChannelRegistry, mockConfig, "name"))
                .getAllBindings());
  }
}
