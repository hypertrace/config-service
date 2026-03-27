package ai.traceable.fraud.policy.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.Guice;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class FraudPolicyConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config mockConfig = mock(Config.class);
    ConfigChangeEventGenerator mockEventGenerator = mock(ConfigChangeEventGenerator.class);
    GrpcChannelRegistry mockRegistry = mock(GrpcChannelRegistry.class);
    doReturn(mock(ManagedChannel.class)).when(mockRegistry).forPlaintextAddress("localhost", 50061);
    when(mockConfig.getConfig("entity.service"))
        .thenReturn(
            ConfigFactory.parseString(
                "config {\n"
                    + "  host = localhost\n"
                    + "  port = 50061\n"
                    + "  timeout = 10s\n"
                    + "}\n"
                    + "attributeMap {\n"
                    + "  service.id = \"SERVICE.id\"\n"
                    + "  service.name = \"SERVICE.name\"\n"
                    + "  service.environment = \"SERVICE.environment\"\n"
                    + "  api.id = \"API.id\"\n"
                    + "  api.name = \"API.name\"\n"
                    + "  api.url.pattern = \"API.urlPattern\"\n"
                    + "  api.resolved.url.patterns = \"API.resolvedUrlPatterns\"\n"
                    + "  api.labels = \"API.labels\"\n"
                    + "  api.httpMethod = \"API.httpMethod\"\n"
                    + "  api.serviceName = \"API.serviceName\"\n"
                    + "  api.type = \"API.apiType\"\n"
                    + "  api.isLearnt = \"API.isLearnt\"\n"
                    + "  api.environment = \"API.environment\"\n"
                    + "  api.isGenAi = \"API.isGenAiEndpoint\"\n"
                    + "  api.associatedAiModels = \"API.associatedAiModels\"\n"
                    + "  api.associatedAiVendors = \"API.associatedAiVendors\"\n"
                    + "  api.promptAttributeKeys = \"API.promptAttributeKeys\"\n"
                    + "}"));
    when(mockConfig.getConfig("entity.fetcher.cache"))
        .thenReturn(
            ConfigFactory.parseString(
                "service.mapping.cache {\n"
                    + "  maxSize = 100\n"
                    + "  refreshAfterWriteDuration = 6h\n"
                    + "  expireAfterWriteDuration = 24h\n"
                    + "}\n"
                    + "api.mapping.cache {\n"
                    + "  maxSize = 100\n"
                    + "  refreshAfterWriteDuration = 6h\n"
                    + "  expireAfterAccessDuration = 24h\n"
                    + "}"));
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new FraudPolicyConfigServiceModule(
                        mockChannel, mockConfig, mockEventGenerator, mockRegistry))
                .getAllBindings());
  }
}
