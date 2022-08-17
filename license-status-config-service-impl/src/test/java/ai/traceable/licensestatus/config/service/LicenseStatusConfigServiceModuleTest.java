package ai.traceable.licensestatus.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

public class LicenseStatusConfigServiceModuleTest {

  @Test
  public void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config config = ConfigFactory.empty();

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new LicenseStatusConfigServiceModule(
                        new GrpcChannelRegistry(), mockChannel, config))
                .getAllBindings());
  }
}
