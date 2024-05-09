package ai.traceable.fraud.datamodel.derivation.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.google.inject.Stage;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class FraudDataModelDerivationConfigServiceModuleTest {
  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    ConfigChangeEventGenerator mockEventGenerator = mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new FraudDataModelDerivationConfigServiceModule(
                        mockChannel, mockEventGenerator))
                .getAllBindings());
  }
}
