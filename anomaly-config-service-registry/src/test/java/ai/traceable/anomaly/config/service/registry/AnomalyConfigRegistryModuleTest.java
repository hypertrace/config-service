package ai.traceable.anomaly.config.service.registry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.inject.Guice;
import org.junit.jupiter.api.Test;

class AnomalyConfigRegistryModuleTest {
  @Test
  public void testResolveBindings() {
    assertDoesNotThrow(
        () -> Guice.createInjector(new AnomalyConfigRegistryModule()).getAllBindings());
  }
}
