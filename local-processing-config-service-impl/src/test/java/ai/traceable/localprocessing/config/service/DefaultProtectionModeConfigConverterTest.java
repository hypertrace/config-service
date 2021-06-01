package ai.traceable.localprocessing.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import org.junit.jupiter.api.Test;

public class DefaultProtectionModeConfigConverterTest {

  @Test
  void conversionToAndFromValue() {
    ProtectionMode defaultProtectionMode = ProtectionMode.PROTECTION_MODE_ADVANCED;
    assertEquals(
        defaultProtectionMode,
        DefaultProtectionModeConfigConverter.fromValue(
                DefaultProtectionModeConfigConverter.toValue(defaultProtectionMode))
            .get());
  }
}
