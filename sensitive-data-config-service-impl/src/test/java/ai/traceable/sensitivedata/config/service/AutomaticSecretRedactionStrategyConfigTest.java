package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class AutomaticSecretRedactionStrategyConfigTest {

  @Test
  void conversionToAndFromValue() {
    AutomaticSecretRedactionStrategyConfig automaticSecretRedactionStrategyConfig =
        new AutomaticSecretRedactionStrategyConfig(true);
    assertEquals(
        automaticSecretRedactionStrategyConfig,
        AutomaticSecretRedactionStrategyConfig.fromValue(
                automaticSecretRedactionStrategyConfig.toValue())
            .get());
  }
}
