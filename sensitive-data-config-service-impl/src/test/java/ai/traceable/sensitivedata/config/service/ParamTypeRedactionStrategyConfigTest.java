package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import org.junit.jupiter.api.Test;

class ParamTypeRedactionStrategyConfigTest {

  @Test
  void conversionToAndFromValue() {
    ParamTypeRedactionStrategyConfig paramTypeRedactionStrategyConfig =
        new ParamTypeRedactionStrategyConfig(RedactionStrategy.REDACTION_STRATEGY_HASH);
    assertEquals(
        paramTypeRedactionStrategyConfig,
        ParamTypeRedactionStrategyConfig.fromValue(paramTypeRedactionStrategyConfig.toValue())
            .get());
  }
}
