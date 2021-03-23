package ai.traceable.blocking.config.service.regions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ai.traceable.blocking.config.service.v1.RuleActionType;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionTypeConverterTest {
  private ActionTypeConverter actionTypeConverter;

  @BeforeEach
  void setup() {
    this.actionTypeConverter = new ActionTypeConverter();
  }

  @Test
  void shouldConvertActionType() {
    assertEquals(
        Optional.of(RuleActionType.RULE_ACTION_TYPE_BLOCK),
        this.actionTypeConverter.convert(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK));
    assertTrue(
        this.actionTypeConverter
            .convert(RegionRuleActionType.REGION_RULE_ACTION_TYPE_UNSPECIFIED)
            .isEmpty());
  }
}
