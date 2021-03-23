package ai.traceable.blocking.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.blocking.config.service.v1.BlockingInfo;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockingInfoConverterTest {
  private BlockingInfoConverter blockingInfoConverter;

  @BeforeEach
  void setup() {
    this.blockingInfoConverter = new BlockingInfoConverter();
  }

  @Test
  void convertBlockedRegionRule_noExpiration() {
    RegionRule regionRule =
        RegionRule.newBuilder()
            .setId("rule-id-1")
            .setName("name-1")
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            .build();

    assertEquals(
        Optional.of(
            BlockingInfo.newBuilder()
                .setInfo("CglydWxlLWlkLTESBm5hbWUtMQ==")
                .setCategory("CUSTOM_REGION_RULE")
                .setExpirationMillis(0)
                .build()),
        this.blockingInfoConverter.convert(regionRule));
  }

  @Test
  void convertBlockedRegionRule_expiration() {
    RegionRule regionRule =
        RegionRule.newBuilder()
            .setId("rule-id-1")
            .setName("name-1")
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            .setExpirationMillis(100)
            .build();

    assertEquals(
        Optional.of(
            BlockingInfo.newBuilder()
                .setInfo("CglydWxlLWlkLTESBm5hbWUtMQ==")
                .setCategory("CUSTOM_REGION_RULE")
                .setExpirationMillis(100)
                .build()),
        this.blockingInfoConverter.convert(regionRule));
  }

  @Test
  void convertRegionRule_unknownRuleType() {
    RegionRule regionRule =
        RegionRule.newBuilder()
            .setId("rule-id-1")
            .setName("name-1")
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_UNSPECIFIED)
            .build();

    assertTrue(this.blockingInfoConverter.convert(regionRule).isEmpty());
  }
}
