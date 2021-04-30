package ai.traceable.region.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionRuleConverterTest {
  private RegionRuleConverter regionRuleConverter;

  @BeforeEach
  void setup() {
    this.regionRuleConverter = new RegionRuleConverter();
  }

  @Test
  void should_convert_regionRuleConfig_toRegionRule() throws InvalidProtocolBufferException {
    Struct ruleConfigStruct =
        Struct.newBuilder()
            .putFields("id", Value.newBuilder().setStringValue("id-1").build())
            .putFields(
                "region_id",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("region-1").build())
                            .addValues(Value.newBuilder().setStringValue("region-2").build())
                            .build())
                    .build())
            .putFields("name", Value.newBuilder().setStringValue("rule-name").build())
            .putFields(
                "action_type",
                Value.newBuilder()
                    .setStringValue(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK.name())
                    .build())
            .putFields("expiration_millis", Value.newBuilder().setStringValue("123").build())
            .build();
    Value ruleConfig = Value.newBuilder().setStructValue(ruleConfigStruct).build();
    RegionRule regionRule = regionRuleConverter.convert(ruleConfig);

    assertEquals(
        RegionRule.newBuilder()
            .setId("id-1")
            .addAllRegionId(List.of("region-1", "region-2"))
            .setName("rule-name")
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            .setExpirationMillis(123)
            .build(),
        regionRule);
  }

  @Test
  void should_fail_regionRuleConfig_toRegionRule_invalidRuleConfig() {
    assertThrows(
        InvalidProtocolBufferException.class,
        () -> regionRuleConverter.convert(Value.getDefaultInstance()));
  }

  @Test
  void should_convert_regionRuleConfig_toRegionRule_nullRuleConfig()
      throws InvalidProtocolBufferException {
    assertEquals(RegionRule.getDefaultInstance(), regionRuleConverter.convert((Value) null));
  }

  @Test
  void should_convert_regionRule_toRegionRuleConfig() throws InvalidProtocolBufferException {
    RegionRule regionRule =
        RegionRule.newBuilder()
            .setId("id-1")
            .addAllRegionId(List.of("region-1", "region-2"))
            .setName("rule-name")
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            .setExpirationMillis(123)
            .build();

    Value regionRuleConfig = regionRuleConverter.convert(regionRule);

    Struct ruleConfigStruct =
        Struct.newBuilder()
            .putFields("id", Value.newBuilder().setStringValue("id-1").build())
            .putFields(
                "regionId",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("region-1").build())
                            .addValues(Value.newBuilder().setStringValue("region-2").build())
                            .build())
                    .build())
            .putFields("name", Value.newBuilder().setStringValue("rule-name").build())
            .putFields(
                "actionType",
                Value.newBuilder()
                    .setStringValue(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK.name())
                    .build())
            .putFields("expirationMillis", Value.newBuilder().setStringValue("123").build())
            .build();
    assertEquals(Value.newBuilder().setStructValue(ruleConfigStruct).build(), regionRuleConfig);
  }
}
