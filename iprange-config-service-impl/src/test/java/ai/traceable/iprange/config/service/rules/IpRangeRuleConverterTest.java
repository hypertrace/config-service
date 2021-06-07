package ai.traceable.iprange.config.service.rules;

import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK;
import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IpRangeRuleConverterTest {
  private IpRangeRuleConverter ipRangeRuleConverter;

  @BeforeEach
  void setUp() {
    this.ipRangeRuleConverter = new IpRangeRuleConverter();
  }

  @Test
  void should_convert_ipRangeRuleConfig_toipRangeRule() throws InvalidProtocolBufferException {
    Struct ruleDetailsStructure =
        Struct.newBuilder()
            .putFields("name", Value.newBuilder().setStringValue("Tester-1").build())
            .putFields(
                "description", Value.newBuilder().setStringValue("Range rule test 1").build())
            .putFields(
                "rawInputIpData",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("1.1.1.1/16").build())
                            .addValues(Value.newBuilder().setStringValue("1.2.3.4").build())
                            .addValues(Value.newBuilder().setStringValue("12.42.33.41").build())
                            .build())
                    .build())
            .putFields(
                "ruleAction", Value.newBuilder().setStringValue(RULE_ACTION_BLOCK.name()).build())
            .build();

    IpRangeRuleDetails ipRangeRuleDetails =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester-1")
            .setDescription("Range rule test 1")
            .addAllRawInputIpData(List.of("1.1.1.1/16", "1.2.3.4", "12.42.33.41"))
            .setRuleAction(RULE_ACTION_BLOCK)
            .build();

    Struct ruleConfigStructure =
        Struct.newBuilder()
            .putFields("id", Value.newBuilder().setStringValue("test-id").build())
            .putFields(
                "ruleDetails", Value.newBuilder().setStructValue(ruleDetailsStructure).build())
            .putFields("internal", Value.newBuilder().setBoolValue(true).build())
            .putFields(
                "ipRanges",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("1.1.1.1/16").build())
                            .build())
                    .build())
            .putFields(
                "ipAddresses",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("1.2.3.4").build())
                            .addValues(Value.newBuilder().setStringValue("12.42.33.41").build())
                            .build())
                    .build())
            .build();

    Value ruleConfig = Value.newBuilder().setStructValue(ruleConfigStructure).build();
    IpRangeRule ipRangeRule = ipRangeRuleConverter.convert(ruleConfig);

    assertEquals(
        IpRangeRule.newBuilder()
            .setId("test-id")
            .setRuleDetails(ipRangeRuleDetails)
            .setInternal(true)
            .addAllIpRanges(List.of("1.1.1.1/16"))
            .addAllIpAddresses(List.of("1.2.3.4", "12.42.33.41"))
            .build(),
        ipRangeRule);

    Value reconvertedValue = ipRangeRuleConverter.convert(ipRangeRule);
    assertEquals(reconvertedValue, ruleConfig);
  }

  @Test
  void should_fail_ipRangeRuleConfig_toIpRangeRule_invalidRuleConfig() {
    assertThrows(
        InvalidProtocolBufferException.class,
        () -> ipRangeRuleConverter.convert(Value.getDefaultInstance()));
  }

  @Test
  void should_convert_ipRangeRuleConfig_toIpRangeRule_nullRuleConfig()
      throws InvalidProtocolBufferException {
    assertEquals(IpRangeRule.getDefaultInstance(), ipRangeRuleConverter.convert((Value) null));
  }

  @Test
  void should_convert_ipRangeRule_toIpRangeRuleConfig() throws InvalidProtocolBufferException {
    // Note the fields with default value are dropped
    IpRangeRuleDetails ipRangeRuleDetails =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester-1")
            .setDescription("Range rule test 1")
            .addAllRawInputIpData(List.of("1.1.1.1/16", "1.2.3.4", "12.42.33.41"))
            .setRuleAction(RULE_ACTION_BLOCK)
            .build();

    IpRangeRule ipRangeRule =
        IpRangeRule.newBuilder()
            .setId("test-id")
            .setRuleDetails(ipRangeRuleDetails)
            .setInternal(true)
            .addAllIpRanges(List.of("1.1.1.1/16"))
            .addAllIpAddresses(List.of("1.2.3.4", "12.42.33.41"))
            .build();

    Struct ruleDetailsStructure =
        Struct.newBuilder()
            .putFields("name", Value.newBuilder().setStringValue("Tester-1").build())
            .putFields(
                "description", Value.newBuilder().setStringValue("Range rule test 1").build())
            .putFields(
                "rawInputIpData",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("1.1.1.1/16").build())
                            .addValues(Value.newBuilder().setStringValue("1.2.3.4").build())
                            .addValues(Value.newBuilder().setStringValue("12.42.33.41").build())
                            .build())
                    .build())
            .putFields(
                "ruleAction", Value.newBuilder().setStringValue(RULE_ACTION_BLOCK.name()).build())
            .build();

    Struct ruleConfigStructure =
        Struct.newBuilder()
            .putFields("id", Value.newBuilder().setStringValue("test-id").build())
            .putFields(
                "ruleDetails", Value.newBuilder().setStructValue(ruleDetailsStructure).build())
            .putFields("internal", Value.newBuilder().setBoolValue(true).build())
            .putFields(
                "ipRanges",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("1.1.1.1/16").build())
                            .build())
                    .build())
            .putFields(
                "ipAddresses",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("1.2.3.4").build())
                            .addValues(Value.newBuilder().setStringValue("12.42.33.41").build())
                            .build())
                    .build())
            .build();

    Value ipRangeRuleConfig = ipRangeRuleConverter.convert(ipRangeRule);
    Value expectedIpRangeRuleConfig =
        Value.newBuilder().setStructValue(ruleConfigStructure).build();

    assertEquals(ipRangeRuleConfig, expectedIpRangeRuleConfig);

    IpRangeRule reconvertedIpRangeRule = ipRangeRuleConverter.convert(ipRangeRuleConfig);
    assertEquals(reconvertedIpRangeRule, ipRangeRule);
  }
}
