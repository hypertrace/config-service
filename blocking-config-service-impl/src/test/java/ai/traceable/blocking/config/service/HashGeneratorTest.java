package ai.traceable.blocking.config.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import ai.traceable.blocking.config.service.v1.BlockingRule;
import ai.traceable.blocking.config.service.v1.IpBlocking;
import ai.traceable.blocking.config.service.v1.IpRange;
import ai.traceable.blocking.config.service.v1.IpV4Range;
import ai.traceable.blocking.config.service.v1.RuleActionType;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HashGeneratorTest {
  private HashGenerator hashGenerator;

  @BeforeEach
  void setup() {
    this.hashGenerator = new HashGenerator();
  }

  @Test
  void shouldGenerateHash() {
    List<BlockingRule> rules =
        List.of(
            BlockingRule.newBuilder()
                .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                .setIpBlocking(
                    IpBlocking.newBuilder()
                        .addIpRange(
                            IpRange.newBuilder()
                                .setIpv4Range(
                                    IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L).build())
                                .build())
                        .build())
                .build(),
            BlockingRule.getDefaultInstance());
    assertNotNull(this.hashGenerator.generate(rules));
  }

  @Test
  @DisplayName("generate same hash for same input")
  void shouldGenerate_sameHash_sameInput() {
    List<BlockingRule> rules =
        List.of(
            BlockingRule.newBuilder()
                .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                .setIpBlocking(
                    IpBlocking.newBuilder()
                        .addIpRange(
                            IpRange.newBuilder()
                                .setIpv4Range(
                                    IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L).build())
                                .build())
                        .build())
                .build(),
            BlockingRule.getDefaultInstance());
    assertEquals(
        this.hashGenerator.generate(List.copyOf(rules)), this.hashGenerator.generate(rules));
  }

  @Test
  @DisplayName("generate different hash for different input")
  void shouldGenerate_differentHash_differentInput() {
    List<BlockingRule> rules1 =
        List.of(
            BlockingRule.newBuilder()
                .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                .setIpBlocking(
                    IpBlocking.newBuilder()
                        .addIpRange(
                            IpRange.newBuilder()
                                .setIpv4Range(
                                    IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L).build())
                                .build())
                        .build())
                .build());

    List<BlockingRule> rules2 =
        List.of(
            BlockingRule.newBuilder()
                .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                .setIpBlocking(
                    IpBlocking.newBuilder()
                        .addIpRange(
                            IpRange.newBuilder()
                                .setIpv4Range(
                                    IpV4Range.newBuilder().setStartIp(234L).setEndIp(789L).build())
                                .build())
                        .build())
                .build());
    assertNotEquals(this.hashGenerator.generate(rules2), this.hashGenerator.generate(rules1));
  }
}
