package ai.traceable.blocking.config.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import ai.traceable.blocking.config.service.v1.BlockingRules;
import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.blocking.config.service.v1.IpRange;
import ai.traceable.blocking.config.service.v1.IpV4Range;
import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.blocking.config.service.v1.RegionIpBlockingDetails;
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
    RegionBlockingRules regionBlockingRules =
        RegionBlockingRules.newBuilder()
            .addRegionIpBlockingDetails(
                RegionIpBlockingDetails.newBuilder()
                    .setRegionId("region-1")
                    .addIpRanges(
                        IpRange.newBuilder()
                            .setIpv4Range(
                                IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L).build())
                            .build())
                    .build())
            .build();
    BlockingRules blockingRules =
        BlockingRules.newBuilder().setRegionBlockingRules(regionBlockingRules).build();
    assertNotNull(this.hashGenerator.generate(blockingRules));
  }

  @Test
  @DisplayName("generate same hash for same input")
  void shouldGenerate_sameHash_sameInput() {
    List<RegionIpBlockingDetails> regionIpBlockingDetails =
        List.of(
            RegionIpBlockingDetails.newBuilder()
                .setRegionId("region-1")
                .addIpRanges(
                    IpRange.newBuilder()
                        .setIpv4Range(
                            IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L).build())
                        .build())
                .build());

    BlockingRules blockingRules1 =
        BlockingRules.newBuilder()
            .setRegionBlockingRules(
                RegionBlockingRules.newBuilder()
                    .addAllRegionIpBlockingDetails(regionIpBlockingDetails))
            .setCustomModsecBlockingRules(
                CustomModsecBlockingRules.newBuilder().setCustomModsecRulesBlob("1234").build())
            .build();
    BlockingRules blockingRules2 =
        BlockingRules.newBuilder()
            .setRegionBlockingRules(
                RegionBlockingRules.newBuilder()
                    .addAllRegionIpBlockingDetails(List.copyOf(regionIpBlockingDetails)))
            .setCustomModsecBlockingRules(
                CustomModsecBlockingRules.newBuilder().setCustomModsecRulesBlob("1234").build())
            .build();
    assertEquals(
        this.hashGenerator.generate(blockingRules1), this.hashGenerator.generate(blockingRules2));
  }

  @Test
  @DisplayName("generate different hash for different input")
  void shouldGenerate_differentHash_differentInput() {

    List<RegionIpBlockingDetails> regionIpBlockingDetails1 =
        List.of(
            RegionIpBlockingDetails.newBuilder()
                .setRegionId("region-1")
                .addIpRanges(
                    IpRange.newBuilder()
                        .setIpv4Range(
                            IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L).build())
                        .build())
                .build());
    List<RegionIpBlockingDetails> regionIpBlockingDetails2 =
        List.of(
            RegionIpBlockingDetails.newBuilder()
                .setRegionId("region-1")
                .addIpRanges(
                    IpRange.newBuilder()
                        .setIpv4Range(
                            IpV4Range.newBuilder().setStartIp(234L).setEndIp(789L).build())
                        .build())
                .build());

    BlockingRules blockingRules1 =
        BlockingRules.newBuilder()
            .setRegionBlockingRules(
                RegionBlockingRules.newBuilder()
                    .addAllRegionIpBlockingDetails(regionIpBlockingDetails1))
            .setCustomModsecBlockingRules(
                CustomModsecBlockingRules.newBuilder().setCustomModsecRulesBlob("1234").build())
            .build();
    BlockingRules blockingRules2 =
        BlockingRules.newBuilder()
            .setRegionBlockingRules(
                RegionBlockingRules.newBuilder()
                    .addAllRegionIpBlockingDetails(List.copyOf(regionIpBlockingDetails2)))
            .setCustomModsecBlockingRules(
                CustomModsecBlockingRules.newBuilder().setCustomModsecRulesBlob("4567").build())
            .build();

    assertNotEquals(
        this.hashGenerator.generate(blockingRules1), this.hashGenerator.generate(blockingRules2));
  }
}
