package ai.traceable.blocking.config.service.iptype;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.blocking.config.service.v1.IpRange;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.blocking.config.service.v1.IpV4Range;
import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IpTypeRulesLoaderTest {
  private final Map<IpType, IpTypeRule> testIpTypeRules = new HashMap<>();

  @Test
  void buildIpTypeRules() {
    insertTestDataForVerification();
    IpTypeRulesLoader builder = new IpTypeRulesLoader(new LatestInstantNamedPathFinder());
    List<IpTypeRule> ipTypeRules =
        builder
            .getLatestDataSupplier(
                new FileRefreshConfig(
                    ConfigFactory.parseMap(
                        Map.of("mode", "RESOURCE_FILE", "resource.file", "iptype/highrisk.csv"))))
            .get();
    assertEquals(5, ipTypeRules.size());
    ipTypeRules.forEach(rule -> assertEquals(testIpTypeRules.get(rule.getIpType()), rule));
  }

  private void insertTestDataForVerification() {
    // based on data in test/resources/iptype/highrisk.csv
    testIpTypeRules.put(
        IpType.IP_TYPE_BOT, getIpTypeRule(IpType.IP_TYPE_BOT, List.of(1L, 2L, 4L, 4L)));
    testIpTypeRules.put(
        IpType.IP_TYPE_PROXY, getIpTypeRule(IpType.IP_TYPE_PROXY, List.of(10L, 22L, 4L, 4L)));
    testIpTypeRules.put(
        IpType.IP_TYPE_TOR, getIpTypeRule(IpType.IP_TYPE_TOR, List.of(1L, 2L, 40L, 40L)));
    testIpTypeRules.put(
        IpType.IP_TYPE_HOSTING_PROVIDER,
        getIpTypeRule(IpType.IP_TYPE_HOSTING_PROVIDER, List.of(11L, 21L, 4L, 4L)));
    testIpTypeRules.put(
        IpType.IP_TYPE_VPN, getIpTypeRule(IpType.IP_TYPE_VPN, List.of(31L, 42L, 43L, 49L)));
  }

  private IpTypeRule getIpTypeRule(IpType ipType, List<Long> rangePairs) {
    assert rangePairs.size() % 2 == 0;
    List<IpRange> ipRanges = new ArrayList<>();
    for (int i = 0; i < rangePairs.size(); i += 2) {
      long startIp = rangePairs.get(i);
      long endIp = rangePairs.get(i + 1);
      if (startIp == endIp) {
        ipRanges.add(IpRange.newBuilder().setIpv4Address((int) startIp).build());
      } else {
        ipRanges.add(
            IpRange.newBuilder()
                .setIpv4Range(IpV4Range.newBuilder().setStartIp(startIp).setEndIp(endIp).build())
                .build());
      }
    }
    return IpTypeRule.newBuilder().setIpType(ipType).addAllIpRanges(ipRanges).build();
  }
}
