package ai.traceable.blocking.config.service.common.iptype;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo.IpRangeInfo;
import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import com.typesafe.config.ConfigFactory;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IpTypeRulesLoaderTest {
  private final Map<IpTypeRuleInfo.IpType, IpTypeRuleInfo> testIpTypeRules =
      new EnumMap<>(IpTypeRuleInfo.IpType.class);

  @Test
  void buildIpTypeRules() {
    insertTestDataForVerification();
    IpTypeRulesLoader builder =
        new IpTypeRulesLoader(
            new LatestInstantNamedPathFinder(),
            new FileRefreshConfig(
                ConfigFactory.parseMap(
                    Map.of("mode", "RESOURCE_FILE", "resource.file", "iptype/highrisk.csv"))));
    Map<IpTypeRuleInfo.IpType, IpTypeRuleInfo> ipTypeRulesMap =
        builder.getLatestDataSupplier().get();
    assertEquals(5, ipTypeRulesMap.size());
    ipTypeRulesMap
        .entrySet()
        .forEach(entry -> assertEquals(testIpTypeRules.get(entry.getKey()), entry.getValue()));
  }

  private void insertTestDataForVerification() {
    // based on data in test/resources/iptype/highrisk.csv
    testIpTypeRules.put(
        IpTypeRuleInfo.IpType.BOT,
        getIpTypeRule(IpTypeRuleInfo.IpType.BOT, List.of(1L, 2L, 4L, 4L)));
    testIpTypeRules.put(
        IpTypeRuleInfo.IpType.PUBLIC_PROXY,
        getIpTypeRule(IpTypeRuleInfo.IpType.PUBLIC_PROXY, List.of(10L, 22L, 4L, 4L)));
    testIpTypeRules.put(
        IpTypeRuleInfo.IpType.TOR_EXIT_NODE,
        getIpTypeRule(IpTypeRuleInfo.IpType.TOR_EXIT_NODE, List.of(1L, 2L, 40L, 40L)));
    testIpTypeRules.put(
        IpTypeRuleInfo.IpType.HOSTING_PROVIDER,
        getIpTypeRule(IpTypeRuleInfo.IpType.HOSTING_PROVIDER, List.of(11L, 21L, 4L, 4L)));
    testIpTypeRules.put(
        IpTypeRuleInfo.IpType.ANONYMOUS_VPN,
        getIpTypeRule(IpTypeRuleInfo.IpType.ANONYMOUS_VPN, List.of(31L, 42L, 43L, 49L)));
  }

  private IpTypeRuleInfo getIpTypeRule(IpTypeRuleInfo.IpType ipType, List<Long> rangePairs) {
    assert rangePairs.size() % 2 == 0;

    IpTypeRuleInfo commonIpTypeRule = new IpTypeRuleInfo(ipType);
    for (int i = 0; i < rangePairs.size(); i += 2) {
      long startIp = rangePairs.get(i);
      long endIp = rangePairs.get(i + 1);
      if (startIp == endIp) {
        commonIpTypeRule.getIpv4Addresses().add((int) startIp);
      } else {
        commonIpTypeRule.getIpv4Ranges().add(new IpRangeInfo((int) startIp, (int) endIp));
      }
    }
    return commonIpTypeRule;
  }
}
