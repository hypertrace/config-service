package ai.traceable.blocking.config.service.common.rules;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRulesLoader;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import java.util.Map;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BlockingRulesSupplierContext {

  private final Map<RulesFetcher.RulesFetcherType, RulesFetcher> rulesFetchers;
  private final IpTypeRulesLoader ipTypeRulesLoader;

  @Inject
  public BlockingRulesSupplierContext(
      Map<RulesFetcher.RulesFetcherType, RulesFetcher> rulesFetchers,
      IpTypeRulesLoader ipTypeRulesLoader) {
    this.rulesFetchers = rulesFetchers;
    this.ipTypeRulesLoader = ipTypeRulesLoader;
  }

  public RulesFetcher getRulesFetcher(RulesFetcher.RulesFetcherType fetcherType) {
    return rulesFetchers.get(fetcherType);
  }

  public Map<IpTypeRuleInfo.IpType, IpTypeRuleInfo> getIpTypeRulesInfoMap() {
    return ipTypeRulesLoader.getLatestDataSupplier().get();
  }
}
