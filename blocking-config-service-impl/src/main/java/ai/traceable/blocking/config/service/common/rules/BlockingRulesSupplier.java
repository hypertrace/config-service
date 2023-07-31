package ai.traceable.blocking.config.service.common.rules;

import ai.traceable.blocking.config.service.common.rules.fetchers.CustomSignatureRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.MaliciousSourcesRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RegionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.region.config.service.v1.DetailedRegion;
import com.google.common.base.Suppliers;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class BlockingRulesSupplier {

  private final RequestContext requestContext;
  private final Optional<String> environmentId;

  private final Function<CustomModsecRuleVersion, GetCustomSignatureModsecRulesResponse>
      customSignatureRulesGetter;
  private final ConcurrentMap<CustomModsecRuleVersion, GetCustomSignatureModsecRulesResponse>
      customSignatureRulesMap = new ConcurrentHashMap<>();
  private final Supplier<List<DetailedRegion>> regionRulesSupplier;
  private final Supplier<List<MaliciousSourcesRule>> maliciousSourcesRulesSupplier;

  public BlockingRulesSupplier(
      Map<RulesFetcher.RulesFetcherType, RulesFetcher> rulesFetchers,
      RequestContext requestContext,
      Optional<String> environmentId) {

    this.requestContext = requestContext;
    this.environmentId = environmentId;

    customSignatureRulesGetter =
        version ->
            ((CustomSignatureRulesFetcher)
                    rulesFetchers.get(RulesFetcher.RulesFetcherType.CUSTOM_SIGNATURE))
                .fetchModsecRules(requestContext, environmentId, version);

    regionRulesSupplier =
        Suppliers.memoize(
            () ->
                ((RegionRulesFetcher) rulesFetchers.get(RulesFetcher.RulesFetcherType.REGION))
                    .fetchRegionRules(requestContext, environmentId));

    maliciousSourcesRulesSupplier =
        Suppliers.memoize(
            () ->
                ((MaliciousSourcesRulesFetcher)
                        rulesFetchers.get(RulesFetcher.RulesFetcherType.MALICIOUS_SOURCES))
                    .fetchRules(requestContext, environmentId));
  }

  public RequestContext getRequestContext() {
    return requestContext;
  }

  public Optional<String> getEnvironmentId() {
    return environmentId;
  }

  public String getCustomSignatureRulesBlob(CustomModsecRuleVersion version) {
    try {
      return customSignatureRulesMap
          .computeIfAbsent(version, customSignatureRulesGetter)
          .getModsecRulesBlob();
    } catch (Exception e) {
      log.error(
          "Error in fetching custom signature modsec rules for request context: {} and environment",
          requestContext,
          environmentId);
      return "";
    }
  }
}
