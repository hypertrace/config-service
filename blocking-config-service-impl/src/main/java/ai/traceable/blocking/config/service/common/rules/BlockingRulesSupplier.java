package ai.traceable.blocking.config.service.common.rules;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.common.rules.fetchers.CustomSignatureRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.MaliciousSourcesRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RegionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.region.config.service.v1.DetailedRegion;
import com.google.common.base.Suppliers;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class BlockingRulesSupplier {

  private final BlockingRulesSupplierContext blockingRulesSupplierContext;
  private final RequestContext requestContext;
  private final Optional<String> environmentId;

  private final Function<CustomModsecRuleVersion, GetCustomSignatureModsecRulesResponse>
      customSignatureRulesGetter;
  private final ConcurrentMap<CustomModsecRuleVersion, GetCustomSignatureModsecRulesResponse>
      customSignatureRulesMap = new ConcurrentHashMap<>();
  private final Supplier<List<DetailedRegion>> regionRulesSupplier;
  private final Supplier<List<MaliciousSourcesRule>> maliciousSourcesRulesSupplier;

  public BlockingRulesSupplier(
      BlockingRulesSupplierContext blockingRulesSupplierContext,
      RequestContext requestContext,
      Optional<String> environmentId) {

    this.blockingRulesSupplierContext = blockingRulesSupplierContext;
    this.requestContext = requestContext;
    this.environmentId = environmentId;

    customSignatureRulesGetter =
        version ->
            ((CustomSignatureRulesFetcher)
                    blockingRulesSupplierContext.getRulesFetcher(
                        RulesFetcher.RulesFetcherType.CUSTOM_SIGNATURE))
                .fetchModsecRules(requestContext, environmentId, version);

    regionRulesSupplier =
        Suppliers.memoize(
            () ->
                ((RegionRulesFetcher)
                        blockingRulesSupplierContext.getRulesFetcher(
                            RulesFetcher.RulesFetcherType.REGION))
                    .fetchRegionRules(requestContext, environmentId));

    maliciousSourcesRulesSupplier =
        Suppliers.memoize(
            () ->
                ((MaliciousSourcesRulesFetcher)
                        blockingRulesSupplierContext.getRulesFetcher(
                            RulesFetcher.RulesFetcherType.MALICIOUS_SOURCES))
                    .fetchRules(requestContext, environmentId));
  }

  public RequestContext getRequestContext() {
    return requestContext;
  }

  public Optional<String> getEnvironmentId() {
    return environmentId;
  }

  public String getCustomSignatureModsecBlob(CustomModsecRuleVersion version) {
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

  public <T> List<T> getRegionIpMappings(Function<DetailedRegion, T> ruleConverter) {
    return regionRulesSupplier.get().stream()
        .map(ruleConverter::apply)
        .collect(Collectors.toUnmodifiableList());
  }

  public <T> List<T> getIpTypeIpMappings(Function<IpTypeRuleInfo, T> ruleConverter) {
    Map<IpLocationType, IpTypeRuleInfo> ipTypesInfoMap =
        blockingRulesSupplierContext.getIpTypeRulesInfoMap();
    return maliciousSourcesRulesSupplier.get().stream()
        .flatMap(
            maliciousSourcesRule ->
                maliciousSourcesRule.getRuleInfo().getConditionsList().stream()
                    .filter(MaliciousSourcesRuleCondition::hasIpLocationTypeCondition)
                    .flatMap(
                        condition ->
                            condition
                                .getIpLocationTypeCondition()
                                .getIpLocationTypesList()
                                .stream()))
        .distinct()
        .filter(Objects::nonNull)
        .map(ipTypesInfoMap::get)
        .filter(Objects::nonNull)
        .map(ruleConverter::apply)
        .collect(Collectors.toUnmodifiableList());
  }
}
