package ai.traceable.blocking.config.service.common.rules;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.common.rules.fetchers.CustomSignatureRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.DlpRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.DlpRulesFetcher.DlpModsecRulesData;
import ai.traceable.blocking.config.service.common.rules.fetchers.MaliciousSourcesRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RegionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.region.config.service.v1.DetailedRegion;
import com.google.common.base.Suppliers;
import java.util.LinkedHashMap;
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

  private static final String NEW_LINE_DELIMITER = "\n";
  private final BlockingRulesSupplierContext blockingRulesSupplierContext;
  private final RequestContext requestContext;
  private final Optional<String> environmentId;

  private final Function<CustomModsecRuleVersion, GetCustomSignatureModsecRulesResponse>
      customSignatureRulesGetter;
  private final Function<List<String>, Map<String, DlpModsecRulesData>> dlpModsecRulesGetter;
  private final ConcurrentMap<CustomModsecRuleVersion, GetCustomSignatureModsecRulesResponse>
      customSignatureRulesMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, DlpModsecRulesData> dlpRulesMap = new ConcurrentHashMap<>();
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

    dlpModsecRulesGetter =
        serviceNames ->
            ((DlpRulesFetcher)
                    blockingRulesSupplierContext.getRulesFetcher(RulesFetcher.RulesFetcherType.DLP))
                .fetchDlpModsecRules(requestContext, environmentId, serviceNames);

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

  /** Returns the custom signature rules modsec blob for the specified version */
  public String getCustomSignatureModsecBlob(CustomModsecRuleVersion version) {

    try {
      return customSignatureRulesMap
          .computeIfAbsent(version, customSignatureRulesGetter)
          .getModsecRulesBlob();
    } catch (Exception e) {
      log.error(
          "Error in fetching custom signature modsec rules for request context: {} and environment: {}",
          requestContext,
          environmentId);
      return "";
    }
  }

  /** Returns the list of modsec blobs keyed by service name */
  public Map<String, String> getCustomSignatureModsecBlobs(
      CustomModsecRuleVersion version, List<String> serviceNames) {

    String customSignatureModsecRulesBlob = getCustomSignatureModsecBlob(version);

    if (CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE
        .equals(version)) {

      fetchDlpRulesForMissingServiceNamesIfAny(serviceNames);
      return serviceNames.stream()
          .collect(
              Collectors.toMap(
                  Function.identity(),
                  serviceName ->
                      getCombinedModsecBlobs(
                          customSignatureModsecRulesBlob, dlpRulesMap.get(serviceName))));
    }

    // Case where Dlp rules are not fetched.
    return serviceNames.stream()
        .collect(Collectors.toMap(Function.identity(), sName -> customSignatureModsecRulesBlob));
  }

  /** Returns list of Region to Ip-range mappings in the form of objects defined by the converter */
  public <T> List<T> getRegionIpMappings(Function<DetailedRegion, T> ruleConverter) {
    return regionRulesSupplier.get().stream()
        .map(ruleConverter::apply)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Returns list of Ip-Type to Ip-range mappings in the form of objects defined by the converter
   */
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

  /** Returns the list of DLP rules keyed by service name */
  public Map<String, List<RateLimitingModsecRule>> getDlpRules(List<String> serviceNames) {
    fetchDlpRulesForMissingServiceNamesIfAny(serviceNames);
    return serviceNames.stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                serviceName ->
                    dlpRulesMap.getOrDefault(serviceName, new DlpModsecRulesData()).getRules(),
                (existingList, newList) -> existingList,
                LinkedHashMap::new));
  }

  /** Method to fetch DLP rules for service not present in the map */
  private void fetchDlpRulesForMissingServiceNamesIfAny(List<String> serviceNames) {
    dlpRulesMap.putAll(
        dlpModsecRulesGetter.apply(
            serviceNames.stream()
                .filter(serviceName -> !dlpRulesMap.containsKey(serviceName))
                .distinct()
                .collect(Collectors.toList())));
  }

  /** Method to combine DLP rules modsec blob with custom signature rules modsec blob */
  private String getCombinedModsecBlobs(
      String customSignatureModsecRulesBlob, DlpModsecRulesData dlpModsecRulesData) {
    String modsecRulesBlobPrefix = customSignatureModsecRulesBlob;
    // no dlp rules, return custom-signature blob as is
    if (dlpModsecRulesData == null || dlpModsecRulesData.getModsecDirectivesBlob().isEmpty()) {
      return modsecRulesBlobPrefix;
    }
    // no custom signature rules blob - add modsec directives from dlp rules modsec blob data
    // if custom signature rules blob is present, it already contains the necessary directives
    if (modsecRulesBlobPrefix.isEmpty()) {
      modsecRulesBlobPrefix = dlpModsecRulesData.getModsecDirectivesBlob();
    }
    if (modsecRulesBlobPrefix.isEmpty()) {
      return modsecRulesBlobPrefix;
    } else {
      return modsecRulesBlobPrefix + NEW_LINE_DELIMITER + dlpModsecRulesData.getModsecRulesBlob();
    }
  }
}
