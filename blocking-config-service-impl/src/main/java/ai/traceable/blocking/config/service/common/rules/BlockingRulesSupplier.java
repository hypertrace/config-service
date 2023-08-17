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
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.region.config.service.v1.DetailedRegion;
import com.google.common.base.Suppliers;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private final Function<List<String>, Map<String, DetailedRegion>> countryIsoCodeRegionsGetter;
  private final ConcurrentMap<CustomModsecRuleVersion, GetCustomSignatureModsecRulesResponse>
      customSignatureRulesMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, DlpModsecRulesData> dlpRulesMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, DetailedRegion> countryIsoCodeRegionsMap =
      new ConcurrentHashMap<>();
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

    countryIsoCodeRegionsGetter =
        countryIsoCodes ->
            ((RegionRulesFetcher)
                    blockingRulesSupplierContext.getRulesFetcher(
                        RulesFetcher.RulesFetcherType.REGION))
                .fetchDetailedRegions(countryIsoCodes);

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
    if (serviceNames.isEmpty()) {
      return Collections.emptyMap();
    }

    String customSignatureModsecRulesBlob = getCustomSignatureModsecBlob(version);
    Function<String, String> combinedModsecBlobFunction;

    if (CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE
        .equals(version)) {
      fetchDlpRulesForMissingServiceNamesIfAny(serviceNames);
      combinedModsecBlobFunction =
          serviceName ->
              getCombinedModsecBlobs(customSignatureModsecRulesBlob, dlpRulesMap.get(serviceName));
    } else {
      // Case where Dlp rules are not fetched.
      combinedModsecBlobFunction = serviceName -> customSignatureModsecRulesBlob;
    }

    return serviceNames.stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                combinedModsecBlobFunction,
                (oldValue, newValue) -> oldValue,
                LinkedHashMap::new));
  }

  /** Returns list of Region to Ip-range mappings in the form of objects defined by the converter */
  public <T> List<T> getRegionIpMappings(
      Function<DetailedRegion, T> ruleConverter, List<String> serviceNames) {

    List<DetailedRegion> detailedRegionsFromRegionRules = regionRulesSupplier.get();
    if (serviceNames.isEmpty()) {
      return detailedRegionsFromRegionRules.stream()
          .map(ruleConverter)
          .collect(Collectors.toUnmodifiableList());
    }

    Set<String> regionRulesCountryIsoCodes =
        getAndPopulateRegionRulesCountryIsoCodes(detailedRegionsFromRegionRules);
    List<String> dlpRulesCountryIsoCodes = getDlpRulesCountryIsoCodes(serviceNames);

    return Stream.concat(regionRulesCountryIsoCodes.stream(), dlpRulesCountryIsoCodes.stream())
        .distinct()
        .map(countryIsoCodeRegionsMap::get)
        .filter(Objects::nonNull)
        .map(ruleConverter)
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
        .map(ruleConverter)
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
    return modsecRulesBlobPrefix + NEW_LINE_DELIMITER + dlpModsecRulesData.getModsecRulesBlob();
  }

  private Set<String> getAndPopulateRegionRulesCountryIsoCodes(
      List<DetailedRegion> detailedRegions) {
    Map<String, DetailedRegion> countryIsoCodeDetailedRegionsMap =
        detailedRegions.stream()
            .collect(
                Collectors.toMap(
                    detailedRegion -> detailedRegion.getRegion().getCountry().getIsoCode(),
                    Function.identity(),
                    (oldValue, newValue) -> oldValue,
                    LinkedHashMap::new));
    // To avoid duplicate fetch from region-rules-grpc service
    this.countryIsoCodeRegionsMap.putAll(countryIsoCodeDetailedRegionsMap);
    return countryIsoCodeDetailedRegionsMap.keySet();
  }

  private List<String> getDlpRulesCountryIsoCodes(List<String> serviceNames) {
    fetchDlpRulesForMissingServiceNamesIfAny(serviceNames);

    // To avoid using entire message for identifying distinct rules
    // to subsequently avoid extracting ip-types from same rule multiple times
    Set<String> ruleIds = ConcurrentHashMap.newKeySet();
    List<String> dlpRulesCountryIsoCodes =
        serviceNames.stream()
            .flatMap(
                serviceName ->
                    dlpRulesMap
                        .getOrDefault(serviceName, new DlpModsecRulesData())
                        .getRules()
                        .stream())
            .filter(rule -> ruleIds.add(rule.getId()))
            .flatMap(rule -> extractDlpCountryIsoCodes(rule.getData().getCondition()))
            .distinct()
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    countryIsoCodeRegionsMap.putAll(
        countryIsoCodeRegionsGetter.apply(
            dlpRulesCountryIsoCodes.stream()
                .filter(serviceName -> !countryIsoCodeRegionsMap.containsKey(serviceName))
                .distinct()
                .collect(Collectors.toList())));
    return dlpRulesCountryIsoCodes;
  }

  private static Stream<String> extractDlpCountryIsoCodes(Condition condition) {
    return condition.hasLeafCondition()
        ? extractDlpCountryIsoCodes(condition.getLeafCondition())
        : condition.getCompositeCondition().getChildrenList().stream()
            .flatMap(BlockingRulesSupplier::extractDlpCountryIsoCodes);
  }

  private static Stream<String> extractDlpCountryIsoCodes(LeafCondition condition) {
    return condition.getRegionCondition().getRegionIdentifiersList().stream()
        .map(RegionCondition.Region::getCountryIsoCode);
  }
}
