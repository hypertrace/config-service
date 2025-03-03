package ai.traceable.blocking.config.service.common.rules;

import static ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.common.rules.fetchers.CustomSignatureRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.DlpRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.ExclusionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.MaliciousSourcesRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RegionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher.RulesFetcherType;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.IpType;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition.Region;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.IpLocationType;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.region.config.service.v1.Country;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.RegionRule;
import com.google.common.base.Suppliers;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * Class to supply all rules info by fetching and combining data from multiple GRPC services
 * Assumption: Agents supporting DLP rules combination will always send service name
 */
@Slf4j
public class BlockingRulesSupplierImpl implements BlockingRulesSupplier {

  private static final String NEW_LINE_DELIMITER = "\n\n";

  private final BlockingRulesSupplierContext blockingRulesSupplierContext;
  private final RequestContext requestContext;
  private final Optional<String> environmentId;
  private final Function<CustomModsecRuleVersion, GetCustomSignatureModsecRulesResponse>
      customSignatureRulesGetter;
  private final Function<Set<String>, Map<String, ModsecRulesData<RateLimitingModsecRule>>>
      dlpModsecRulesGetter;
  private final Function<Set<String>, Map<String, ModsecRulesData<DetectionExclusionModsecRule>>>
      exclusionModsecRulesGetter;
  private final Function<List<String>, List<DetailedRegion>> countryIsoCodeRegionsGetter;
  private final ConcurrentMap<CustomModsecRuleVersion, GetCustomSignatureModsecRulesResponse>
      customSignatureRulesMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ModsecRulesData<RateLimitingModsecRule>> dlpRulesMap =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ModsecRulesData<DetectionExclusionModsecRule>>
      exclusionRulesMap = new ConcurrentHashMap<>();
  private final Supplier<List<RegionRule>> regionRulesSupplier;
  private final Supplier<List<MaliciousSourcesRule>> maliciousSourcesRulesSupplier;

  public BlockingRulesSupplierImpl(
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
            ((DlpRulesFetcher) blockingRulesSupplierContext.getRulesFetcher(RulesFetcherType.DLP))
                .fetchDlpModsecRules(requestContext, environmentId, serviceNames);

    exclusionModsecRulesGetter =
        serviceNames ->
            ((ExclusionRulesFetcher)
                    blockingRulesSupplierContext.getRulesFetcher(RulesFetcherType.EXCLUSION))
                .fetchExclusionModsecRules(requestContext, environmentId, serviceNames);

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
          environmentId,
          e);
      return "";
    }
  }

  /**
   * Returns the list of modsec blobs (combination of custom signature and DLP rules) keyed by
   * service name
   */
  public Map<String, String> getCustomModsecBlobs(
      CustomModsecRuleVersion version, Set<String> serviceNames) {
    if (serviceNames.isEmpty()) {
      // no dlp or exclusion rules would be fetched if service-name is not provided.
      // assumption: agents supporting DLP rules will always send service-name.
      return Collections.emptyMap();
    }

    String customSignatureModsecRulesBlob = getCustomSignatureModsecBlob(version);
    Function<String, String> combinedModsecBlobFunction;

    if (CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE
        .equals(version)) {
      // DLP and or exclusion rules are supported only when multi-match is enabled.
      fetchDlpRulesForMissingServiceNamesIfAny(serviceNames);
      fetchExclusionRulesForMissingServiceNamesIfAny(serviceNames);
      combinedModsecBlobFunction =
          serviceName ->
              getCombinedModsecBlobs(
                  customSignatureModsecRulesBlob,
                  dlpRulesMap.get(serviceName),
                  exclusionRulesMap.get(serviceName));
    } else {
      // When Dlp rules are not supported, only return the custom signature rules.
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
      Function<DetailedRegion, T> ruleConverter, Set<String> serviceNames) {

    List<String> combinedIsoCodes =
        Stream.of(
                regionRulesSupplier.get().stream()
                    .flatMap(
                        regionRule -> regionRule.getRegionIdToCountryMapMap().values().stream())
                    .map(Country::getIsoCode),
                getCustomSignatureRulesCountryIsoCodes().stream(),
                getDlpRulesCountryIsoCodes(serviceNames).stream(),
                getExclusionRulesCountryIsoCodes(serviceNames).stream())
            .flatMap(Function.identity())
            .distinct()
            .collect(Collectors.toUnmodifiableList());

    if (combinedIsoCodes.isEmpty()) {
      return List.of();
    }

    return countryIsoCodeRegionsGetter.apply(combinedIsoCodes).stream()
        .map(ruleConverter)
        .collect(Collectors.toUnmodifiableList());
  }

  public List<RegionRule> getRegionRules() {
    return regionRulesSupplier.get();
  }

  /**
   * Returns list of Ip-Type to Ip-range mappings in the form of objects defined by the converter
   * keyed by service name
   */
  public <T> List<ImmutablePair<T, String>> getIpTypeIpMappings(
      Function<IpTypeRuleInfo, ImmutablePair<T, String>> ruleConverter, Set<String> serviceNames) {
    Map<IpTypeRuleInfo.IpType, IpTypeRuleInfo> ipTypesInfoMap =
        blockingRulesSupplierContext.getIpTypeRulesInfoMap();

    Stream<IpTypeRuleInfo.IpType> maliciousSourcesIpTypes = getMaliciousSourcesIpTypes();
    Stream<IpTypeRuleInfo.IpType> customSignatureIpTypes = getCustomSignatureIpTypes();
    if (serviceNames.isEmpty()) {
      // no dlp or exclusion rules would be fetched if service-name is not provided.
      // assumption: agents supporting DLP rules will always send service-name.
      return convertIpTypeIpMappings(
          ruleConverter,
          ipTypesInfoMap,
          Stream.of(maliciousSourcesIpTypes, customSignatureIpTypes)
              .flatMap(Function.identity())
              .distinct());
    }

    Stream<IpTypeRuleInfo.IpType> dlpRulesIpTypes = getDlpRulesIpTypes(serviceNames);
    Stream<IpTypeRuleInfo.IpType> exclusionRulesIpTypes = getExclusionRulesIpTypes(serviceNames);

    return convertIpTypeIpMappings(
        ruleConverter,
        ipTypesInfoMap,
        Stream.of(
                maliciousSourcesIpTypes,
                customSignatureIpTypes,
                dlpRulesIpTypes,
                exclusionRulesIpTypes)
            .flatMap(Function.identity())
            .distinct());
  }

  /** Returns the list of DLP rules keyed by service name */
  public Map<String, List<RateLimitingModsecRule>> getDlpRules(Set<String> serviceNames) {
    if (serviceNames.isEmpty()) {
      // no dlp rules would be fetched if service-name is not provided.
      // assumption: agents supporting DLP rules will always send service-name.
      return Collections.emptyMap();
    }
    fetchDlpRulesForMissingServiceNamesIfAny(serviceNames);

    return serviceNames.stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                serviceName ->
                    dlpRulesMap.getOrDefault(serviceName, new ModsecRulesData<>()).getRules(),
                (existingList, newList) -> existingList,
                LinkedHashMap::new));
  }

  /** Returns the list of Exclusion rules keyed by service name */
  public Map<String, List<DetectionExclusionModsecRule>> getExclusionRules(
      Set<String> serviceNames) {
    if (serviceNames.isEmpty()) {
      // no exclusion rules would be fetched if service-name is not provided.
      return Collections.emptyMap();
    }
    fetchExclusionRulesForMissingServiceNamesIfAny(serviceNames);

    return serviceNames.stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                serviceName ->
                    exclusionRulesMap.getOrDefault(serviceName, new ModsecRulesData<>()).getRules(),
                (existingList, newList) -> existingList,
                LinkedHashMap::new));
  }

  @Override
  public List<CustomSignatureInlineRule> getCustomSignatureInlineRules() {
    // we just need to return any value from the map irrespective of the key because inline rules
    // list is independent of modsec version. If no value is present then we need to compute for
    // some version & return
    return customSignatureRulesMap.values().stream()
        .findFirst()
        .orElseGet(
            () ->
                customSignatureRulesMap.computeIfAbsent(
                    CUSTOM_MODSEC_RULE_VERSION_V3, customSignatureRulesGetter))
        .getInlineRulesList();
  }

  /** Method to fetch DLP rules for service not present in the map */
  private void fetchDlpRulesForMissingServiceNamesIfAny(Set<String> serviceNames) {
    dlpRulesMap.putAll(
        dlpModsecRulesGetter.apply(
            serviceNames.stream()
                .filter(serviceName -> !dlpRulesMap.containsKey(serviceName))
                .collect(Collectors.toCollection(LinkedHashSet::new))));
  }

  /** Method to fetch Exclusion rules for service not present in the map */
  private void fetchExclusionRulesForMissingServiceNamesIfAny(Set<String> serviceNames) {
    exclusionRulesMap.putAll(
        exclusionModsecRulesGetter.apply(
            serviceNames.stream()
                .filter(serviceName -> !exclusionRulesMap.containsKey(serviceName))
                .collect(Collectors.toCollection(LinkedHashSet::new))));
  }

  /**
   * Method to combine DLP and exclusion rules modsec blob with custom signature rules modsec blob
   */
  private String getCombinedModsecBlobs(
      String customSignatureModsecRulesBlob,
      ModsecRulesData<RateLimitingModsecRule> dlpModsecRulesData,
      ModsecRulesData<DetectionExclusionModsecRule> exclusionModsecRulesData) {
    // First combine custom signature and DLP
    String modsecRulesBlobPrefix = customSignatureModsecRulesBlob;
    // if dlp rules is present
    if (dlpModsecRulesData != null && !dlpModsecRulesData.getModsecDirectivesBlob().isEmpty()) {
      // no custom signature rules blob - add modsec directives from dlp rules modsec blob data
      // if custom signature rules blob is present, it already contains the necessary directives
      if (modsecRulesBlobPrefix.isEmpty()) {
        modsecRulesBlobPrefix = dlpModsecRulesData.getModsecDirectivesBlob();
      }
      modsecRulesBlobPrefix =
          modsecRulesBlobPrefix + NEW_LINE_DELIMITER + dlpModsecRulesData.getModsecRulesBlob();
    }

    // if exclusion rules is present
    if (exclusionModsecRulesData != null
        && !exclusionModsecRulesData.getModsecDirectivesBlob().isEmpty()) {

      if (modsecRulesBlobPrefix.isEmpty()) {
        modsecRulesBlobPrefix = exclusionModsecRulesData.getModsecDirectivesBlob();
      }
      modsecRulesBlobPrefix =
          modsecRulesBlobPrefix
              + NEW_LINE_DELIMITER
              + exclusionModsecRulesData.getModsecRulesBlob();
    }

    return modsecRulesBlobPrefix;
  }

  private Stream<IpTypeRuleInfo.IpType> getCustomSignatureIpTypes() {
    return getCustomSignatureInlineRules().stream()
        .flatMap(
            customSignatureInlineRule ->
                customSignatureInlineRule
                    .getRule()
                    .getDefinition()
                    .getClauseGroup()
                    .getClausesList()
                    .stream()
                    .filter(Clause::hasIpTypeExpression)
                    .flatMap(clause -> clause.getIpTypeExpression().getIpTypesList().stream()))
        .distinct()
        .filter(Objects::nonNull)
        .map(BlockingRulesSupplierImpl::convertIpType)
        .filter(Objects::nonNull);
  }

  private Stream<IpTypeRuleInfo.IpType> getMaliciousSourcesIpTypes() {
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
        .map(IpTypeRuleInfo::convertIpType)
        .filter(Objects::nonNull);
  }

  private Stream<IpTypeRuleInfo.IpType> getDlpRulesIpTypes(Set<String> serviceNames) {
    fetchDlpRulesForMissingServiceNamesIfAny(serviceNames);
    // To avoid using entire message for identifying distinct rules
    // to subsequently avoid extracting ip-types from same rule multiple times
    Set<String> ruleIds = ConcurrentHashMap.newKeySet();
    return serviceNames.stream()
        .flatMap(
            serviceName ->
                dlpRulesMap.getOrDefault(serviceName, new ModsecRulesData<>()).getRules().stream())
        .filter(rule -> ruleIds.add(rule.getId()))
        .flatMap(rule -> extractDlpIpLocationTypes(rule.getData().getCondition()))
        .distinct()
        .filter(Objects::nonNull)
        .map(IpTypeRuleInfo::convertIpType)
        .filter(Objects::nonNull);
  }

  private Stream<IpTypeRuleInfo.IpType> getExclusionRulesIpTypes(Set<String> serviceNames) {
    fetchExclusionRulesForMissingServiceNamesIfAny(serviceNames);
    // To avoid using entire message for identifying distinct rules
    // to subsequently avoid extracting ip-types from same rule multiple times
    Set<String> ruleIds = ConcurrentHashMap.newKeySet();
    return serviceNames.stream()
        .flatMap(
            serviceName ->
                exclusionRulesMap
                    .getOrDefault(serviceName, new ModsecRulesData<>())
                    .getRules()
                    .stream())
        .filter(rule -> ruleIds.add(rule.getRule().getId()))
        .flatMap(rule -> rule.getRule().getRuleInfo().getConditionsList().stream())
        .filter(DetectionExclusionCondition::hasIpLocationTypeCondition)
        .flatMap(
            detectionExclusionCondition ->
                detectionExclusionCondition
                    .getIpLocationTypeCondition()
                    .getIpLocationTypesList()
                    .stream())
        .distinct()
        .filter(Objects::nonNull)
        .map(BlockingRulesSupplierImpl::convertIpType)
        .filter(Objects::nonNull);
  }

  private List<String> getCustomSignatureRulesCountryIsoCodes() {
    return getCustomSignatureInlineRules().stream()
        .flatMap(
            customSignatureInlineRule ->
                customSignatureInlineRule
                    .getRule()
                    .getDefinition()
                    .getClauseGroup()
                    .getClausesList()
                    .stream()
                    .filter(Clause::hasRegionExpression)
                    .flatMap(
                        clause ->
                            clause.getRegionExpression().getRegionIdentifiersList().stream()
                                .map(RegionExpression.Region::getCountryIsoCode)))
        .distinct()
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<String> getDlpRulesCountryIsoCodes(Set<String> serviceNames) {
    // no dlp rules would be fetched if service-name is not provided.
    // assumption: agents supporting DLP rules will always send service-name.
    if (serviceNames.isEmpty()) {
      return Collections.emptyList();
    }

    fetchDlpRulesForMissingServiceNamesIfAny(serviceNames);

    // To avoid using entire message for identifying distinct rules
    // to subsequently avoid extracting ip-types from same rule multiple times
    Set<String> ruleIds = ConcurrentHashMap.newKeySet();
    return serviceNames.stream()
        .flatMap(
            serviceName ->
                dlpRulesMap.getOrDefault(serviceName, new ModsecRulesData<>()).getRules().stream())
        .filter(rule -> ruleIds.add(rule.getId()))
        .flatMap(rule -> extractDlpCountryIsoCodes(rule.getData().getCondition()))
        .distinct()
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
  }

  private static Stream<String> extractDlpCountryIsoCodes(Condition condition) {
    return condition.hasLeafCondition()
        ? extractDlpCountryIsoCodes(condition.getLeafCondition())
        : condition.getCompositeCondition().getChildrenList().stream()
            .flatMap(BlockingRulesSupplierImpl::extractDlpCountryIsoCodes);
  }

  private static Stream<String> extractDlpCountryIsoCodes(LeafCondition condition) {
    return condition.getRegionCondition().getRegionIdentifiersList().stream()
        .map(RegionCondition.Region::getCountryIsoCode);
  }

  private List<String> getExclusionRulesCountryIsoCodes(Set<String> serviceNames) {
    // no exclusion rules would be fetched if service-name is not provided.
    if (serviceNames.isEmpty()) {
      return Collections.emptyList();
    }

    fetchExclusionRulesForMissingServiceNamesIfAny(serviceNames);

    // To avoid using entire message for identifying distinct rules
    // to subsequently avoid extracting region-iso from same rule multiple times
    Set<String> ruleIds = ConcurrentHashMap.newKeySet();
    return serviceNames.stream()
        .flatMap(
            serviceName ->
                exclusionRulesMap
                    .getOrDefault(serviceName, new ModsecRulesData<>())
                    .getRules()
                    .stream())
        .filter(rule -> ruleIds.add(rule.getRule().getId()))
        .flatMap(rule -> rule.getRule().getRuleInfo().getConditionsList().stream())
        .filter(DetectionExclusionCondition::hasRegionCondition)
        .flatMap(
            detectionExclusionCondition ->
                detectionExclusionCondition.getRegionCondition().getRegionsList().stream())
        .map(Region::getCountryIsoCode)
        .distinct()
        .collect(Collectors.toUnmodifiableList());
  }

  private static <T> List<ImmutablePair<T, String>> convertIpTypeIpMappings(
      Function<IpTypeRuleInfo, ImmutablePair<T, String>> ruleConverter,
      Map<IpTypeRuleInfo.IpType, IpTypeRuleInfo> ipTypesInfoMap,
      Stream<IpTypeRuleInfo.IpType> ipTypes) {
    return ipTypes
        .filter(Objects::nonNull)
        .map(ipTypesInfoMap::get)
        .filter(Objects::nonNull)
        .map(ruleConverter)
        .collect(Collectors.toUnmodifiableList());
  }

  private static Stream<IpLocationType> extractDlpIpLocationTypes(Condition condition) {
    return condition.hasLeafCondition()
        ? extractDlpIpLocationTypes(condition.getLeafCondition())
        : condition.getCompositeCondition().getChildrenList().stream()
            .flatMap(BlockingRulesSupplierImpl::extractDlpIpLocationTypes);
  }

  private static Stream<IpLocationType> extractDlpIpLocationTypes(LeafCondition condition) {
    return condition.getIpLocationTypeCondition().getIpLocationTypesList().stream();
  }

  @Nullable
  private static IpTypeRuleInfo.IpType convertIpType(IpType ipType) {
    switch (ipType) {
      case IP_TYPE_BOT:
        return IpTypeRuleInfo.IpType.BOT;
      case IP_TYPE_ANONYMOUS_VPN:
        return IpTypeRuleInfo.IpType.ANONYMOUS_VPN;
      case IP_TYPE_HOSTING_PROVIDER:
        return IpTypeRuleInfo.IpType.HOSTING_PROVIDER;
      case IP_TYPE_PUBLIC_PROXY:
        return IpTypeRuleInfo.IpType.PUBLIC_PROXY;
      case IP_TYPE_TOR_EXIT_NODE:
        return IpTypeRuleInfo.IpType.TOR_EXIT_NODE;
      default:
        return null;
    }
  }

  @Nullable
  private static IpTypeRuleInfo.IpType convertIpType(
      ai.traceable.detection.exclusion.config.service.v1.IpLocationType ipLocationType) {
    switch (ipLocationType) {
      case IP_LOCATION_TYPE_BOT:
        return IpTypeRuleInfo.IpType.BOT;
      case IP_LOCATION_TYPE_ANONYMOUS_VPN:
        return IpTypeRuleInfo.IpType.ANONYMOUS_VPN;
      case IP_LOCATION_TYPE_HOSTING_PROVIDER:
        return IpTypeRuleInfo.IpType.HOSTING_PROVIDER;
      case IP_LOCATION_TYPE_PUBLIC_PROXY:
        return IpTypeRuleInfo.IpType.PUBLIC_PROXY;
      case IP_LOCATION_TYPE_TOR_EXIT_NODE:
        return IpTypeRuleInfo.IpType.TOR_EXIT_NODE;
      default:
        return null;
    }
  }
}
