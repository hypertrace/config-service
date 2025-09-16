package ai.traceable.blocking.config.service.common.rules;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.RegionRule;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface BlockingRulesSupplier {

  RequestContext getRequestContext();

  Optional<String> getEnvironmentId();

  /** Returns the custom signature rules modsec blob for the specified version */
  String getCustomSignatureModsecBlob(CustomModsecRuleVersion version);

  /**
   * Returns the list of modsec blobs (combination of custom signature and DLP rules) keyed by
   * service name
   */
  Map<String, String> getCustomModsecBlobs(
      CustomModsecRuleVersion version, Set<String> serviceNames);

  /** Returns list of Region to Ip-range mappings in the form of objects defined by the converter */
  <T> List<T> getRegionIpMappings(
      Function<DetailedRegion, T> ruleConverter, Set<String> serviceNames);

  List<RegionRule> getRegionRules();

  /**
   * Returns list of Ip-Type to Ip-range mappings in the form of objects defined by the converter
   * keyed by service name
   */
  <T> List<ImmutablePair<T, String>> getIpTypeIpMappings(
      Function<IpTypeRuleInfo, ImmutablePair<T, String>> ruleConverter, Set<String> serviceNames);

  /** Returns the list of DLP rules keyed by service name */
  Map<String, List<RateLimitingModsecRule>> getDlpRules(Set<String> serviceNames);

  /** Returns the list of Exclusion rules keyed by service name */
  Map<String, List<DetectionExclusionModsecRule>> getExclusionRules(Set<String> serviceNames);

  /** Returns the list of custom signature inline rule */
  List<CustomSignatureInlineRule> getCustomSignatureInlineRules();

  /** Returns the list of custom signature inline rule keyed by service name */
  Map<String, List<CustomSignatureInlineRule>> getCustomSignatureInlineRules(
      Set<String> serviceNames);
}
