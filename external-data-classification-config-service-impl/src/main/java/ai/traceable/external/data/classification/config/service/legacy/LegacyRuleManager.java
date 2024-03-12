package ai.traceable.external.data.classification.config.service.legacy;

import static ai.traceable.config.utils.ExternalAgentAttributeConfigServiceConstants.JSON_PATH_EXTRACTION_FIX_MIN_TPA_VERSION;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.external.data.classification.config.service.legacy.RedactionRulesDao.RedactionRuleFilter;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
public class LegacyRuleManager {
  // this should be in sync with id in PiiFilterConfigServiceImpl in sensitive data config service
  // impl and id in RedactionRulesDao in data classification config service impl
  static final String LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID =
      "legacy-datatype-sensitive-headers-id";

  private final RedactionRulesDao redactionRulesDao;
  private final FeatureCachingClient featureCachingClient;
  private final SemanticVersioningComparator semanticVersioningComparator;
  private final RedactionRulesTranslator redactionRulesTranslator;
  private final InsightsServiceCoordinator insightsServiceCoordinator;

  public List<DataType> getDataTypesFromLegacyRedactionRules(
      RequestContext requestContext,
      Set<String> enabledLegacyDataTypeIds,
      GetDataClassificationConfigRequest.AgentCapabilities agentCapabilities) {
    List<RedactionRule> redactionRules =
        this.redactionRulesDao.getRulesMatchFilter(
            requestContext,
            new RedactionRuleFilter(
                enabledLegacyDataTypeIds,
                !isSessionIdentificationV2SupportedByAgent(requestContext, agentCapabilities)));

    return this.redactionRulesTranslator.translateRedactionRules(redactionRules);
  }

  public List<DataType> getDataTypesFromLegacySensitiveHeaders(
      RequestContext requestContext, Set<String> enabledLegacyDataTypeIds) {
    if (!enabledLegacyDataTypeIds.contains(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID)) {
      return Collections.emptyList();
    }

    RedactionStrategy redactionStrategy =
        this.redactionRulesDao.getParamTypeHeaderRedactionStrategy(requestContext);
    List<Parameter> sensitiveHeaderParameters =
        insightsServiceCoordinator.getSensitiveHeaderParameters(requestContext);
    return redactionRulesTranslator
        .translateDataTypeForSensitiveHeaders(sensitiveHeaderParameters, redactionStrategy)
        .map(List::of)
        .orElseGet(Collections::emptyList);
  }

  public boolean isSessionIdentificationV2SupportedByAgent(
      RequestContext requestContext,
      GetDataClassificationConfigRequest.AgentCapabilities agentCapabilities) {
    return featureCachingClient.isSessionIdentificationV2EnabledForTenant(requestContext)
        && agentCapabilities.getComponentsList().stream()
            .map(GetDataClassificationConfigRequest.Component::getTraceablePlatformAgentVersion)
            .allMatch(
                tpaVersion ->
                    semanticVersioningComparator.isVersionSupported(
                        tpaVersion, JSON_PATH_EXTRACTION_FIX_MIN_TPA_VERSION));
  }
}
