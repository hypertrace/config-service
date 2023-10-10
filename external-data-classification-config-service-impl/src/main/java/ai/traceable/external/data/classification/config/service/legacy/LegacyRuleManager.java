package ai.traceable.external.data.classification.config.service.legacy;

import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.external.data.classification.config.service.legacy.RedactionRulesDao.RedactionRuleFilter;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
public class LegacyRuleManager {

  // this should be in sync with id in RedactionRulesDao in data classification config service impl
  // and with id in PiiFilterConfigServiceImpl in sensitive data config service
  private static final String LEGACY_SENSITIVE_HEADERS_DATA_SET_ID =
      "legacy-dataset-sensitive-headers-id";
  private static final String LEGACY_DATASET_ID_PREFIX = "legacy-";
  // this should be in sync with id in RedactionRulesDao in data classification config service impl
  // and with id in PiiFilterConfigServiceImpl in sensitive data config service
  private static final String LEGACY_REDACT_DATA_SET_ID = "legacy-dataset-redacted-id";
  // this should be in sync with id in RedactionRulesDao in data classification config service impl
  // and with id in PiiFilterConfigServiceImpl in sensitive data config service
  private static final String LEGACY_OBFUSCATE_DATA_SET_ID = "legacy-dataset-obfuscated-id";

  private final RedactionRulesDao redactionRulesDao;
  private final RedactionRulesTranslator redactionRulesTranslator;
  private final InsightsServiceCoordinator insightsServiceCoordinator;

  public List<DataType> getDataTypesFromLegacyRedactionRules(
      RequestContext requestContext, Collection<DataSet> enabledDataSets) {
    List<RedactionRule> redactionRules =
        this.redactionRulesDao.getEnabledRedactionRules(
            requestContext, this.getFilterForEnabledRedactionStrategies(enabledDataSets));

    return this.redactionRulesTranslator.translateRedactionRules(redactionRules);
  }

  public List<DataType> getDataTypesFromLegacySensitiveHeaders(
      RequestContext requestContext, Collection<DataSet> enabledDataSets) {
    boolean legacySensitiveHeadersEnabled =
        enabledDataSets.stream()
            .anyMatch(dataSet -> dataSet.getId().equals(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID));

    if (!legacySensitiveHeadersEnabled) {
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

  public boolean isLegacyDataSet(DataSet dataSet) {
    return dataSet.getId().startsWith(LEGACY_DATASET_ID_PREFIX);
  }

  private RedactionRuleFilter getFilterForEnabledRedactionStrategies(
      Collection<DataSet> enabledDataSets) {
    Map<String, RedactionStrategy> legacyDataSetIdMap =
        Map.of(
            LEGACY_REDACT_DATA_SET_ID,
            REDACTION_STRATEGY_REDACT,
            LEGACY_OBFUSCATE_DATA_SET_ID,
            REDACTION_STRATEGY_HASH);

    Set<RedactionStrategy> enabledStrategies =
        enabledDataSets.stream()
            .map(DataSet::getId)
            .map(legacyDataSetIdMap::get)
            .filter(Objects::nonNull)
            .collect(ImmutableSet.toImmutableSet());

    return new RedactionRuleFilter(enabledStrategies);
  }
}
