package ai.traceable.external.data.classification.config.service;

import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.FullValueRegexDataType;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.PredicateSupportLevel;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
class PlatformDataTypeManager {
  private static final String DB_STATEMENT_DATA_TYPE_ID = "db_query_attributes";

  private final DataClassificationRulesDao dataClassificationRulesDao;
  private final DataClassificationRulesTranslator rulesTranslator;
  private final FeatureCachingClient featureCachingClient;
  private final ExternalDataClassificationConfig dataClassificationConfig;

  List<DataType> getDataTypes(
      RequestContext requestContext,
      Optional<String> environmentName,
      PredicateSupportLevel predicateSupportLevel) {
    return this.rulesTranslator.translateDataTypes(
        this.dataClassificationRulesDao.getResolvedDataTypesInEvaluationOrder(requestContext),
        environmentName,
        predicateSupportLevel);
  }

  List<DataType> getDefaultDataTypes(RequestContext requestContext) {
    if (featureCachingClient.isRaspInspectionEnabled(requestContext)) {
      return this.dataClassificationConfig.getDefaultExternalDataTypes().stream()
          .map(
              dataType -> {
                if (DB_STATEMENT_DATA_TYPE_ID.equals(dataType.getDataTypeId())) {
                  return dataType.toBuilder().clearTransformation().build();
                }
                return dataType;
              })
          .collect(toUnmodifiableList());
    }
    return this.dataClassificationConfig.getDefaultExternalDataTypes();
  }

  List<FullValueRegexDataType> getFullValueRegexDataTypes(
      RequestContext requestContext, Optional<String> environmentName) {
    return this.rulesTranslator.translateFullValueRegexDataTypes(
        this.dataClassificationRulesDao.getResolvedDataTypesInEvaluationOrder(requestContext),
        environmentName);
  }
}
