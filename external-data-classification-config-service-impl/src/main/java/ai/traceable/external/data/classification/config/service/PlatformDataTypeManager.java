package ai.traceable.external.data.classification.config.service;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.external.data.classification.config.service.legacy.LegacyRuleManager;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.PredicateSupportLevel;
import com.google.common.base.Equivalence;
import com.google.common.base.Equivalence.Wrapper;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
class PlatformDataTypeManager {
  private static final String DB_STATEMENT_DATA_TYPE_ID = "db_query_attributes";
  private static final Equivalence<ResolvedPlatformDataType> dataTypeEquivalence =
      Equivalence.equals().onResultOf(ResolvedPlatformDataType::getDataType);
  private final DataClassificationRulesDao dataClassificationRulesDao;
  private final LegacyRuleManager legacyRuleManager;
  private final DataClassificationRulesTranslator rulesTranslator;
  private final FeatureCachingClient featureCachingClient;
  private final ExternalDataClassificationConfig dataClassificationConfig;

  List<DataType> getDataTypes(
      RequestContext requestContext,
      Collection<DataSet> enabledDataSets,
      Optional<String> environmentName,
      PredicateSupportLevel predicateSupportLevel) {
    Map<String, ai.traceable.data.classification.config.service.v1.DataType> dataTypeMap =
        this.getDataTypeMap(requestContext);
    return enabledDataSets.stream()
        .filter(Predicate.not(this.legacyRuleManager::isLegacyDataSet))
        .sorted(Comparator.comparingInt(o -> comparatorUtility(o.getInfo())))
        .flatMap(dataSet -> this.buildResolvedTypes(dataSet, dataTypeMap))
        .map(dataTypeEquivalence::wrap)
        .distinct()
        .map(Wrapper::get)
        .collect(
            collectingAndThen(
                toUnmodifiableList(),
                rules ->
                    this.rulesTranslator.translateDataTypes(
                        rules, environmentName, predicateSupportLevel)));
  }

  private Map<String, ai.traceable.data.classification.config.service.v1.DataType> getDataTypeMap(
      RequestContext requestContext) {
    return this.dataClassificationRulesDao.getAllDataTypes(requestContext).stream()
        .collect(
            toUnmodifiableMap(
                ai.traceable.data.classification.config.service.v1.DataType::getId, identity()));
  }

  private Stream<ResolvedPlatformDataType> buildResolvedTypes(
      DataSet dataSet,
      Map<String, ai.traceable.data.classification.config.service.v1.DataType> dataTypeMap) {
    return dataSet.getInfo().getDataTypeIdsList().stream()
        .map(dataTypeMap::get)
        .filter(Objects::nonNull)
        .map(
            dataType ->
                new ResolvedPlatformDataType(dataType, dataSet.getInfo().getDataSuppression()));
  }

  List<DataSet> getEnabledDataSets(RequestContext requestContext) {
    return this.dataClassificationRulesDao.getAllDataSets(requestContext).stream()
        .filter(dataSet -> dataSet.getInfo().getEnabled())
        .collect(toUnmodifiableList());
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

  private int comparatorUtility(DataSetInfo dataSetInfo) {
    switch (dataSetInfo.getDataSuppression()) {
      case DATA_SUPPRESSION_REDACT:
        return 0;
      case DATA_SUPPRESSION_OBFUSCATE:
        return 1;
      case DATA_SUPPRESSION_RAW:
        return 2;
      case UNRECOGNIZED:
      case DATA_SUPPRESSION_UNSPECIFIED:
      default:
        return Integer.MAX_VALUE;
    }
  }
}
