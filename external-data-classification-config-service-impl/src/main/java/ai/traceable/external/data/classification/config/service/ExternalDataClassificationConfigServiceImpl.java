package ai.traceable.external.data.classification.config.service;

import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_RAW;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;
import static java.util.function.Function.identity;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.ExternalDataClassificationServiceGrpc.ExternalDataClassificationServiceImplBase;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class ExternalDataClassificationConfigServiceImpl
    extends ExternalDataClassificationServiceImplBase {

  private static final String LEGACY_DATASET_ID_PREFIX = "legacy-";
  // this should be in sync with id in RedactionRulesDao in data classification config service impl
  // and with id in PiiFilterConfigServiceImpl in sensitive data config service
  private static final String LEGACY_REDACT_DATA_SET_ID = "legacy-dataset-redacted-id";
  // this should be in sync with id in RedactionRulesDao in data classification config service impl
  // and with id in PiiFilterConfigServiceImpl in sensitive data config service
  private static final String LEGACY_OBFUSCATE_DATA_SET_ID = "legacy-dataset-obfuscated-id";
  // this should be in sync with id in RedactionRulesDao in data classification config service impl
  // and with id in PiiFilterConfigServiceImpl in sensitive data config service
  private static final String LEGACY_SENSITIVE_HEADERS_DATA_SET_ID =
      "legacy-dataset-sensitive-headers-id";

  private final ExternalDataClassificationConfig externalDataClassificationConfig;
  private final ExternalDataClassificationConfigRequestValidator
      externalDataClassificationConfigRequestValidator;
  private final RedactionRulesDao redactionRulesDao;
  private final DataClassificationRulesDao dataClassificationRulesDao;
  private final RedactionRulesTranslator redactionRulesTranslator;
  private final DataClassificationRulesTranslator dataClassificationRulesTranslator;
  private final ExternalDataClassificationRuleResponseBuilder responseBuilder;
  private final InsightsServiceCoordinator insightsServiceCoordinator;
  private final FeatureCachingClient featureCachingClient;

  @Inject
  public ExternalDataClassificationConfigServiceImpl(
      ExternalDataClassificationConfig externalDataClassificationConfig,
      ExternalDataClassificationConfigRequestValidator
          externalDataClassificationConfigRequestValidator,
      RedactionRulesDao redactionRulesDao,
      DataClassificationRulesDao dataClassificationRulesDao,
      RedactionRulesTranslator redactionRulesTranslator,
      DataClassificationRulesTranslator dataClassificationRulesTranslator,
      ExternalDataClassificationRuleResponseBuilder responseBuilder,
      InsightsServiceCoordinator insightsServiceCoordinator,
      FeatureCachingClient featureCachingClient) {
    this.externalDataClassificationConfig = externalDataClassificationConfig;
    this.externalDataClassificationConfigRequestValidator =
        externalDataClassificationConfigRequestValidator;
    this.redactionRulesDao = redactionRulesDao;
    this.dataClassificationRulesDao = dataClassificationRulesDao;
    this.redactionRulesTranslator = redactionRulesTranslator;
    this.dataClassificationRulesTranslator = dataClassificationRulesTranslator;
    this.responseBuilder = responseBuilder;
    this.insightsServiceCoordinator = insightsServiceCoordinator;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  public void getDataClassificationConfig(
      GetDataClassificationConfigRequest request,
      StreamObserver<GetDataClassificationConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.externalDataClassificationConfigRequestValidator.validateOrThrow(
          requestContext, request);
      if (!this.featureCachingClient.isDataClassificationRp2Enabled(requestContext)) {
        responseObserver.onNext(this.responseBuilder.buildDisabledResponse());
        responseObserver.onCompleted();
        return;
      }
      Optional<String> envOptional =
          Optional.of(request.getEnvironmentFilter().getEnvironmentName())
              .filter(envName -> !envName.isBlank());
      Optional<DataSuppression> dataSuppressionOverrideOptional =
          envOptional.flatMap(
              env ->
                  this.dataClassificationRulesDao.getDataSuppressionOverride(requestContext, env));
      Map<String, DataType> dataTypesToIdMap =
          this.dataClassificationRulesDao.getAllDataTypes(requestContext).stream()
              .collect(Collectors.toUnmodifiableMap(DataType::getId, identity()));
      List<DataSet> enabledDataSets =
          this.dataClassificationRulesDao.getAllDataSets(requestContext).stream()
              .filter(dataSet -> dataSet.getInfo().getEnabled())
              .collect(Collectors.toUnmodifiableList());
      List<DataSet> dataSetsToTranslate =
          enabledDataSets.stream()
              .map(
                  dataSet ->
                      updateDataSetIfDataSuppressionOverridden(
                          dataSet, dataSuppressionOverrideOptional))
              .filter(
                  dataSet ->
                      !dataSet.getId().startsWith(LEGACY_DATASET_ID_PREFIX)
                          && (dataSet
                                  .getInfo()
                                  .getDataSuppression()
                                  .equals(DataSuppression.DATA_SUPPRESSION_REDACT)
                              || dataSet
                                  .getInfo()
                                  .getDataSuppression()
                                  .equals(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)))
              .sorted(Comparator.comparingInt(o -> comparatorUtility(o.getInfo())))
              .collect(Collectors.toUnmodifiableList());
      List<DataType> dataTypes = new ArrayList<>();
      Map<String, DataSuppression> dataTypesToDataSuppressionMap = new HashMap<>();
      for (DataSet dataSet : dataSetsToTranslate) {
        DataSuppression dataSuppression = dataSet.getInfo().getDataSuppression();
        for (String dataTypeId : dataSet.getInfo().getDataTypeIdsList()) {
          if (dataTypesToIdMap.containsKey(dataTypeId)
              && !dataTypesToDataSuppressionMap.containsKey(dataTypeId)) {
            dataTypesToDataSuppressionMap.put(dataTypeId, dataSuppression);
            dataTypes.add(dataTypesToIdMap.get(dataTypeId));
          }
        }
      }
      dataTypes = Collections.unmodifiableList(dataTypes);
      Map<String, DataSet> enabledDataSetMap =
          enabledDataSets.stream()
              .collect(Collectors.toUnmodifiableMap(DataSet::getId, identity()));
      List<ai.traceable.external.data.classification.config.service.v1.DataType> externalDataTypes =
          new ArrayList<>();
      List<RedactionRule> redactionRules =
          this.redactionRulesDao.getEnabledRedactionRules(requestContext);
      externalDataTypes.addAll(
          redactionRulesTranslator.translateRedactionRules(
              getFilteredRedactionRules(
                  redactionRules, enabledDataSetMap, dataSuppressionOverrideOptional)));
      externalDataTypes.addAll(
          dataClassificationRulesTranslator.translateDataTypes(
              dataTypes,
              dataTypesToDataSuppressionMap,
              envOptional,
              request.getPredicateSupportLevel()));

      // sensitive headers
      RedactionStrategy redactionStrategy =
          dataSuppressionOverrideOptional
              .flatMap(this::translateDataSuppression)
              .orElseGet(
                  () -> redactionRulesDao.getParamTypeHeaderRedactionStrategy(requestContext));
      if (enabledDataSetMap.containsKey(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID)
          && (redactionStrategy.equals(REDACTION_STRATEGY_HASH)
              || redactionStrategy.equals(REDACTION_STRATEGY_REDACT))) {
        List<Parameter> sensitiveHeaderParameters =
            insightsServiceCoordinator.getSensitiveHeaderParameters(requestContext);
        redactionRulesTranslator
            .translateDataTypeForSensitiveHeaders(sensitiveHeaderParameters, redactionStrategy)
            .ifPresent(externalDataTypes::add);
      }

      // Default external-only types
      externalDataTypes.addAll(this.externalDataClassificationConfig.getDefaultExternalDataTypes());
      responseObserver.onNext(
          this.responseBuilder.buildEnabledResponse(
              request,
              externalDataTypes,
              this.externalDataClassificationConfig.getDefaultDataParsingRules()));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get external data classification rules", e);
      responseObserver.onError(e);
    }
  }

  private DataSet updateDataSetIfDataSuppressionOverridden(
      DataSet dataSet, Optional<DataSuppression> dataSuppressionOverrideOptional) {
    return dataSuppressionOverrideOptional
        .map(
            dataSuppression ->
                dataSet.toBuilder()
                    .setInfo(dataSet.getInfo().toBuilder().setDataSuppression(dataSuppression))
                    .build())
        .orElse(dataSet);
  }

  private List<RedactionRule> getFilteredRedactionRules(
      List<RedactionRule> redactionRules,
      Map<String, DataSet> enabledDataSetMap,
      Optional<DataSuppression> dataSuppressionOverrideOptional) {
    Set<RedactionStrategy> allowedRedactionStrategy = new HashSet<>();
    Optional.ofNullable(enabledDataSetMap.get(LEGACY_REDACT_DATA_SET_ID))
        .ifPresent(ds -> allowedRedactionStrategy.add(REDACTION_STRATEGY_REDACT));
    Optional.ofNullable(enabledDataSetMap.get(LEGACY_OBFUSCATE_DATA_SET_ID))
        .ifPresent(ds -> allowedRedactionStrategy.add(REDACTION_STRATEGY_HASH));
    Optional<RedactionStrategy> redactionStrategyOverrideOptional =
        dataSuppressionOverrideOptional.flatMap(this::translateDataSuppression);
    return redactionRules.stream()
        .filter(
            // filter redactions rules as follows :
            // 1. all session identifier redaction rules are allowed
            // 2. redactions rules only in enabled legacy data sets are allowed
            redactionRule ->
                redactionRule.getSessionIdentifier()
                    || allowedRedactionStrategy.contains(redactionRule.getRedactionStrategy()))
        .map(
            redactionRule ->
                updateRedactionRuleIfRedactionStrategyOverridden(
                    redactionRule, redactionStrategyOverrideOptional))
        .filter(
            // post update filter redactions rules as follows :
            // 1. all session identifier redaction rules are still allowed
            // 2. redactions rules only with redaction strategy as REDACT or HASH are allowed
            redactionRule ->
                redactionRule.getSessionIdentifier()
                    || redactionRule.getRedactionStrategy().equals(REDACTION_STRATEGY_HASH)
                    || redactionRule.getRedactionStrategy().equals(REDACTION_STRATEGY_REDACT))
        .collect(Collectors.toUnmodifiableList());
  }

  private RedactionRule updateRedactionRuleIfRedactionStrategyOverridden(
      RedactionRule redactionRule, Optional<RedactionStrategy> redactionStrategyOverrideOptional) {
    return redactionStrategyOverrideOptional
        .map(
            redactionStrategy ->
                redactionRule.toBuilder().setRedactionStrategy(redactionStrategy).build())
        .orElse(redactionRule);
  }

  private static int comparatorUtility(DataSetInfo dataSetInfo) {
    switch (dataSetInfo.getDataSuppression()) {
      case DATA_SUPPRESSION_REDACT:
        return 0;
      case DATA_SUPPRESSION_OBFUSCATE:
        return 1;
      case DATA_SUPPRESSION_RAW:
        return 2;
      default:
        return Integer.MAX_VALUE;
    }
  }

  private Optional<RedactionStrategy> translateDataSuppression(DataSuppression dataSuppression) {
    switch (dataSuppression) {
      case DATA_SUPPRESSION_REDACT:
        return Optional.of(REDACTION_STRATEGY_REDACT);
      case DATA_SUPPRESSION_OBFUSCATE:
        return Optional.of(REDACTION_STRATEGY_HASH);
      case DATA_SUPPRESSION_RAW:
        return Optional.of(REDACTION_STRATEGY_RAW);
      default:
        log.warn(
            "Unable to translate data suppression {} to appropriate redaction strategy",
            dataSuppression);
        return Optional.empty();
    }
  }
}
