package ai.traceable.external.data.classification.config.service;

import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class ExternalDataClassificationConfigServiceImpl
    extends ExternalDataClassificationServiceImplBase {
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
  private static final String LEGACY_DATASET_ID_PREFIX = "legacy-";

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
      List<RedactionRule> redactionRules =
          this.redactionRulesDao.getAllRedactionRules(requestContext);
      Map<String, DataType> dataTypesToIdMap =
          this.dataClassificationRulesDao.getAllDataTypes(requestContext).stream()
              .collect(Collectors.toUnmodifiableMap(DataType::getId, Function.identity()));
      List<DataType> dataTypes = new ArrayList<>();
      List<DataSet> dataSets =
          this.dataClassificationRulesDao.getAllDataSets(requestContext).stream()
              .filter(
                  dataSet ->
                      dataSet.getInfo().getEnabled()
                          && !dataSet.getId().startsWith(LEGACY_DATASET_ID_PREFIX)
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
      Map<String, DataSuppression> dataTypesToDataSuppressionMap = new HashMap<>();
      for (DataSet dataSet : dataSets) {
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
      List<ai.traceable.external.data.classification.config.service.v1.DataType> externalDataTypes =
          new ArrayList<>();
      externalDataTypes.addAll(redactionRulesTranslator.translateRedactionRules(redactionRules));
      externalDataTypes.addAll(
          dataClassificationRulesTranslator.translateDataTypes(
              dataTypes,
              dataTypesToDataSuppressionMap,
              Optional.of(request.getEnvironmentFilter().getEnvironmentName())
                  .filter(envName -> !envName.isBlank())));

      // sensitive headers
      RedactionStrategy redactionStrategy =
          redactionRulesDao.getParamTypeHeaderRedactionStrategy(requestContext);
      if (redactionStrategy.equals(REDACTION_STRATEGY_HASH)
          || redactionStrategy.equals(REDACTION_STRATEGY_REDACT)) {
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
}
