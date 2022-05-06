package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule;
import ai.traceable.external.data.classification.config.service.v1.ExternalDataClassificationServiceGrpc.ExternalDataClassificationServiceImplBase;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import com.google.inject.Inject;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class ExternalDataClassificationConfigServiceImpl
    extends ExternalDataClassificationServiceImplBase {
  private final ExternalDataClassificationConfigRequestValidator
      externalDataClassificationConfigRequestValidator;
  private final RedactionRulesDao redactionRulesDao;
  private final DataClassificationRulesDao dataClassificationRulesDao;
  private final RedactionRulesTranslator redactionRulesTranslator;
  private final DataClassificationRulesTranslator dataClassificationRulesTranslator;
  private final ExternalDataClassificationRuleResponseBuilder responseBuilder;
  private static final String DATA_PARSING_RULES = "data.parsing.rules";
  private static final String EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE =
      "external.data.classification.config.service";
  private final List<DataParsingRule> dataParsingRulesList;

  @Inject
  public ExternalDataClassificationConfigServiceImpl(
      Config config,
      ExternalDataClassificationConfigRequestValidator
          externalDataClassificationConfigRequestValidator,
      RedactionRulesDao redactionRulesDao,
      DataClassificationRulesDao dataClassificationRulesDao,
      RedactionRulesTranslator redactionRulesTranslator,
      DataClassificationRulesTranslator dataClassificationRulesTranslator,
      ExternalDataClassificationRuleResponseBuilder responseBuilder) {
    this.externalDataClassificationConfigRequestValidator =
        externalDataClassificationConfigRequestValidator;
    this.redactionRulesDao = redactionRulesDao;
    this.dataClassificationRulesDao = dataClassificationRulesDao;
    this.redactionRulesTranslator = redactionRulesTranslator;
    this.dataClassificationRulesTranslator = dataClassificationRulesTranslator;
    this.responseBuilder = responseBuilder;
    List<? extends ConfigObject> dataParsingRulesObjectList = null;
    Config externalDataClassificationConfig =
        config.getConfig(EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE);
    dataParsingRulesObjectList = externalDataClassificationConfig.getObjectList(DATA_PARSING_RULES);
    if (dataParsingRulesObjectList != null) {
      this.dataParsingRulesList = this.buildDataParsingRulesList(dataParsingRulesObjectList);
    } else {
      this.dataParsingRulesList = Collections.emptyList();
    }
  }

  @Override
  public void getDataClassificationConfig(
      GetDataClassificationConfigRequest request,
      StreamObserver<GetDataClassificationConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.externalDataClassificationConfigRequestValidator.validateOrThrow(
          requestContext, request);
      List<RedactionRule> redactionRules =
          this.redactionRulesDao.getAllRedactionRules(requestContext);
      // filter out redaction rules from data classification rules
      Set<String> redactionRulesIds =
          redactionRules.stream().map(RedactionRule::getId).collect(Collectors.toUnmodifiableSet());
      List<DataType> dataTypes =
          this.dataClassificationRulesDao.getAllDataTypes(requestContext).stream()
              .filter(dataType -> !redactionRulesIds.contains(dataType.getId()))
              .collect(Collectors.toUnmodifiableList());
      List<DataSet> dataSets = this.dataClassificationRulesDao.getAllDataSets(requestContext);

      dataSets.stream().sorted(
          new Comparator<>() {
            @Override
            public int compare(DataSet o1, DataSet o2) {
              return Integer.compare(
                  comparatorUtility(o1.getInfo()), comparatorUtility(o2.getInfo()));
            }
          });
      Map<String, DataType> dataTypesToIdMap =
          dataTypes.stream()
              .collect(Collectors.toUnmodifiableMap(DataType::getId, Function.identity()));
      Map<String, DataSuppression> dataTypesToDataSuppressionMap = new HashMap<>();
      for (DataSet dataSet : dataSets) {
        for (String dataTypeId : dataSet.getInfo().getDataTypeIdsList()) {
          DataSuppression dataSuppression = dataSet.getInfo().getDataSuppression();
          if (!dataTypesToDataSuppressionMap.containsKey(dataTypeId)) {
            dataTypesToDataSuppressionMap.put(dataTypeId, dataSuppression);
          }
        }
      }
      List<ai.traceable.external.data.classification.config.service.v1.DataType> externalDataTypes =
          new ArrayList<>();
      externalDataTypes.addAll(redactionRulesTranslator.translateRedactionRules(redactionRules));
      externalDataTypes.addAll(
          dataClassificationRulesTranslator.translateDataTypes(
              dataTypesToIdMap, dataTypesToDataSuppressionMap));

      responseObserver.onNext(
          this.responseBuilder.buildResponse(
              request, externalDataTypes, this.dataParsingRulesList));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get external data classification rules", e);
      responseObserver.onError(e);
    }
  }

  private List<DataParsingRule> buildDataParsingRulesList(
      List<? extends ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(ExternalDataClassificationConfigServiceImpl::buildDataParsingRuleFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private static DataParsingRule buildDataParsingRuleFromConfig(ConfigObject configObject) {
    String jsonString = configObject.render();
    DataParsingRule.Builder builder = DataParsingRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
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
