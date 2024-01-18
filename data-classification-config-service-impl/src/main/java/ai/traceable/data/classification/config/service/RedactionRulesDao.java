package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_OBFUSCATE;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_RAW;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_REDACT;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_UNSPECIFIED;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_RAW;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.sensitivedata.config.service.v1.DeleteRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RedactionRulesDao {
  static final String LEGACY_REDACT_DATA_SET_NAME = "Legacy Dataset - Redacted";
  static final String LEGACY_REDACT_DATA_SET_DESCRIPTION =
      "Legacy dataset containing redaction rules with redacted strategy";
  // this should be in sync with id in LegacyRuleManager in external data
  // classification config service impl and with id in PiiFilterConfigServiceImpl in sensitive data
  // config service impl
  static final String LEGACY_REDACT_DATA_SET_ID = "legacy-dataset-redacted-id";
  static final String LEGACY_OBFUSCATE_DATA_SET_NAME = "Legacy Dataset - Obfuscated";
  static final String LEGACY_OBFUSCATE_DATA_SET_DESCRIPTION =
      "Legacy dataset containing redaction rules with obfuscated strategy";
  // this should be in sync with id in LegacyRuleManager in external data
  // classification config service impl and with id in PiiFilterConfigServiceImpl in sensitive data
  // config service impl
  static final String LEGACY_OBFUSCATE_DATA_SET_ID = "legacy-dataset-obfuscated-id";
  static final String LEGACY_RAW_DATA_SET_NAME = "Legacy Dataset - Unsuppressed";
  static final String LEGACY_RAW_DATA_SET_DESCRIPTION =
      "Legacy dataset containing redaction rules with collect strategy";
  static final String LEGACY_RAW_DATA_SET_ID = "legacy-dataset-unsuppressed-id";

  static final String LEGACY_SENSITIVE_HEADERS_DATA_SET_NAME = "Legacy Dataset - Sensitive Headers";
  static final String LEGACY_SENSITIVE_HEADERS_DATA_SET_DESCRIPTION =
      "Legacy dataset containing redaction rules for sensitive headers";
  // this should be in sync with id in LegacyRuleManager in external data
  // classification config service impl and with id in PiiFilterConfigServiceImpl in sensitive data
  // config service impl
  static final String LEGACY_SENSITIVE_HEADERS_DATA_SET_ID = "legacy-dataset-sensitive-headers-id";
  static final String LEGACY_SENSITIVE_HEADERS_DATA_TYPE_NAME =
      "Legacy Datatype - Sensitive Headers";
  static final String LEGACY_SENSITIVE_HEADERS_DATA_TYPE_DESCRIPTION =
      "Legacy datatype for sensitive headers";
  // this should be in sync with id in PiiFilterConfigServiceImpl in sensitive data config service
  // impl and id in RedactionRulesTranslator in external data classification config service impl
  static final String LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID =
      "legacy-datatype-sensitive-headers-id";
  static final DataType LEGACY_SENSITIVE_HEADERS_DATA_TYPE =
      DataType.newBuilder()
          .setId(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID)
          .setRule(
              DataTypeRule.newBuilder()
                  .setName(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_NAME)
                  .setDescription(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_DESCRIPTION))
          .build();

  static final String LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_NAME =
      "Legacy Dataset - Automatic Secret Redaction";
  static final String LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_DESCRIPTION =
      "Legacy dataset representing automatic secret redaction redaction";
  // this should be in sync with id in PiiFilterConfigServiceImpl in sensitive data config service
  // impl
  static final String LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID =
      "legacy-dataset-automatic-secret-redaction-id";
  static final String LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_NAME =
      "Legacy Datatype - Automatic Secret Redaction";
  static final String LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_DESCRIPTION =
      "Legacy datatype for automatic secret redaction";
  // this should be in sync with id in PiiFilterConfigServiceImpl in sensitive data config service
  // impl
  static final String LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID =
      "legacy-datatype-automatic-secret-redaction-id";
  static final DataType LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE =
      DataType.newBuilder()
          .setId(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID)
          .setRule(
              DataTypeRule.newBuilder()
                  .setName(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_NAME)
                  .setDescription(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_DESCRIPTION))
          .build();

  static final Set<String> LEGACY_DATA_SET_IDS =
      Set.of(
          LEGACY_REDACT_DATA_SET_ID,
          LEGACY_OBFUSCATE_DATA_SET_ID,
          LEGACY_RAW_DATA_SET_ID,
          LEGACY_SENSITIVE_HEADERS_DATA_SET_ID,
          LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID);

  private final SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub;
  private final LegacyDataSetStore legacyDataSetStore;

  @Inject
  public RedactionRulesDao(
      SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub,
      LegacyDataSetStore legacyDataSetStore) {
    this.sensitiveDataConfigServiceBlockingStub = sensitiveDataConfigServiceBlockingStub;
    this.legacyDataSetStore = legacyDataSetStore;
  }

  public List<DataSet> getDataSetsFromRedactionRules(RequestContext requestContext) {
    Map<String, Boolean> legacyDataEnabledMap =
        this.legacyDataSetStore.getAllObjects(requestContext).stream()
            .map(ConfigObject::getData)
            .collect(
                Collectors.toUnmodifiableMap(
                    DataSet::getId, dataSet -> dataSet.getInfo().getEnabled()));
    List<RedactionRule> redactionRules =
        getEnabledRedactionRulesWithoutSessionIdentifiers(requestContext);
    List<String> ruleIdsForRedact = new ArrayList<>();
    List<String> ruleIdsForObfuscate = new ArrayList<>();
    List<String> rulesIdsForRaw = new ArrayList<>();
    redactionRules.forEach(
        rule -> {
          switch (rule.getRedactionStrategy()) {
            case REDACTION_STRATEGY_REDACT:
              ruleIdsForRedact.add(rule.getId());
              break;
            case REDACTION_STRATEGY_HASH:
              ruleIdsForObfuscate.add(rule.getId());
              break;
            default:
              rulesIdsForRaw.add(rule.getId());
          }
        });
    Optional<DataSet> dataSetForRedact =
        buildDataSet(
            ruleIdsForRedact,
            LEGACY_REDACT_DATA_SET_ID,
            LEGACY_REDACT_DATA_SET_NAME,
            LEGACY_REDACT_DATA_SET_DESCRIPTION,
            DATA_SUPPRESSION_REDACT,
            legacyDataEnabledMap);

    Optional<DataSet> dataSetForObfuscate =
        buildDataSet(
            ruleIdsForObfuscate,
            LEGACY_OBFUSCATE_DATA_SET_ID,
            LEGACY_OBFUSCATE_DATA_SET_NAME,
            LEGACY_OBFUSCATE_DATA_SET_DESCRIPTION,
            DATA_SUPPRESSION_OBFUSCATE,
            legacyDataEnabledMap);

    Optional<DataSet> dataSetForRaw =
        buildDataSet(
            rulesIdsForRaw,
            LEGACY_RAW_DATA_SET_ID,
            LEGACY_RAW_DATA_SET_NAME,
            LEGACY_RAW_DATA_SET_DESCRIPTION,
            DATA_SUPPRESSION_RAW,
            legacyDataEnabledMap);

    return Stream.of(
            getAutomaticSecretRedactionDataSet(requestContext, legacyDataEnabledMap),
            getSensitiveHeadersDataSet(requestContext, legacyDataEnabledMap),
            dataSetForRedact,
            dataSetForObfuscate,
            dataSetForRaw)
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<DataSet> getDataSetWithIdFromRedactionRules(
      RequestContext requestContext, String id) {
    return getDataSetsFromRedactionRules(requestContext).stream()
        .filter(dataSet -> id.equals(dataSet.getId()))
        .findAny();
  }

  public List<DataType> getAllDataTypesFromRedactionRules(RequestContext requestContext) {
    List<DataType> dataTypes = new ArrayList<>();
    getAutomaticSecretRedactionDataType(requestContext).ifPresent(dataTypes::add);
    getSensitiveHeadersDataType(requestContext).ifPresent(dataTypes::add);
    dataTypes.addAll(
        getEnabledRedactionRulesWithoutSessionIdentifiers(requestContext).stream()
            .map(this::convertRedactionRuleToDataType)
            .collect(Collectors.toList()));
    return ImmutableList.copyOf(dataTypes);
  }

  public DataSet updateDataSet(RequestContext requestContext, String dataSetId, DataSetInfo info) {
    Optional<DataSet> dataSetOptional =
        getDataSetWithIdFromRedactionRules(requestContext, dataSetId);
    DataSet dataSet = dataSetOptional.orElseThrow(Status.NOT_FOUND::asRuntimeException);
    if (dataSetId.equals(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID)) {
      handleAutomaticSecretRedactionDataSetUpdate(requestContext, info);
    } else if (dataSetId.equals(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID)) {
      handleSensitiveHeadersDataSetUpdate(requestContext, info);
    } else {
      List<String> deleteRuleIds =
          dataSet.getInfo().getDataTypeIdsList().stream()
              .filter(ruleId -> !info.getDataTypeIdsList().contains(ruleId))
              .collect(Collectors.toUnmodifiableList());
      deleteRuleIds.forEach(ruleId -> deleteRedactionRule(requestContext, ruleId));
    }
    // handle dataset enable/disable part
    handleLegacyDataSetEnabledFlag(requestContext, dataSetId, info);
    return DataSet.newBuilder().setId(dataSetId).setInfo(info).build();
  }

  public void deleteDataSet(RequestContext requestContext, String dataSetId) {
    Optional<DataSet> dataSetOptional =
        getDataSetWithIdFromRedactionRules(requestContext, dataSetId);
    DataSet dataSet = dataSetOptional.orElseThrow(Status.NOT_FOUND::asRuntimeException);
    if (dataSetId.equals(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID)) {
      handleAutomaticSecretRedactionDataSetDelete(requestContext);
    } else if (dataSetId.equals(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID)) {
      handleSensitiveHeadersDataSetDelete(requestContext);
    } else {
      dataSet
          .getInfo()
          .getDataTypeIdsList()
          .forEach(ruleId -> deleteRedactionRule(requestContext, ruleId));
    }
  }

  private Optional<DataSet> buildDataSet(
      List<String> ruleIds,
      String dataSetId,
      String dataSetName,
      String dataSetDescription,
      DataSuppression dataSuppression,
      Map<String, Boolean> legacyDataEnabledMap) {
    if (!ruleIds.isEmpty()) {
      return Optional.of(
          DataSet.newBuilder()
              .setId(dataSetId)
              .setInfo(
                  DataSetInfo.newBuilder()
                      .setName(dataSetName)
                      .setDescription(dataSetDescription)
                      .setEnabled(
                          Optional.ofNullable(legacyDataEnabledMap.get(dataSetId)).orElse(true))
                      .setDataSuppression(dataSuppression)
                      .addAllDataTypeIds(ruleIds))
              .build());
    }
    return Optional.empty();
  }

  private DataType convertRedactionRuleToDataType(RedactionRule rule) {
    DataType.Builder builder = DataType.newBuilder().setId(rule.getId());
    DataTypeRule.Builder ruleBuilder =
        DataTypeRule.newBuilder().setName(rule.getName()).setDescription(rule.getDescription());
    return builder.setRule(ruleBuilder).build();
  }

  private List<RedactionRule> getEnabledRedactionRulesWithoutSessionIdentifiers(
      RequestContext requestContext) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
                .getRedactionRulesList()
                .stream()
                .filter(
                    redactionRule ->
                        !redactionRule.getDisabled() && !redactionRule.getSessionIdentifier())
                .collect(Collectors.toUnmodifiableList()));
  }

  private void deleteRedactionRule(RequestContext requestContext, String ruleId) {
    requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub.deleteRedactionRule(
                DeleteRedactionRuleRequest.newBuilder().setRedactionRuleId(ruleId).build()));
  }

  private Optional<DataSet> getAutomaticSecretRedactionDataSet(
      RequestContext requestContext, Map<String, Boolean> legacyDataEnabledMap) {
    if (getAutomaticSecretRedactionStrategy(requestContext)) {
      return buildDataSet(
          List.of(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID),
          LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID,
          LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_NAME,
          LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_DESCRIPTION,
          DATA_SUPPRESSION_REDACT,
          legacyDataEnabledMap);
    }
    return Optional.empty();
  }

  private Optional<DataType> getAutomaticSecretRedactionDataType(RequestContext requestContext) {
    if (getAutomaticSecretRedactionStrategy(requestContext)) {
      return Optional.of(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE);
    }
    return Optional.empty();
  }

  private void handleAutomaticSecretRedactionDataSetUpdate(
      RequestContext requestContext, DataSetInfo dataSetInfo) {
    // there is only single data type in this data set
    // if that is removed, then handle it as deletion of data set
    if (dataSetInfo.getDataTypeIdsList().isEmpty()) {
      handleAutomaticSecretRedactionDataSetDelete(requestContext);
    }
  }

  private void handleAutomaticSecretRedactionDataSetDelete(RequestContext requestContext) {
    setAutomaticSecretRedactionStrategyToFalse(requestContext);
  }

  private boolean getAutomaticSecretRedactionStrategy(RequestContext requestContext) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .getAutomaticSecretRedactionStrategy(
                    GetAutomaticSecretRedactionStrategyRequest.getDefaultInstance())
                .getEnabled());
  }

  private void setAutomaticSecretRedactionStrategyToFalse(RequestContext requestContext) {
    requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub.updateAutomaticSecretRedactionStrategy(
                UpdateAutomaticSecretRedactionStrategyRequest.newBuilder()
                    .setEnabled(false)
                    .build()));
  }

  private Optional<DataSet> getSensitiveHeadersDataSet(
      RequestContext requestContext, Map<String, Boolean> legacyDataEnabledMap) {
    RedactionStrategy redactionStrategy = getRedactionStrategyForHeaderParamType(requestContext);
    if (redactionStrategy.equals(REDACTION_STRATEGY_REDACT)
        || redactionStrategy.equals(REDACTION_STRATEGY_HASH)) {
      return buildDataSet(
          List.of(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID),
          LEGACY_SENSITIVE_HEADERS_DATA_SET_ID,
          LEGACY_SENSITIVE_HEADERS_DATA_SET_NAME,
          LEGACY_SENSITIVE_HEADERS_DATA_SET_DESCRIPTION,
          mapToDataSuppression(redactionStrategy),
          legacyDataEnabledMap);
    }
    return Optional.empty();
  }

  private Optional<DataType> getSensitiveHeadersDataType(RequestContext requestContext) {
    RedactionStrategy redactionStrategy = getRedactionStrategyForHeaderParamType(requestContext);
    if (redactionStrategy.equals(REDACTION_STRATEGY_REDACT)
        || redactionStrategy.equals(REDACTION_STRATEGY_HASH)) {
      return Optional.of(LEGACY_SENSITIVE_HEADERS_DATA_TYPE);
    }
    return Optional.empty();
  }

  private void handleSensitiveHeadersDataSetUpdate(
      RequestContext requestContext, DataSetInfo dataSetInfo) {
    // there is only single data type in this data set
    // if that is removed, then handle it as deletion of data set
    if (dataSetInfo.getDataTypeIdsList().isEmpty()) {
      handleSensitiveHeadersDataSetDelete(requestContext);
    }
  }

  private void handleSensitiveHeadersDataSetDelete(RequestContext requestContext) {
    setRedactionStrategyForHeaderParamTypeToUnspecified(requestContext);
  }

  private RedactionStrategy getRedactionStrategyForHeaderParamType(RequestContext requestContext) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .getRedactionStrategyForType(
                    GetRedactionStrategyForTypeRequest.newBuilder()
                        .setParamType(ParamType.PARAM_TYPE_HEADER)
                        .build())
                .getRedactionStrategy());
  }

  private void setRedactionStrategyForHeaderParamTypeToUnspecified(RequestContext requestContext) {
    requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub.updateRedactionStrategyForType(
                UpdateRedactionStrategyForTypeRequest.newBuilder()
                    .setParamType(ParamType.PARAM_TYPE_HEADER)
                    .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED)
                    .build()));
  }

  private DataSuppression mapToDataSuppression(RedactionStrategy redactionStrategy) {
    switch (redactionStrategy) {
      case REDACTION_STRATEGY_REDACT:
        return DATA_SUPPRESSION_REDACT;
      case REDACTION_STRATEGY_HASH:
        return DATA_SUPPRESSION_OBFUSCATE;
      case REDACTION_STRATEGY_RAW:
        return DATA_SUPPRESSION_RAW;
      default:
        return DATA_SUPPRESSION_UNSPECIFIED;
    }
  }

  private void handleLegacyDataSetEnabledFlag(
      RequestContext requestContext, String dataSetId, DataSetInfo newDataSetInfo) {
    // by default legacy datasets are enabled
    boolean isExistingDataSetEnabled = true;
    Optional<DataSet> existingDataSet = this.legacyDataSetStore.getData(requestContext, dataSetId);
    if (existingDataSet.isPresent()) {
      isExistingDataSetEnabled = existingDataSet.get().getInfo().getEnabled();
    }
    if (isExistingDataSetEnabled != newDataSetInfo.getEnabled()) {
      this.legacyDataSetStore.upsertObject(
          requestContext,
          DataSet.newBuilder()
              .setId(dataSetId)
              .setInfo(DataSetInfo.newBuilder().setEnabled(newDataSetInfo.getEnabled()))
              .build());
    }
  }
}
