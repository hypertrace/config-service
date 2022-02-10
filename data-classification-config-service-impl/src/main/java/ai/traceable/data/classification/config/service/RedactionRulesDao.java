package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_OBFUSCATE;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_RAW;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_REDACT;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.sensitivedata.config.service.v1.DeleteRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RedactionRulesDao {
  static final String LEGACY_REDACT_DATA_SET_NAME = "Legacy Dataset - Redacted";
  static final String LEGACY_REDACT_DATA_SET_DESCRIPTION =
      "Legacy dataset containing redaction rules with redacted strategy";
  static final String LEGACY_REDACT_DATA_SET_ID = "legacy-dataset-redacted-id";
  static final String LEGACY_OBFUSCATE_DATA_SET_NAME = "Legacy Dataset - Obfuscated";
  static final String LEGACY_OBFUSCATE_DATA_SET_DESCRIPTION =
      "Legacy dataset containing redaction rules with obfuscated strategy";
  static final String LEGACY_OBFUSCATE_DATA_SET_ID = "legacy-dataset-obfuscated-id";
  static final String LEGACY_RAW_DATA_SET_NAME = "Legacy Dataset - Unsuppressed";
  static final String LEGACY_RAW_DATA_SET_DESCRIPTION =
      "Legacy dataset containing redaction rules with collect strategy";
  static final String LEGACY_RAW_DATA_SET_ID = "legacy-dataset-unsuppressed-id";
  static final Set<String> LEGACY_DATA_SET_IDS =
      Set.of(LEGACY_REDACT_DATA_SET_ID, LEGACY_OBFUSCATE_DATA_SET_ID, LEGACY_RAW_DATA_SET_ID);

  private final SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub;

  @Inject
  public RedactionRulesDao(
      SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub) {
    this.sensitiveDataConfigServiceBlockingStub = sensitiveDataConfigServiceBlockingStub;
  }

  public List<DataSet> getDataSetsFromRedactionRules(RequestContext requestContext) {
    List<RedactionRule> allRedactionRules = getAllRedactionRules(requestContext);
    List<String> ruleIdsForRedact = new ArrayList<>();
    List<String> ruleIdsForObfuscate = new ArrayList<>();
    List<String> rulesIdsForRaw = new ArrayList<>();
    allRedactionRules.forEach(
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
            DATA_SUPPRESSION_REDACT);

    Optional<DataSet> dataSetForObfuscate =
        buildDataSet(
            ruleIdsForObfuscate,
            LEGACY_OBFUSCATE_DATA_SET_ID,
            LEGACY_OBFUSCATE_DATA_SET_NAME,
            LEGACY_OBFUSCATE_DATA_SET_DESCRIPTION,
            DATA_SUPPRESSION_OBFUSCATE);

    Optional<DataSet> dataSetForRaw =
        buildDataSet(
            rulesIdsForRaw,
            LEGACY_RAW_DATA_SET_ID,
            LEGACY_RAW_DATA_SET_NAME,
            LEGACY_RAW_DATA_SET_DESCRIPTION,
            DATA_SUPPRESSION_RAW);

    return Stream.of(dataSetForRedact, dataSetForObfuscate, dataSetForRaw)
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
    return getAllRedactionRules(requestContext).stream()
        .map(this::convertRedactionRuleToDataType)
        .collect(Collectors.toUnmodifiableList());
  }

  public DataSet updateDataSet(RequestContext requestContext, String dataSetId, DataSetInfo info) {
    Optional<DataSet> dataSetOptional =
        getDataSetWithIdFromRedactionRules(requestContext, dataSetId);
    DataSet dataSet = dataSetOptional.orElseThrow(Status.NOT_FOUND::asRuntimeException);
    List<String> deleteRuleIds =
        dataSet.getInfo().getDataTypeIdsList().stream()
            .filter(ruleId -> !info.getDataTypeIdsList().contains(ruleId))
            .collect(Collectors.toUnmodifiableList());
    deleteRuleIds.forEach(ruleId -> deleteRedactionRule(requestContext, ruleId));
    return DataSet.newBuilder().setId(dataSetId).setInfo(info).build();
  }

  public void deleteDataSet(RequestContext requestContext, String dataSetId) {
    Optional<DataSet> dataSetOptional =
        getDataSetWithIdFromRedactionRules(requestContext, dataSetId);
    dataSetOptional.ifPresent(
        dataSet ->
            dataSet
                .getInfo()
                .getDataTypeIdsList()
                .forEach(ruleId -> deleteRedactionRule(requestContext, ruleId)));
  }

  private Optional<DataSet> buildDataSet(
      List<String> ruleIds,
      String dataSetId,
      String dataSetName,
      String dataSetDescription,
      DataSuppression dataSuppression) {
    if (!ruleIds.isEmpty()) {
      return Optional.of(
          DataSet.newBuilder()
              .setId(dataSetId)
              .setInfo(
                  DataSetInfo.newBuilder()
                      .setName(dataSetName)
                      .setDescription(dataSetDescription)
                      .setEnabled(true)
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

  private List<RedactionRule> getAllRedactionRules(RequestContext requestContext) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
                .getRedactionRulesList()
                .stream()
                .filter(redactionRule -> !redactionRule.getSessionIdentifier())
                .collect(Collectors.toUnmodifiableList()));
  }

  private void deleteRedactionRule(RequestContext requestContext, String ruleId) {
    requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub.deleteRedactionRule(
                DeleteRedactionRuleRequest.newBuilder().setRedactionRuleId(ruleId).build()));
  }
}
