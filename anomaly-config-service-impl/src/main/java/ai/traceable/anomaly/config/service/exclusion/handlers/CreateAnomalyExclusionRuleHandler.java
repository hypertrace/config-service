package ai.traceable.anomaly.config.service.exclusion.handlers;

import ai.traceable.anomaly.config.service.exclusion.converters.AnomalyExclusionRuleConfigConverter;
import ai.traceable.anomaly.config.service.exclusion.utils.UuidGenerator;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRuleUtils;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.CreateAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.CreateAnomalyExclusionRuleResponse;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import io.grpc.Status;
import javax.inject.Inject;

public class CreateAnomalyExclusionRuleHandler {
  private final ConfigServiceHandler configServiceHandler;
  private final AnomalyExclusionRuleConfigConverter ruleConfigConverter;
  private final ModsecRuleUtils modsecRuleUtils;
  private final UuidGenerator uuidGenerator;
  private final AnomalyConfigStatus anomalyConfigStatus;

  @Inject
  CreateAnomalyExclusionRuleHandler(
      ConfigServiceHandler configServiceHandler,
      AnomalyExclusionRuleConfigConverter ruleConfigConverter,
      ModsecRuleUtils modsecRuleUtils,
      UuidGenerator uuidGenerator) {
    this.configServiceHandler = configServiceHandler;
    this.ruleConfigConverter = ruleConfigConverter;
    this.modsecRuleUtils = modsecRuleUtils;
    this.uuidGenerator = uuidGenerator;
    this.anomalyConfigStatus = AnomalyConfigStatus.newBuilder().getDefaultInstanceForType();
  }

  public CreateAnomalyExclusionRuleResponse createRule(CreateAnomalyExclusionRuleRequest request) {
    AnomalyExclusionRuleData anomalyExclusionRuleData = request.getRuleData();
    String ruleId = uuidGenerator.generateId(anomalyExclusionRuleData);

    boolean configAlreadyExists = true;
    try {
      // check if same rule config already exists, don't create the duplicate config.
      configServiceHandler.getExclusionConfigByRuleId(ruleId);
    } catch (Exception e) {
      if (Status.fromThrowable(e).getCode().equals(Status.NOT_FOUND.getCode())) {
        configAlreadyExists = false;
      } else {
        throw e;
      }
    }
    if (configAlreadyExists) {
      throw new IllegalStateException("Trying to create a duplicate rule");
    }

    // If event family is modSec, we want to get the rule_id out of subrule id.
    EventExclusionInfo enrichedEventExclusionInfo =
        anomalyExclusionRuleData.getEventExclusionInfo();
    if (AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC.equals(
            anomalyExclusionRuleData.getEventExclusionInfo().getAnomalyEventFamily())
        && EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE.equals(
            anomalyExclusionRuleData.getEventExclusionInfo().getEventExclusionType())) {
      String enrichedEventTypeId =
          modsecRuleUtils.getModsecParentRuleId(
              anomalyExclusionRuleData.getEventExclusionInfo().getEventTypeId());
      enrichedEventExclusionInfo =
          enrichedEventExclusionInfo.toBuilder().setEventTypeId(enrichedEventTypeId).build();
    }

    AnomalyExclusionRuleConfig anomalyExclusionRuleConfig =
        AnomalyExclusionRuleConfig.newBuilder()
            .setId(ruleId)
            .setRuleData(
                anomalyExclusionRuleData.toBuilder()
                    .setEventExclusionInfo(enrichedEventExclusionInfo))
            .setConfigStatus(anomalyConfigStatus)
            .build();

    configServiceHandler.upsertExclusionConfigByRuleId(
        ruleId, ruleConfigConverter.convert(anomalyExclusionRuleConfig));

    return CreateAnomalyExclusionRuleResponse.newBuilder()
        .setConfig(anomalyExclusionRuleConfig)
        .build();
  }
}
