package ai.traceable.data.exfiltration.config.service.detection.rule;

import static ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleConfig.ID_FIELD_NUMBER;
import static ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleData.NAME_FIELD_NUMBER;
import static ai.traceable.data.exfiltration.config.service.v1.DeleteDataExfiltrationDetectionRuleRequest.RULE_ID_FIELD_NUMBER;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.data.exfiltration.config.service.v1.CreateDataExfiltrationDetectionRuleRequest;
import ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleConfig;
import ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleData;
import ai.traceable.data.exfiltration.config.service.v1.DeleteDataExfiltrationDetectionRuleRequest;
import ai.traceable.data.exfiltration.config.service.v1.GetDataExfiltrationDetectionRulesRequest;
import ai.traceable.data.exfiltration.config.service.v1.UpdateDataExfiltrationDetectionRuleRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DataExfiltrationDetectionRulesRequestValidator {
  public void validateGetRequest(
      RequestContext requestContext, GetDataExfiltrationDetectionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateCreateRequest(
      RequestContext requestContext, CreateDataExfiltrationDetectionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateRuleData(request.getRuleData());
  }

  public void validateUpdateRequest(
      RequestContext requestContext, UpdateDataExfiltrationDetectionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateRuleConfig(request.getRuleConfig());
  }

  public void validateDeleteRequest(
      RequestContext requestContext, DeleteDataExfiltrationDetectionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, RULE_ID_FIELD_NUMBER);
  }

  private void validateRuleConfig(DataExfiltrationDetectionRuleConfig config) {
    validateNonDefaultPresenceOrThrow(config, ID_FIELD_NUMBER);
    validateRuleData(config.getRuleData());
  }

  private void validateRuleData(DataExfiltrationDetectionRuleData data) {

    validateNonDefaultPresenceOrThrow(data, NAME_FIELD_NUMBER);
  }
}
