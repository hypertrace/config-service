package ai.traceable.data.handling.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.data.handling.config.service.v1.CreateDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleAction;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleCondition;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleData;
import ai.traceable.data.handling.config.service.v1.DeleteDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.GetDataHandlingRulesRequest;
import ai.traceable.data.handling.config.service.v1.RankDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.UpdateDataHandlingRuleRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DataHandlingConfigRequestValidator {

  public void validateOrThrow(RequestContext requestContext, GetDataHandlingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, CreateDataHandlingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateRuleData(request.getData());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateDataHandlingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateDataHandlingRuleRequest.ID_FIELD_NUMBER);
    this.validateRuleData(request.getData());
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteDataHandlingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteDataHandlingRuleRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, RankDataHandlingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, RankDataHandlingRuleRequest.RULE_ID_TO_UPDATE_FIELD_NUMBER);
    if (request.getRuleIdToUpdate().equals(request.getPrecedingRuleId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Can't rerank a rule against itself: " + printMessage(request))
          .asRuntimeException();
    }
  }

  private void validateRuleData(DataHandlingRuleData ruleData) {
    validateNonDefaultPresenceOrThrow(ruleData, DataHandlingRuleData.NAME_FIELD_NUMBER);

    validateNonDefaultPresenceOrThrow(ruleData, DataHandlingRuleData.CONDITIONS_FIELD_NUMBER);
    ruleData.getConditionsList().forEach(this::validateRuleCondition);

    validateNonDefaultPresenceOrThrow(ruleData, DataHandlingRuleData.ACTIONS_FIELD_NUMBER);
    ruleData.getActionsList().forEach(this::validateRuleAction);
  }

  private void validateRuleAction(DataHandlingRuleAction action) {
    // TODO - currently noop
  }

  private void validateRuleCondition(DataHandlingRuleCondition condition) {
    // TODO - currently noop
  }
}
