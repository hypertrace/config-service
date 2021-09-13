package ai.traceable.data.handling.config.service.validation;

import ai.traceable.data.handling.config.service.v1.CreateDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleAction;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleCondition;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleData;
import ai.traceable.data.handling.config.service.v1.DeleteDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.GetDataHandlingRulesRequest;
import ai.traceable.data.handling.config.service.v1.RankDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.UpdateDataHandlingRuleRequest;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DataHandlingConfigRequestValidator {
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

  public void validateOrThrow(RequestContext requestContext, GetDataHandlingRulesRequest request) {
    this.validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, CreateDataHandlingRuleRequest request) {
    this.validateRequestContext(requestContext);
    this.validateRuleData(request.getData());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateDataHandlingRuleRequest request) {
    this.validateRequestContext(requestContext);
    this.validateNonDefaultPresence(request, UpdateDataHandlingRuleRequest.ID_FIELD_NUMBER);
    this.validateRuleData(request.getData());
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteDataHandlingRuleRequest request) {
    this.validateRequestContext(requestContext);
    this.validateNonDefaultPresence(request, DeleteDataHandlingRuleRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, RankDataHandlingRuleRequest request) {
    this.validateRequestContext(requestContext);
    this.validateNonDefaultPresence(
        request, RankDataHandlingRuleRequest.RULE_ID_TO_UPDATE_FIELD_NUMBER);
    if (request.getRuleIdToUpdate().equals(request.getPrecedingRuleId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Can't rerank a rule against itself: " + this.printOrToString(request))
          .asRuntimeException();
    }
  }

  private void validateRuleData(DataHandlingRuleData ruleData) {
    this.validateNonDefaultPresence(ruleData, DataHandlingRuleData.NAME_FIELD_NUMBER);

    this.validateNonDefaultPresence(ruleData, DataHandlingRuleData.CONDITIONS_FIELD_NUMBER);
    ruleData.getConditionsList().forEach(this::validateRuleCondition);

    this.validateNonDefaultPresence(ruleData, DataHandlingRuleData.ACTIONS_FIELD_NUMBER);
    ruleData.getActionsList().forEach(this::validateRuleAction);
  }

  private void validateRuleAction(DataHandlingRuleAction action) {
    // TODO - currently noop
  }

  private void validateRuleCondition(DataHandlingRuleCondition condition) {
    // TODO - currently noop
  }

  private void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID in request")
          .asRuntimeException();
    }
  }

  private <T extends Message> void validateNonDefaultPresence(T source, int fieldNumber) {
    FieldDescriptor descriptor = source.getDescriptorForType().findFieldByNumber(fieldNumber);

    if (descriptor.isRepeated()) {
      this.validateNonDefaultPresenceRepeated(source, descriptor);
    } else if (!source.hasField(descriptor)
        || source.getField(descriptor).equals(descriptor.getDefaultValue())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Expected field value %s but not present:%n %s",
                  descriptor.getFullName(), this.printOrToString(source)))
          .asRuntimeException();
    }
  }

  private <T extends Message> void validateNonDefaultPresenceRepeated(
      T source, FieldDescriptor descriptor) {
    if (source.getRepeatedFieldCount(descriptor) == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Expected at least 1 value for repeated field %s but not present:%n %s",
                  descriptor.getFullName(), this.printOrToString(source)))
          .asRuntimeException();
    }
  }

  private String printOrToString(Message message) {
    try {
      return JSON_PRINTER.print(message);
    } catch (Exception exception) {
      return message.toString();
    }
  }
}
