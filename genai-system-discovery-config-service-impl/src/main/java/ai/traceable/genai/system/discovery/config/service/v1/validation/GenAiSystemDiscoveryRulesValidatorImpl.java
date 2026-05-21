package ai.traceable.genai.system.discovery.config.service.v1.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.genai.system.discovery.config.service.v1.CompositeCondition;
import ai.traceable.genai.system.discovery.config.service.v1.Condition;
import ai.traceable.genai.system.discovery.config.service.v1.CreateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.DeleteGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.DynamicNameExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.EntityPathReference;
import ai.traceable.genai.system.discovery.config.service.v1.ExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiInfoExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiNameExtractionAction;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRuleData;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesRequest;
import ai.traceable.genai.system.discovery.config.service.v1.KeyValueCondition;
import ai.traceable.genai.system.discovery.config.service.v1.LeafCondition;
import ai.traceable.genai.system.discovery.config.service.v1.MatchCondition;
import ai.traceable.genai.system.discovery.config.service.v1.MatchGroupOperation;
import ai.traceable.genai.system.discovery.config.service.v1.ReferenceCondition;
import ai.traceable.genai.system.discovery.config.service.v1.StaticNameAction;
import ai.traceable.genai.system.discovery.config.service.v1.UpdateGenAiSystemDiscoveryRuleRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class GenAiSystemDiscoveryRulesValidatorImpl implements GenAiSystemDiscoveryRulesValidator {
  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetGenAiSystemDiscoveryRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, CreateGenAiSystemDiscoveryRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateGenAiSystemDiscoveryRuleData(request.getGenAiSystemDiscoveryRuleData());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, UpdateGenAiSystemDiscoveryRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateGenAiSystemDiscoveryRule(request.getGenAiSystemDiscoveryRule());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteGenAiSystemDiscoveryRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteGenAiSystemDiscoveryRuleRequest.RULE_ID_FIELD_NUMBER);
  }

  @Override
  public void validateGenAiSystemDiscoveryRule(GenAiSystemDiscoveryRule rule) {
    validateNonDefaultPresenceOrThrow(rule, GenAiSystemDiscoveryRule.RULE_ID_FIELD_NUMBER);
    validateGenAiSystemDiscoveryRuleData(rule.getGenAiSystemDiscoveryRuleData());
  }

  private void validateGenAiSystemDiscoveryRuleData(
      GenAiSystemDiscoveryRuleData genAiSystemDiscoveryRuleData) {
    validateNonDefaultPresenceOrThrow(
        genAiSystemDiscoveryRuleData, GenAiSystemDiscoveryRuleData.NAME_FIELD_NUMBER);
    validateCondition(genAiSystemDiscoveryRuleData.getCondition());
    validateGenAiInfoExtractionAction(genAiSystemDiscoveryRuleData.getGenAiInfoExtractionAction());
  }

  private void validateCondition(Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        validateLeafCondition(condition.getLeafCondition());
        break;
      case COMPOSITE_CONDITION:
        validateCompositeCondition(condition.getCompositeCondition());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Invalid condition case : %s", condition.getConditionCase()))
            .asRuntimeException();
    }
  }

  private void validateLeafCondition(LeafCondition leafCondition) {
    switch (leafCondition.getConditionCase()) {
      case URL_CONDITION:
        validateMatchCondition(leafCondition.getUrlCondition());
        break;
      case ATTRIBUTE_CONDITION:
        validateKeyValueCondition(leafCondition.getAttributeCondition());
        break;
      case REQUEST_BODY_CONDITION:
        validateKeyValueCondition(leafCondition.getRequestBodyCondition());
        break;
      case RESPONSE_BODY_CONDITION:
        validateKeyValueCondition(leafCondition.getResponseBodyCondition());
        break;
      case REFERENCE_CONDITION:
        validateReferenceCondition(leafCondition.getReferenceCondition());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Invalid leaf condition case : %s", leafCondition.getConditionCase()))
            .asRuntimeException();
    }
  }

  private void validateReferenceCondition(ReferenceCondition referenceCondition) {
    final EntityPathReference reference = referenceCondition.getReference();
    if (reference.getEntityType().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Reference entity type should not be blank in reference condition")
          .asRuntimeException();
    }
    if (reference.getPath().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Reference path should not be blank in reference condition")
          .asRuntimeException();
    }
    validateMatchCondition(referenceCondition.getValueMatch());
  }

  private void validateMatchCondition(MatchCondition urlCondition) {
    validateNonDefaultPresenceOrThrow(urlCondition, MatchCondition.OPERATOR_FIELD_NUMBER);
    if (urlCondition.getValue().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Value in matchCondition should not be blank : %s", urlCondition))
          .asRuntimeException();
    }
  }

  private void validateKeyValueCondition(KeyValueCondition attributeCondition) {
    if (isDefaultKeyOrValueMatchCondition(attributeCondition)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Both keyMatchCondition and valueMatchCondition should be present in attributeCondition : %s",
                  attributeCondition))
          .asRuntimeException();
    }
    validateMatchCondition(attributeCondition.getKeyMatchCondition());
    validateMatchCondition(attributeCondition.getValueMatchCondition());
  }

  private boolean isDefaultKeyOrValueMatchCondition(KeyValueCondition attributeCondition) {
    return MatchCondition.getDefaultInstance().equals(attributeCondition.getKeyMatchCondition())
        || MatchCondition.getDefaultInstance().equals(attributeCondition.getValueMatchCondition());
  }

  private void validateCompositeCondition(CompositeCondition compositeCondition) {
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.CHILDREN_FIELD_NUMBER);
    compositeCondition.getChildrenList().forEach(this::validateCondition);
  }

  private void validateGenAiInfoExtractionAction(
      GenAiInfoExtractionAction genAiInfoExtractionAction) {
    validateGenAiNameExtractionAction(genAiInfoExtractionAction.getProviderAction());
    validateGenAiNameExtractionAction(genAiInfoExtractionAction.getModelAction());
  }

  private void validateGenAiNameExtractionAction(GenAiNameExtractionAction action) {
    switch (action.getGenAiNameExtractionActionCase()) {
      case STATIC_NAME_ACTION:
        validateStaticNameAction(action.getStaticNameAction());
        break;
      case DYNAMIC_NAME_EXTRACTION_ACTION:
        validateDynamicNameExtractionAction(action.getDynamicNameExtractionAction());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid genai name extraction action case : %s",
                    action.getGenAiNameExtractionActionCase()))
            .asRuntimeException();
    }
  }

  private void validateStaticNameAction(StaticNameAction staticNameAction) {
    if (staticNameAction.getValue().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Value in static action should not be blank")
          .asRuntimeException();
    }
  }

  private void validateDynamicNameExtractionAction(
      DynamicNameExtractionAction dynamicNameExtractionAction) {
    switch (dynamicNameExtractionAction.getDynamicNameExtractionActionCase()) {
      case URL_EXTRACTION_ACTION:
        validateMatchGroupOperation(dynamicNameExtractionAction.getUrlExtractionAction());
        break;
      case ATTRIBUTE_EXTRACTION_ACTION:
        validateExtractionAction(dynamicNameExtractionAction.getAttributeExtractionAction());
        break;
      case REQUEST_BODY_EXTRACTION_ACTION:
        validateExtractionAction(dynamicNameExtractionAction.getRequestBodyExtractionAction());
        break;
      case RESPONSE_BODY_EXTRACTION_ACTION:
        validateExtractionAction(dynamicNameExtractionAction.getResponseBodyExtractionAction());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid dynamic name extraction action case : %s",
                    dynamicNameExtractionAction.getDynamicNameExtractionActionCase()))
            .asRuntimeException();
    }
  }

  private void validateExtractionAction(ExtractionAction attributeExtractionAction) {
    if (attributeExtractionAction.getKey().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Key should not be blank in attribute extraction action : %s",
                  attributeExtractionAction))
          .asRuntimeException();
    }
    switch (attributeExtractionAction.getValueExtractionOperationCase()) {
      case NO_OPERATION:
        break;
      case MATCH_GROUP_OPERATION:
        validateMatchGroupOperation(attributeExtractionAction.getMatchGroupOperation());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid value extraction operation case : %s",
                    attributeExtractionAction.getValueExtractionOperationCase()))
            .asRuntimeException();
    }
  }

  private void validateMatchGroupOperation(MatchGroupOperation matchGroupOperation) {
    if (matchGroupOperation.getMatchGroup().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Match group should not be blank in match group operation : %s",
                  matchGroupOperation))
          .asRuntimeException();
    }
    if (matchGroupOperation.getMatchGroupRegex().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Match group regex should not be blank in match group operation : %s",
                  matchGroupOperation))
          .asRuntimeException();
    }
  }
}
