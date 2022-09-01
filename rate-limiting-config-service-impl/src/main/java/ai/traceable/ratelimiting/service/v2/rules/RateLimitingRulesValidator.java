package ai.traceable.ratelimiting.service.v2.rules;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.IpLocationType;
import ai.traceable.ratelimiting.config.service.v2.IpLocationTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.IpReputationCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import com.google.protobuf.Message;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingRulesValidator implements RulesValidator {
  @Override
  public void validateOrThrow(RequestContext requestContext, GetRateLimitingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, UpdateRateLimitingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateRateLimitingRuleRequest.RULE_ID_FIELD_NUMBER);
    validateRateLimitingRuleData(request.getData());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteRateLimitingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteRateLimitingRuleRequest.RULE_ID_FIELD_NUMBER);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, CreateRateLimitingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateRateLimitingRuleData(request.getData());
  }

  private void validateRateLimitingRuleData(RateLimitingRuleData data) {
    validateNonDefaultPresenceOrThrow(data, RateLimitingRuleData.CATEGORY_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(data, RateLimitingRuleData.NAME_FIELD_NUMBER);
    validateCondition(data.getCondition());
    validateNonDefaultPresenceOrThrow(
        data, RateLimitingRuleData.THRESHOLD_ACTION_CONFIGS_FIELD_NUMBER);
    data.getThresholdActionConfigsList().forEach(this::validateThresholdActionConfig);
    if (data.hasRuleConfigScope()) {
      validateRuleConfigScope(data.getRuleConfigScope());
    }
  }

  private void validateRuleConfigScope(RuleConfigScope scope) {
    if (scope.hasEnvironmentScope()) {
      validateNonDefaultPresenceOrThrow(
          scope.getEnvironmentScope(), EnvironmentScope.ENVIRONMENT_IDS_FIELD_NUMBER);
    }
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
        throwInvalidArgumentException(
            String.format("Invalid Case in %s:%n %s", getName(condition), printMessage(condition)));
    }
  }

  private void validateLeafCondition(LeafCondition leafCondition) {
    switch (leafCondition.getConditionCase()) {
      case SCOPE_CONDITION:
        validateScopeCondition(leafCondition.getScopeCondition());
        break;
      case KEY_VALUE_CONDITION:
        validateKeyValueCondition(leafCondition.getKeyValueCondition());
        break;
      case DATATYPE_CONDITION:
        validateDatatypeCondition(leafCondition.getDatatypeCondition());
        break;
      case REGION_CONDITION:
        validateRegionCondition(leafCondition.getRegionCondition());
        break;
      case IP_ADDRESS_CONDITION:
        validateIpAddressCondition(leafCondition.getIpAddressCondition());
        break;
      case IP_LOCATION_TYPE_CONDITION:
        validateIpLocationTypeCondition(leafCondition.getIpLocationTypeCondition());
        break;
      case IP_REPUTATION_CONDITION:
        validateIpReputationCondition(leafCondition.getIpReputationCondition());
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s", getName(leafCondition), printMessage(leafCondition)));
    }
  }

  private void validateScopeCondition(ScopeCondition scopeCondition) {
    switch (scopeCondition.getScopeCase()) {
      case ENTITY_SCOPE:
        validateEntityScope(scopeCondition.getEntityScope());
        break;
      case LABEL_SCOPE:
        validateLabelScope(scopeCondition.getLabelScope());
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s", getName(scopeCondition), printMessage(scopeCondition)));
    }
  }

  private void validateEntityScope(ScopeCondition.EntityScope entityScope) {
    validateNonDefaultPresenceOrThrow(
        entityScope, ScopeCondition.EntityScope.ENTITY_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        entityScope, ScopeCondition.EntityScope.ENTITY_IDS_FIELD_NUMBER);
  }

  private void validateLabelScope(ScopeCondition.LabelScope labelScope) {
    validateNonDefaultPresenceOrThrow(
        labelScope, ScopeCondition.LabelScope.LABEL_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(labelScope, ScopeCondition.LabelScope.LABEL_IDS_FIELD_NUMBER);
  }

  private void validateKeyValueCondition(KeyValueCondition keyValueCondition) {
    validateNonDefaultPresenceOrThrow(keyValueCondition, KeyValueCondition.TYPE_FIELD_NUMBER);
    if (!keyValueCondition.hasKeyCondition() && !keyValueCondition.hasValueCondition()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(keyValueCondition), printMessage(keyValueCondition)));
    }
    if (keyValueCondition.hasKeyCondition()) {
      validateStringCondition(keyValueCondition.getKeyCondition());
    }
    if (keyValueCondition.hasValueCondition()) {
      validateStringCondition(keyValueCondition.getValueCondition());
    }
  }

  private void validateStringCondition(KeyValueCondition.StringCondition stringCondition) {
    validateNonDefaultPresenceOrThrow(
        stringCondition, KeyValueCondition.StringCondition.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        stringCondition, KeyValueCondition.StringCondition.VALUE_FIELD_NUMBER);
  }

  private void validateDatatypeCondition(DatatypeCondition datatypeCondition) {
    if (datatypeCondition.getDatasetIdsList().isEmpty()
        && datatypeCondition.getDatatypeIdsList().isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(datatypeCondition), printMessage(datatypeCondition)));
    }
  }

  private void validateRegionCondition(RegionCondition regionCondition) {
    validateNonDefaultPresenceOrThrow(regionCondition, RegionCondition.REGIONS_FIELD_NUMBER);
  }

  private void validateIpAddressCondition(IpAddressCondition ipAddressCondition) {
    if (ipAddressCondition.getRawInputIpDataList().isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(ipAddressCondition), printMessage(ipAddressCondition)));
    }
  }

  private void validateIpReputationCondition(IpReputationCondition ipReputationCondition) {
    validateNonDefaultPresenceOrThrow(
        ipReputationCondition, IpReputationCondition.MIN_IP_REPUTATION_SEVERITY_FIELD_NUMBER);
  }

  private void validateIpLocationTypeCondition(IpLocationTypeCondition ipLocationTypeCondition) {
    validateNonDefaultPresenceOrThrow(
        ipLocationTypeCondition, IpLocationTypeCondition.IP_LOCATION_TYPES_FIELD_NUMBER);
    ipLocationTypeCondition.getIpLocationTypesList().forEach(this::validateIpLocationType);
  }

  private void validateIpLocationType(IpLocationType ipLocationType) {
    if (ipLocationType.equals(IpLocationType.IP_LOCATION_TYPE_UNSPECIFIED)) {
      throwInvalidArgumentException("Invalid IP Location Type");
    }
  }

  private void validateCompositeCondition(CompositeCondition compositeCondition) {
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.OPERATOR_FIELD_NUMBER);
    compositeCondition.getChildrenList().forEach(this::validateCondition);
  }

  private void validateThresholdActionConfig(ThresholdActionConfig thresholdActionConfig) {
    validateNonDefaultPresenceOrThrow(
        thresholdActionConfig,
        ThresholdActionConfig.RESOURCE_ACCESS_THRESHOLD_CONFIGS_FIELD_NUMBER);
    thresholdActionConfig
        .getResourceAccessThresholdConfigsList()
        .forEach(this::validateResourceAccessThresholdConfig);
    validateNonDefaultPresenceOrThrow(
        thresholdActionConfig, ThresholdActionConfig.ACTIONS_FIELD_NUMBER);
    thresholdActionConfig.getActionsList().forEach(this::validateAction);
  }

  private void validateResourceAccessThresholdConfig(
      ResourceAccessThresholdConfig resourceAccessThresholdConfig) {
    validateNonDefaultPresenceOrThrow(
        resourceAccessThresholdConfig,
        ResourceAccessThresholdConfig.USER_AGGREGATE_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        resourceAccessThresholdConfig,
        ResourceAccessThresholdConfig.API_AGGREGATE_TYPE_FIELD_NUMBER);
    switch (resourceAccessThresholdConfig.getThresholdConfigCase()) {
      case ROLLING_WINDOW_THRESHOLD_CONFIG:
        validateRollingWindowThresholdConfig(
            resourceAccessThresholdConfig.getRollingWindowThresholdConfig());
        break;
      case VALUE_BASED_THRESHOLD_CONFIG:
        validateValueBasedThresholdConfig(
            resourceAccessThresholdConfig.getValueBasedThresholdConfig());
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s",
                getName(resourceAccessThresholdConfig),
                printMessage(resourceAccessThresholdConfig)));
    }
  }

  private void validateRollingWindowThresholdConfig(
      ResourceAccessThresholdConfig.RollingWindowThresholdConfig rollingWindowThresholdConfig) {
    validateNonDefaultPresenceOrThrow(
        rollingWindowThresholdConfig,
        ResourceAccessThresholdConfig.RollingWindowThresholdConfig.COUNT_ALLOWED_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        rollingWindowThresholdConfig,
        ResourceAccessThresholdConfig.RollingWindowThresholdConfig.DURATION_ISO_FIELD_NUMBER);
  }

  private void validateValueBasedThresholdConfig(
      ResourceAccessThresholdConfig.ValueBasedThresholdConfig valueBasedThresholdConfig) {
    validateNonDefaultPresenceOrThrow(
        valueBasedThresholdConfig,
        ResourceAccessThresholdConfig.ValueBasedThresholdConfig.UNIQUE_VALUES_ALLOWED_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        valueBasedThresholdConfig,
        ResourceAccessThresholdConfig.ValueBasedThresholdConfig.DURATION_ISO_FIELD_NUMBER);
  }

  private void validateAction(Action action) {
    switch (action.getActionCase()) {
      case ALERT:
      case BLOCK:
        break;
      default:
        throwInvalidArgumentException(
            String.format("Invalid Case in %s:%n %s", getName(action), printMessage(action)));
    }
  }

  private void throwInvalidArgumentException(String description) {
    throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
  }

  private String getName(Message message) {
    return message.getDescriptorForType().getName();
  }
}
