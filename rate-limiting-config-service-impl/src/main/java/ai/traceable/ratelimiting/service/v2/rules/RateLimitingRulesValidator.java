package ai.traceable.ratelimiting.service.v2.rules;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.ApiAggregateType;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.EmailDomainCondition;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionType;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.IpLocationType;
import ai.traceable.ratelimiting.config.service.v2.IpLocationTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.IpReputationCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.UserAgentCondition;
import ai.traceable.ratelimiting.config.service.v2.UserAggregateType;
import ai.traceable.ratelimiting.config.service.v2.UserIdCondition;
import com.google.protobuf.Message;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingRulesValidator implements RulesValidator {
  private static final String UTF_8_REGEX_PREFIX = "(*UTF8)";

  @Override
  public void validateOrThrow(RequestContext requestContext, GetRateLimitingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      UpdateRateLimitingRuleRequest request,
      List<RateLimitingRule> existingRules) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateRateLimitingRuleRequest.RULE_ID_FIELD_NUMBER);
    RateLimitingRuleData requestData = request.getData();
    Optional<RateLimitingRule> rule =
        getRuleOfSameNameAndCategory(
            requestData.getCategory(), requestData.getName(), existingRules);
    if (rule.isPresent() && !rule.get().getId().equals(request.getRuleId())) {
      throwInvalidArgumentException(
          String.format(
              "Rate limiting rule with name (%s) already exists for category : %s",
              requestData.getName(), requestData.getCategory()));
    }
    validateRateLimitingRuleData(requestData);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteRateLimitingRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteRateLimitingRuleRequest.RULE_ID_FIELD_NUMBER);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      CreateRateLimitingRuleRequest request,
      List<RateLimitingRule> existingRules) {
    validateRequestContextOrThrow(requestContext);
    RateLimitingRuleData requestData = request.getData();
    Optional<RateLimitingRule> rule =
        getRuleOfSameNameAndCategory(
            requestData.getCategory(), requestData.getName(), existingRules);
    if (rule.isPresent()) {
      throwInvalidArgumentException(
          String.format(
              "Rate limiting rule with name (%s) already exists for category : %s",
              requestData.getName(), requestData.getCategory()));
    }
    validateRateLimitingRuleData(requestData);
  }

  private Optional<RateLimitingRule> getRuleOfSameNameAndCategory(
      Category category, String name, List<RateLimitingRule> existingRules) {
    Optional<RateLimitingRule> sameNameRuleOfSameCategory =
        existingRules.stream()
            .filter(
                rule ->
                    rule.getData().getCategory().equals(category)
                        && rule.getData().getName().equals(name))
            .findFirst();
    return sameNameRuleOfSameCategory;
  }

  private void validateRateLimitingRuleData(RateLimitingRuleData data) {
    validateNonDefaultPresenceOrThrow(data, RateLimitingRuleData.CATEGORY_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(data, RateLimitingRuleData.NAME_FIELD_NUMBER);
    validateCondition(data.getCondition());
    validateNonDefaultPresenceOrThrow(
        data, RateLimitingRuleData.THRESHOLD_ACTION_CONFIGS_FIELD_NUMBER);
    data.getThresholdActionConfigsList().forEach(this::validateThresholdActionConfig);
    validateRuleConfigScope(data.getRuleConfigScope());
  }

  private void validateRuleConfigScope(RuleConfigScope scope) {
    if (scope.hasEnvironmentScope()) {
      validateNonDefaultPresenceOrThrow(
          scope.getEnvironmentScope(), EnvironmentScope.ENVIRONMENT_IDS_FIELD_NUMBER);
      if (scope.getEnvironmentScope().getEnvironmentIdsList().stream().anyMatch(String::isEmpty)) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Environment id should not be empty string.")
            .asRuntimeException();
      }
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
      case USER_ID_CONDITION:
        validateUserIdCondition(leafCondition.getUserIdCondition());
        break;
      case EMAIL_DOMAIN_CONDITION:
        validateEmailDomainCondition(leafCondition.getEmailDomainCondition());
        break;
      case USER_AGENT_CONDITION:
        validateUserAgentCondition(leafCondition.getUserAgentCondition());
        break;
      case IP_CONNECTION_TYPE_CONDITION:
        validateIpConnectionTypeCondition(leafCondition.getIpConnectionTypeCondition());
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
    if (stringCondition.getOperator() == MatchOperator.MATCH_OPERATOR_MATCHES_REGEX
        || stringCondition.getOperator() == MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX) {
      validateRegex(stringCondition.getValue());
    }
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
    if (regionCondition.getRegionsList().isEmpty()
        && regionCondition.getRegionIdentifiersList().isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "At least one region value should be provided for region condition : %s",
              regionCondition));
    }

    regionCondition
        .getRegionsList()
        .forEach(
            region -> {
              if (region.isEmpty()) {
                throwInvalidArgumentException(
                    String.format(
                        "Region value cannot be empty for region condition : %s", regionCondition));
              }
            });

    regionCondition
        .getRegionIdentifiersList()
        .forEach(
            region -> {
              if (region.getCountryIsoCode().isEmpty()) {
                throwInvalidArgumentException(
                    String.format(
                        "Region value cannot be empty for region condition : %s", regionCondition));
              }
            });
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

  private void validateUserIdCondition(UserIdCondition userIdCondition) {
    List<String> actorEntityIds = userIdCondition.getActorEntityIdsList();
    List<String> userIdRegexes = userIdCondition.getUserIdRegexesList();
    if (actorEntityIds.isEmpty() && userIdRegexes.isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(userIdCondition), printMessage(userIdCondition)));
    }

    if (!userIdRegexes.isEmpty()) {
      userIdRegexes.forEach(this::validateRegex);
    }
  }

  private void validateEmailDomainCondition(EmailDomainCondition emailDomainCondition) {
    List<String> emailDomains = emailDomainCondition.getEmailDomainsList();
    List<String> emailRegexes = emailDomainCondition.getEmailRegexesList();
    if (emailDomains.isEmpty() && emailRegexes.isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(emailDomainCondition), printMessage(emailDomainCondition)));
    }

    if (!emailRegexes.isEmpty()) {
      emailRegexes.forEach(this::validateRegex);
    }
  }

  private void validateUserAgentCondition(UserAgentCondition userAgentCondition) {
    List<String> userAgents = userAgentCondition.getUserAgentsList();
    List<String> userAgentRegexes = userAgentCondition.getUserAgentRegexesList();
    if (userAgents.isEmpty() && userAgentRegexes.isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(userAgentCondition), printMessage(userAgentCondition)));
    }

    if (!userAgentRegexes.isEmpty()) {
      userAgentRegexes.forEach(this::validateRegex);
    }
  }

  private void validateIpConnectionTypeCondition(
      IpConnectionTypeCondition ipConnectionTypeCondition) {
    validateNonDefaultPresenceOrThrow(
        ipConnectionTypeCondition, IpConnectionTypeCondition.IP_CONNECTION_TYPES_FIELD_NUMBER);
    ipConnectionTypeCondition.getIpConnectionTypesList().forEach(this::validateIpConnectionType);
  }

  private void validateIpLocationType(IpLocationType ipLocationType) {
    if (ipLocationType.equals(IpLocationType.IP_LOCATION_TYPE_UNSPECIFIED)) {
      throwInvalidArgumentException("Invalid IP Location Type");
    }
  }

  private void validateIpConnectionType(IpConnectionType ipConnectionType) {
    if (ipConnectionType.equals(IpConnectionType.IP_CONNECTION_TYPE_UNSPECIFIED)
        || ipConnectionType.equals(IpConnectionType.UNRECOGNIZED)) {
      throwInvalidArgumentException(
          String.format("Invalid IP Connection Type : %s", ipConnectionType));
    }
  }

  private void validateCompositeCondition(CompositeCondition compositeCondition) {
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.CHILDREN_FIELD_NUMBER);
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

    boolean userAggregationAcrossAllPresent =
        thresholdActionConfig.getResourceAccessThresholdConfigsList().stream()
            .map(ResourceAccessThresholdConfig::getUserAggregateType)
            .anyMatch(
                userAggregateType ->
                    userAggregateType.equals(UserAggregateType.USER_AGGREGATE_TYPE_ACROSS_USERS));

    thresholdActionConfig
        .getActionsList()
        .forEach(action -> validateAction(action, userAggregationAcrossAllPresent));
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
      case DYNAMIC_THRESHOLD_CONFIG:
        validateDynamicThresholdConfig(resourceAccessThresholdConfig.getDynamicThresholdConfig());
        if (!resourceAccessThresholdConfig
                .getUserAggregateType()
                .equals(UserAggregateType.USER_AGGREGATE_TYPE_PER_USER)
            || !resourceAccessThresholdConfig
                .getApiAggregateType()
                .equals(ApiAggregateType.API_AGGREGATE_TYPE_PER_ENDPOINT)) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  "Dynamic threshold supports aggregation only on per user and per api endpoint")
              .asRuntimeException();
        }
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
    if (rollingWindowThresholdConfig.getCountAllowed() < 0) {
      throwInvalidArgumentException(
          "Count allowed should be greater than or equal to 0 for rolling threshold config");
    }
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
    validateNonDefaultPresenceOrThrow(
        valueBasedThresholdConfig,
        ResourceAccessThresholdConfig.ValueBasedThresholdConfig.VALUE_TYPE_FIELD_NUMBER);
  }

  private void validateDynamicThresholdConfig(
      ResourceAccessThresholdConfig.DynamicThresholdConfig dynamicThresholdConfig) {
    validateNonDefaultPresenceOrThrow(
        dynamicThresholdConfig,
        ResourceAccessThresholdConfig.DynamicThresholdConfig
            .PERCENT_EXCEEDING_MEAN_ALLOWED_FIELD_NUMBER);
    if (!dynamicThresholdConfig.hasMeanCalculationDuration()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Dynamic threshold config does not have valid mean calculation duration")
          .asRuntimeException();
    }
    if (!dynamicThresholdConfig.hasDuration()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Dynamic threshold config does not have valid duration")
          .asRuntimeException();
    }
  }

  private void validateRegex(String regexPattern) {
    if (regexPattern.startsWith(UTF_8_REGEX_PREFIX)) {
      return;
    }
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern.compile(regexPattern);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid Regex Value for the rate limit rule expression", e);
    }
  }

  private void validateAction(Action action, boolean aggregateAcrossAllUsersPresent) {
    switch (action.getActionCase()) {
      case ALERT:
      case BLOCK:
        if (aggregateAcrossAllUsersPresent) {
          throwInvalidArgumentException("Block action unsupported on aggregation across users");
        }
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
