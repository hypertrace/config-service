package ai.traceable.attribute.resolution.config.service.v1.validation;

import static ai.traceable.attribute.resolution.config.service.v1.KeyLocation.KEY_LOCATION_URL_PATH;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_CONTAINS;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_ENDS_WITH;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_EQUALS;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_NOT_CONTAINS;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_NOT_EQUALS;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH;
import static ai.traceable.attribute.resolution.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_STARTS_WITH;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.attribute.resolution.config.service.v1.Action;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfigData;
import ai.traceable.attribute.resolution.config.service.v1.ConfigScope;
import ai.traceable.attribute.resolution.config.service.v1.CreateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.DeleteAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.DynamicAction;
import ai.traceable.attribute.resolution.config.service.v1.EnvironmentScope;
import ai.traceable.attribute.resolution.config.service.v1.Filter;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsFilter;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsRequest;
import ai.traceable.attribute.resolution.config.service.v1.KeyFilter;
import ai.traceable.attribute.resolution.config.service.v1.KeyLocation;
import ai.traceable.attribute.resolution.config.service.v1.LogicalFilter;
import ai.traceable.attribute.resolution.config.service.v1.MatchGroupOperation;
import ai.traceable.attribute.resolution.config.service.v1.RelationalFilter;
import ai.traceable.attribute.resolution.config.service.v1.RelationalOperator;
import ai.traceable.attribute.resolution.config.service.v1.ScopedCondition;
import ai.traceable.attribute.resolution.config.service.v1.ServiceScope;
import ai.traceable.attribute.resolution.config.service.v1.StaticAction;
import ai.traceable.attribute.resolution.config.service.v1.UpdateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.ValueFilter;
import ai.traceable.attribute.resolution.config.service.v1.manager.AttributeResolutionConfigManager;
import ai.traceable.config.utils.RegexValidator;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class AttributeResolutionConfigValidatorImpl implements AttributeResolutionConfigValidator {
  private static final List<KeyLocation> NON_VALUE_FILTER_KEY_LOCATION =
      List.of(KEY_LOCATION_URL_PATH);
  private static final List<RelationalOperator> ALLOWED_URL_OPERATORS =
      List.of(
          RELATIONAL_OPERATOR_EQUALS,
          RELATIONAL_OPERATOR_NOT_EQUALS,
          RELATIONAL_OPERATOR_CONTAINS,
          RELATIONAL_OPERATOR_STARTS_WITH,
          RELATIONAL_OPERATOR_ENDS_WITH,
          RELATIONAL_OPERATOR_REGEX_MATCH,
          RELATIONAL_OPERATOR_NOT_CONTAINS);

  private final AttributeResolutionConfigManager manager;

  @Override
  public void validateOrThrow(
      RequestContext context, GetAttributeResolutionConfigsRequest request) {
    validateRequestContextOrThrow(context);
  }

  @Override
  public void validateOrThrow(
      RequestContext context, CreateAttributeResolutionConfigRequest request) {
    validateRequestContextOrThrow(context);
    validateAttributeResolutionConfigData(request.getData());
    validateDuplicateConfigName(context, request.getData().getName());
  }

  @Override
  public void validateOrThrow(
      RequestContext context, UpdateAttributeResolutionConfigRequest request) {
    validateRequestContextOrThrow(context);
    validateAttributeResolutionConfig(request.getConfig());
    validateDuplicateConfigName(context, request.getConfig());
  }

  @Override
  public void validateOrThrow(
      RequestContext context, DeleteAttributeResolutionConfigRequest request) {
    validateRequestContextOrThrow(context);
    validateNonDefaultPresenceOrThrow(
        request, DeleteAttributeResolutionConfigRequest.ID_FIELD_NUMBER);
  }

  private void validateAttributeResolutionConfig(AttributeResolutionConfig config) {
    if (config.getId().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "ID should not be empty or blank in attribute resolution config : %s", config))
          .asRuntimeException();
    }
    validateAttributeResolutionConfigData(config.getData());
  }

  @Override
  public void validateAttributeResolutionConfigData(AttributeResolutionConfigData data) {
    if (data.getName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Name should not be empty or blank in attribute resolution config data : %s",
                  data))
          .asRuntimeException();
    }
    if (data.getAttributeKey().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Attribute key should not be empty or blank in attribute resolution config data : %s",
                  data))
          .asRuntimeException();
    }
    if (data.getEntityType().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Entity type should not be empty or blank in attribute resolution config data : %s",
                  data))
          .asRuntimeException();
    }
    if (data.getScopedConditionsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Scope conditions list should not be empty in attribute resolution config data : %s",
                  data))
          .asRuntimeException();
    }
    data.getScopedConditionsList().forEach(this::validateScopeCondition);
  }

  private void validateScopeCondition(ScopedCondition scopedCondition) {
    validateScope(scopedCondition.getScope());
    validateFilter(scopedCondition.getFilter());
    validateAction(scopedCondition.getAction());
  }

  private void validateAction(Action action) {
    switch (action.getActionCase()) {
      case STATIC_ACTION:
        validateStaticAction(action.getStaticAction());
        break;
      case DYNAMIC_ACTION:
        validateDynamicAction(action.getDynamicAction());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Invalid action case : %s", action.getActionCase()))
            .asRuntimeException();
    }
  }

  private void validateDynamicAction(DynamicAction dynamicAction) {
    validateKeyFilter(dynamicAction.getKeyFilter());
    switch (dynamicAction.getDynamicValueActionCase()) {
      case NO_OPERATION:
        break;
      case MATCH_GROUP_OPERATION:
        validateMatchGroupOperation(dynamicAction.getMatchGroupOperation());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid dynamic value action case : %s",
                    dynamicAction.getDynamicValueActionCase()))
            .asRuntimeException();
    }
  }

  private void validateMatchGroupOperation(MatchGroupOperation matchGroupOperation) {
    if (matchGroupOperation.getMatchGroup().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Match group should not be empty or blank in match group operation : %s",
                  matchGroupOperation))
          .asRuntimeException();
    }
    if (matchGroupOperation.getMatchGroupRegex().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Match group regex should not be empty or blank in match group operation : %s",
                  matchGroupOperation))
          .asRuntimeException();
    }
    if (!RegexValidator.validateRegex(matchGroupOperation.getMatchGroupRegex()).isOk()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid match group regex in match group operation : %s", matchGroupOperation))
          .asRuntimeException();
    }
  }

  private void validateStaticAction(StaticAction staticAction) {
    if (!staticAction.hasValue()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Value should be present in static action : %s", staticAction))
          .asRuntimeException();
    }
  }

  private void validateFilter(Filter filter) {
    switch (filter.getFilterCase()) {
      case RELATIONAL_FILTER:
        validateRelationalFilter(filter.getRelationalFilter());
        break;
      case LOGICAL_FILTER:
        validateLogicalFilter(filter.getLogicalFilter());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Invalid filter case : %s", filter.getFilterCase()))
            .asRuntimeException();
    }
  }

  private void validateRelationalFilter(RelationalFilter relationalFilter) {
    validateKeyFilter(relationalFilter.getKeyFilter());
    if (!NON_VALUE_FILTER_KEY_LOCATION.contains(relationalFilter.getKeyFilter().getLocation())) {
      validateValueFilter(relationalFilter.getValueFilter());
    }
  }

  private void validateValueFilter(ValueFilter valueFilter) {
    validateNonDefaultPresenceOrThrow(valueFilter, ValueFilter.OPERATOR_FIELD_NUMBER);
    if (!valueFilter.hasValue()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Value should be present in value filter : %s", valueFilter))
          .asRuntimeException();
    }
  }

  private void validateKeyFilter(KeyFilter keyFilter) {
    validateNonDefaultPresenceOrThrow(keyFilter, KeyFilter.LOCATION_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(keyFilter, KeyFilter.OPERATOR_FIELD_NUMBER);
    if (!keyFilter.hasValue()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Value should be present in key filter : %s", keyFilter))
          .asRuntimeException();
    }

    if (keyFilter.getLocation().equals(KEY_LOCATION_URL_PATH)
        && !ALLOWED_URL_OPERATORS.contains(keyFilter.getOperator())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid operator %s for key location url path", keyFilter.getOperator()))
          .asRuntimeException();
    }
  }

  private void validateLogicalFilter(LogicalFilter logicalFilter) {
    validateNonDefaultPresenceOrThrow(logicalFilter, LogicalFilter.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(logicalFilter, LogicalFilter.FILTERS_FIELD_NUMBER);
    logicalFilter.getFiltersList().forEach(this::validateFilter);
  }

  private void validateScope(ConfigScope scope) {
    switch (scope.getScopeCase()) {
      case CUSTOMER_SCOPE:
        break;
      case ENVIRONMENT_SCOPE:
        validateEnvironmentScope(scope.getEnvironmentScope());
        break;
      case SERVICE_SCOPE:
        validateServiceScope(scope.getServiceScope());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Invalid config scope case : %s", scope.getScopeCase()))
            .asRuntimeException();
    }
  }

  private void validateServiceScope(ServiceScope serviceScope) {
    if (serviceScope.getServiceIdsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Service ids list should not be empty in service scope : %s", serviceScope))
          .asRuntimeException();
    }

    if (serviceScope.hasEnvironmentScope()) {
      validateEnvironmentScope(serviceScope.getEnvironmentScope());
    }
  }

  private void validateEnvironmentScope(EnvironmentScope environmentScope) {
    if (environmentScope.getEnvironmentIdsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Environment ids list should not be empty in environment scope : %s",
                  environmentScope))
          .asRuntimeException();
    }
  }

  private void validateDuplicateConfigName(RequestContext context, String configName) {
    Optional<AttributeResolutionConfig> existingConfigWithSameName =
        findConfigByName(context, configName);
    if (existingConfigWithSameName.isPresent()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Attribute resolution config with name %s already exists", configName))
          .asRuntimeException();
    }
  }

  private void validateDuplicateConfigName(
      RequestContext context, AttributeResolutionConfig config) {
    Optional<AttributeResolutionConfig> existingConfigWithSameName =
        findConfigByName(context, config.getData().getName());
    if (existingConfigWithSameName.isPresent()
        && !existingConfigWithSameName.get().getId().equals(config.getId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Attribute resolution config with name %s already exists",
                  config.getData().getName()))
          .asRuntimeException();
    }
  }

  private Optional<AttributeResolutionConfig> findConfigByName(
      RequestContext context, String configName) {
    List<AttributeResolutionConfig> existingConfigs =
        manager.getAttributeResolutionConfigs(
            context, GetAttributeResolutionConfigsFilter.getDefaultInstance());
    return existingConfigs.stream()
        .filter(config -> config.getData().getName().equals(configName))
        .findFirst();
  }
}
