package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.ratelimiting.config.service.v2.Action.ActionCase.ALLOW;
import static ai.traceable.ratelimiting.config.service.v2.Action.ActionCase.BLOCK;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.DataLocation;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import com.google.protobuf.ProtocolStringList;

public class TransactionActionConfigValidator {
  private final ValidatorUtils validatorUtils = new ValidatorUtils();

  public void validateRateLimitingRuleData(RateLimitingRuleData data) {
    if (!data.getThresholdActionConfigsList().isEmpty()) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Threshold condition should not be present in rate limiting rule : %n%s, if transaction action config is present",
              data));
    }
    if (!data.getCategory().equals(Category.CATEGORY_DATA_EXFILTRATION)) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Invalid category : %n%s if transaction action config is present",
              data.getCategory()));
    }
    Action.ActionCase actionCase = data.getTransactionActionConfig().getAction().getActionCase();
    if (actionCase.equals(ALLOW) || actionCase.equals(BLOCK)) {
      validateConditionForTransactionActionConfig(data.getCondition());
    }
  }

  private void validateConditionForTransactionActionConfig(Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        validateLeafConditionForTransactionActionConfig(condition.getLeafCondition());
        break;
      case COMPOSITE_CONDITION:
        validateCompositeConditionForTransactionBasedConfig(condition.getCompositeCondition());
        break;
      default:
        validatorUtils.throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s",
                validatorUtils.getName(condition), printMessage(condition)));
    }
  }

  private void validateCompositeConditionForTransactionBasedConfig(
      CompositeCondition compositeCondition) {
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.CHILDREN_FIELD_NUMBER);
    compositeCondition.getChildrenList().forEach(this::validateConditionForTransactionActionConfig);
  }

  private void validateLeafConditionForTransactionActionConfig(LeafCondition leafCondition) {
    switch (leafCondition.getConditionCase()) {
      case SCOPE_CONDITION:
        validateScopeConditionForTransactionActionConfig(leafCondition.getScopeCondition());
        break;
      case DATATYPE_CONDITION:
        validateDatatypeConditionForTransactionActionConfig(leafCondition.getDatatypeCondition());
        break;
      case REGION_CONDITION:
        validateRegionConditionForTransactionActionConfig(leafCondition.getRegionCondition());
        break;
      case IP_ADDRESS_CONDITION:
        validateIpAddressConditionForTransactionActionConfig(leafCondition.getIpAddressCondition());
        break;
      case KEY_VALUE_CONDITION:
        validateKeyValueConditionForTransactionActionConfig(leafCondition);
        break;
      case IP_LOCATION_TYPE_CONDITION:
        validatorUtils.validateLeafCondition(leafCondition);
        break;
      default:
        validatorUtils.throwInvalidArgumentException(
            String.format(
                "Invalid leaf condition : %s, for transaction action config", leafCondition));
    }
  }

  private void validateKeyValueConditionForTransactionActionConfig(LeafCondition condition) {
    switch (condition.getKeyValueCondition().getType()) {
      case TYPE_RESPONSE_BODY:
      case TYPE_RESPONSE_COOKIE:
      case TYPE_RESPONSE_HEADER:
      case TYPE_RESPONSE_BODY_PARAMETER:
      case TYPE_TAG:
        validatorUtils.throwInvalidArgumentException(
            String.format(
                "Invalid key value condition type for transaction action config : %s",
                condition.getKeyValueCondition()));
        break;
      default:
        validatorUtils.validateLeafCondition(condition);
    }
  }

  private void validateIpAddressConditionForTransactionActionConfig(
      IpAddressCondition ipAddressCondition) {
    if (ipAddressCondition.getExclude()) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Exclude should be set false when transaction action config is present : %s",
              ipAddressCondition));
    }
    validatorUtils.validateIpAddressCondition(ipAddressCondition);
  }

  private void validateRegionConditionForTransactionActionConfig(RegionCondition regionCondition) {
    if (regionCondition.getExclude()) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Exclude should be set false when transaction action config is present : %s",
              regionCondition));
    }
    validatorUtils.validateRegionCondition(regionCondition);
  }

  private void validateDatatypeMatchingForTransactionActionConfig(
      DatatypeCondition.DatatypeMatching datatypeMatching) {

    if (!datatypeMatching.hasRegexBasedMatching()) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Invalid datatype matching : %s for transaction action config", datatypeMatching));
    }
    if (datatypeMatching.getRegexBasedMatching().hasCustomMatchingLocation())
      validateCustomMatchingLocationKeyValueCondition(
          datatypeMatching.getRegexBasedMatching().getCustomMatchingLocation());
  }

  private void validateCustomMatchingLocationKeyValueCondition(
      KeyValueCondition keyValueCondition) {
    validateNonDefaultPresenceOrThrow(keyValueCondition, KeyValueCondition.TYPE_FIELD_NUMBER);
    if (keyValueCondition.hasValueCondition()) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Value condition should not be present in custom location matching : %s",
              keyValueCondition));
    }
    switch (keyValueCondition.getType()) {
      case TYPE_REQUEST_BODY:
        if (keyValueCondition.hasKeyCondition()) {
          validatorUtils.throwInvalidArgumentException(
              String.format(
                  "For type : %s%n key condition should not be present in custom location matching: %s%n",
                  keyValueCondition.getType(), keyValueCondition));
        }
        break;
      case TYPE_HOST:
      case TYPE_HTTP_METHOD:
      case TYPE_REQUEST_HEADER:
      case TYPE_REQUEST_COOKIE:
      case TYPE_QUERY_PARAMETER:
      case TYPE_REQUEST_BODY_PARAMETER:
        if (!keyValueCondition.hasKeyCondition()) {
          validatorUtils.throwInvalidArgumentException(
              String.format(
                  "For type : %s%n key condition should be present in custom location matching: %s%n",
                  keyValueCondition.getType(), keyValueCondition));
        }
        validatorUtils.validateStringCondition(keyValueCondition.getKeyCondition());
        break;
      default:
        if (keyValueCondition.hasKeyCondition()) {
          validatorUtils.throwInvalidArgumentException(
              String.format(
                  "Invalid type : %s%n for custom location matching: %s",
                  keyValueCondition.getType(), keyValueCondition));
        }
    }
  }

  private void validateDatatypeConditionForTransactionActionConfig(
      DatatypeCondition datatypeCondition) {
    if (!datatypeCondition.getDataLocation().equals(DataLocation.DATA_LOCATION_REQUEST)) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Data location should be request in datatype condition : %s for transaction action config",
              datatypeCondition));
    }
    if (!datatypeCondition.getDataSensitivityLevelsList().isEmpty()) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Data sensitivity level should not be present in datatype condition : %s for transaction action config",
              datatypeCondition));
    }
    if (datatypeCondition.getDatatypeIdsList().isEmpty()
        && datatypeCondition.getDatasetIdsList().isEmpty()) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Any one of datatype or dataset should be present in datatype condition : %s for transaction action config",
              datatypeCondition));
    }
    if (datatypeCondition.hasDatatypeMatching()) {
      validateDatatypeMatchingForTransactionActionConfig(datatypeCondition.getDatatypeMatching());
    }
  }

  private void validateScopeConditionForTransactionActionConfig(ScopeCondition scopeCondition) {
    if (scopeCondition.hasLabelScope()) {
      validatorUtils.throwInvalidArgumentException(
          String.format(
              "Invalid scope condition : %s for transaction action config", scopeCondition));
    }
    if (scopeCondition.hasEntityScope()) {
      if (!scopeCondition
          .getEntityScope()
          .getEntityType()
          .equals(ScopeCondition.EntityType.ENTITY_TYPE_SERVICE)) {
        validatorUtils.throwInvalidArgumentException(
            String.format(
                "Invalid scope condition : %s for transaction action config", scopeCondition));
      }
    }
    if (scopeCondition.hasUrlScope()) {
      ProtocolStringList urlRegexesList = scopeCondition.getUrlScope().getUrlRegexesList();
      if (urlRegexesList.isEmpty()) {
        validatorUtils.throwInvalidArgumentException(
            String.format(
                "Invalid scope condition : %s for transaction action config", scopeCondition));
      }
      validatorUtils.validateRegexes(urlRegexesList);
    }
  }
}
