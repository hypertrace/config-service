package ai.traceable.detection.exclusion.config.service.v1.rules;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.EntityScope;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionType;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationType;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationSeverity;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadataMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.LabelScope;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchOperator;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.Type;
import ai.traceable.detection.exclusion.config.service.v1.UserIdCondition;
import ai.traceable.platform.utils.ip.IpValidationUtils;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DetectionExclusionConditionValidator {

  void validateRuleCondition(DetectionExclusionCondition condition) {
    switch (condition.getConditionCase()) {
      case SCOPE_CONDITION:
        validateScopeCondition(condition.getScopeCondition());
        break;
      case ATTRIBUTE_MATCH_CONDITION:
        validateSpanAttributeMatchCondition(condition.getAttributeMatchCondition());
        break;
      case IP_LOCATION_TYPE_CONDITION:
        validateIpLocationTypeCondition(condition.getIpLocationTypeCondition());
        break;
      case IP_REPUTATION_CONDITION:
        validateIpReputationCondition(condition.getIpReputationCondition());
        break;
      case USER_ID_CONDITION:
        validateUserIdCondition(condition.getUserIdCondition());
        break;
      case EVENT_CONDITION:
        validateEventCondition(condition.getEventCondition());
        break;
      case ANOMALOUS_ATTRIBUTE_CONDITION:
        validateAnomalousAttributeCondition(condition.getAnomalousAttributeCondition());
        break;
      case SOURCE_SCOPE_CONDITION:
        validateScopeCondition(condition.getSourceScopeCondition());
        break;
      case SOURCE_ANOMALOUS_ATTRIBUTE_MATCH_CONDITION:
        validateAnomalousAttributeCondition(condition.getSourceAnomalousAttributeMatchCondition());
        break;
      case IP_ADDRESS_CONDITION:
        validateIpAddressCondition(condition.getIpAddressCondition());
        break;
      case IP_CONNECTION_TYPE_CONDITION:
        validateIpConnectionTypeCondition(condition.getIpConnectionTypeCondition());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid detection exclusion condition type : %s",
                    condition.getConditionCase()))
            .asRuntimeException();
    }
  }

  private void validateIpConnectionTypeCondition(
      IpConnectionTypeCondition ipConnectionTypeCondition) {
    validateNonDefaultPresenceOrThrow(
        ipConnectionTypeCondition, IpConnectionTypeCondition.IP_CONNECTION_TYPES_FIELD_NUMBER);
    ipConnectionTypeCondition.getIpConnectionTypesList().forEach(this::validateIpConnectionType);
  }

  private void validateIpConnectionType(IpConnectionType ipConnectionType) {
    if (ipConnectionType.equals(IpConnectionType.IP_CONNECTION_TYPE_UNSPECIFIED)
        || ipConnectionType.equals(IpConnectionType.UNRECOGNIZED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid IP Connection Type : %s", ipConnectionType))
          .asRuntimeException();
    }
  }

  private void validateScopeCondition(ScopeCondition condition) {
    switch (condition.getScopeCase()) {
      case ENTITY_SCOPE:
        validateEntityScope(condition.getEntityScope());
        break;
      case LABEL_SCOPE:
        validateLabelScope(condition.getLabelScope());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Invalid scopeConditionCase %s", condition.getScopeCase()))
            .asRuntimeException();
    }
  }

  private void validateEntityScope(EntityScope entityScope) {
    validateNonDefaultPresenceOrThrow(entityScope, EntityScope.ENTITY_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(entityScope, EntityScope.ENTITY_IDS_FIELD_NUMBER);
  }

  private void validateLabelScope(LabelScope labelScope) {
    validateNonDefaultPresenceOrThrow(labelScope, LabelScope.LABEL_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(labelScope, LabelScope.LABEL_IDS_FIELD_NUMBER);
  }

  private void validateSpanAttributeMatchCondition(SpanAttributeMatchCondition condition) {
    if (!condition.hasKeyMatchCondition() || !condition.hasValueMatchCondition()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid spanAttributeMatchCondition for detection exclusion rule :%n %s",
                  printMessage(condition)))
          .asRuntimeException();
    }

    KeyMetadataMatchCondition keyMetadataMatchCondition = condition.getKeyMatchCondition();
    validateNonDefaultPresenceOrThrow(
        keyMetadataMatchCondition, KeyMetadataMatchCondition.METADATA_FIELD_NUMBER);
    if (keyMetadataMatchCondition.hasMatchCondition()) {
      validateMatchCondition(keyMetadataMatchCondition.getMatchCondition());
    }

    validateMatchCondition(condition.getValueMatchCondition());
  }

  private void validateIpLocationTypeCondition(IpLocationTypeCondition condition) {
    validateNonDefaultPresenceOrThrow(
        condition, IpLocationTypeCondition.IP_LOCATION_TYPES_FIELD_NUMBER);
    condition.getIpLocationTypesList().forEach(this::validateIpLocationType);
  }

  private void validateIpLocationType(IpLocationType ipLocationType) {
    if (ipLocationType.equals(IpLocationType.IP_LOCATION_TYPE_UNSPECIFIED)
        || ipLocationType.equals(IpLocationType.UNRECOGNIZED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid IP location type : %s", ipLocationType))
          .asRuntimeException();
    }
  }

  private void validateIpReputationCondition(IpReputationCondition condition) {
    switch (condition.getReputationCase()) {
      case MAX_IP_REPUTATION_SCORE:
        if (condition.getMaxIpReputationScore() > 100 || condition.getMaxIpReputationScore() < 0) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  "IP reputation score should be >= 0 and <= 100 for ipReputationCondition")
              .asRuntimeException();
        }
        break;
      case MAX_IP_REPUTATION_SEVERITY:
        IpReputationSeverity ipReputationSeverity = condition.getMaxIpReputationSeverity();
        if (ipReputationSeverity.equals(IpReputationSeverity.IP_REPUTATION_SEVERITY_UNSPECIFIED)
            || ipReputationSeverity.equals(IpReputationSeverity.UNRECOGNIZED)) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format("Invalid IP reputation severity : %s", ipReputationSeverity))
              .asRuntimeException();
        }
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid ipReputationCondition for detection exclusion rule :%n %s",
                    printMessage(condition)))
            .asRuntimeException();
    }
  }

  private void validateUserIdCondition(UserIdCondition condition) {
    List<String> actorEntityIds = condition.getActorEntityIdsList();
    List<String> userIdRegexes = condition.getUserIdRegexesList();
    if (userIdRegexes.isEmpty() && actorEntityIds.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid userIdCondition for detection exclusion rule :%n %s",
                  printMessage(condition)))
          .asRuntimeException();
    }

    if (!userIdRegexes.isEmpty()) {
      userIdRegexes.forEach(this::validateRegex);
    }
  }

  private void validateEventCondition(EventCondition condition) {
    List<CustomRuleEvent> customRuleEvents = condition.getCustomRuleEventsList();
    List<SystemDefinedEvent> systemDefinedEvents = condition.getSystemDefinedEventsList();
    if (customRuleEvents.isEmpty() && systemDefinedEvents.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid eventCondition for detection exclusion rule :%n %s",
                  printMessage(condition)))
          .asRuntimeException();
    }

    if (!customRuleEvents.isEmpty()) {
      customRuleEvents.forEach(this::validateCustomRuleEvent);
    }

    if (!systemDefinedEvents.isEmpty()) {
      systemDefinedEvents.forEach(this::validateSystemDefinedEvent);
    }
  }

  private void validateCustomRuleEvent(CustomRuleEvent customRuleEvent) {
    CustomRuleFamily customRuleFamily = customRuleEvent.getRuleFamily();
    if (customRuleFamily.equals(CustomRuleFamily.CUSTOM_RULE_FAMILY_UNSPECIFIED)
        || customRuleFamily.equals(CustomRuleFamily.UNRECOGNIZED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid custom rule family : %s", customRuleFamily))
          .asRuntimeException();
    }
    if (customRuleEvent.hasRuleId() && customRuleEvent.getRuleId().trim().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("RuleId provided is empty for custom rule family %s", customRuleFamily))
          .asRuntimeException();
    }
  }

  private void validateSystemDefinedEvent(SystemDefinedEvent systemDefinedEvent) {
    SystemDefinedEventFamily systemDefinedEventFamily = systemDefinedEvent.getEventFamily();
    if (systemDefinedEventFamily.equals(
            SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_UNSPECIFIED)
        || systemDefinedEventFamily.equals(SystemDefinedEventFamily.UNRECOGNIZED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Invalid system defined event family : %s", systemDefinedEventFamily))
          .asRuntimeException();
    }

    Optional<String> typeId = getSystemDefinedEventTypeId(systemDefinedEvent);
    if (typeId.isPresent() && typeId.get().trim().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "TypeId provided is empty for system defined event family %s",
                  systemDefinedEventFamily))
          .asRuntimeException();
    }

    if (systemDefinedEvent.hasDescriptionMatchCondition()) {
      validateMatchCondition(systemDefinedEvent.getDescriptionMatchCondition());
    }
  }

  private Optional<String> getSystemDefinedEventTypeId(SystemDefinedEvent systemDefinedEvent) {
    switch (systemDefinedEvent.getTypeIdCase()) {
      case EVENT_TYPE_ID:
        return Optional.of(systemDefinedEvent.getEventTypeId());
      case EVENT_SUB_TYPE_ID:
        return Optional.of(systemDefinedEvent.getEventSubTypeId());
      default:
        return Optional.empty();
    }
  }

  private void validateAnomalousAttributeCondition(AnomalousAttributeCondition condition) {
    if (!condition.hasKeyMatchCondition() && !condition.hasValueMatchCondition()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid anomalousAttributeCondition for detection exclusion rule :%n %s",
                  printMessage(condition)))
          .asRuntimeException();
    }
    if (condition.hasKeyMatchCondition()) {
      validateMatchCondition(condition.getKeyMatchCondition());
    }
    if (condition.hasValueMatchCondition()) {
      validateMatchCondition(condition.getValueMatchCondition());
    }
    validateNonDefaultPresenceOrThrow(
        condition, AnomalousAttributeCondition.OBSERVED_TYPES_FIELD_NUMBER);
    condition.getObservedTypesList().forEach(this::validateType);
    validateNonDefaultPresenceOrThrow(
        condition, AnomalousAttributeCondition.LEARNT_TYPES_FIELD_NUMBER);
    condition.getLearntTypesList().forEach(this::validateType);
  }

  private void validateType(Type type) {
    if (type.equals(Type.TYPE_UNSPECIFIED) || type.equals(Type.UNRECOGNIZED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid type : %s", type))
          .asRuntimeException();
    }
  }

  private void validateMatchCondition(MatchCondition matchCondition) {
    validateNonDefaultPresenceOrThrow(matchCondition, MatchCondition.OPERATOR_FIELD_NUMBER);

    if (!matchCondition.hasValue()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Match condition should have a valid value")
          .asRuntimeException();
    }

    if (matchCondition.getOperator().equals(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
        || matchCondition.getOperator().equals(MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)) {
      Value value = matchCondition.getValue();
      if (!value.hasStringValue()
          && !(value.hasListValue() && listValueContainsOnlyStrings(value.getListValue()))) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Match condition value should be string or list of strings for regex matching")
            .asRuntimeException();
      }
      if (value.hasStringValue()) {
        validateRegex(matchCondition.getValue().getStringValue());
      }
      if (value.hasListValue()) {
        value.getListValue().getValuesList().stream()
            .map(Value::getStringValue)
            .forEach(this::validateRegex);
      }
    }
  }

  private void validateIpAddressCondition(IpAddressCondition condition) {
    if (condition.getCidrIpRangesList().isEmpty() && condition.getIpAddressesList().isEmpty()) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Invalid ipAddressCondition for detection exclusion rule :%n %s",
                  printMessage(condition)))
          .asRuntimeException();
    }

    if (!condition.getIpAddressesList().stream().allMatch(IpValidationUtils::isValidIpAddress)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("IpAddressCondition should have valid IP addresses")
          .asRuntimeException();
    }

    if (!condition.getCidrIpRangesList().stream().allMatch(IpValidationUtils::isValidSubnet)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("IpAddressCondition should have valid CIDR IP ranges")
          .asRuntimeException();
    }
  }

  private boolean listValueContainsOnlyStrings(ListValue listValue) {
    return listValue.getValuesList().stream().allMatch(Value::hasStringValue);
  }

  private void validateRegex(String regexPattern) {
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern.compile(regexPattern);
    } catch (PatternSyntaxException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid Regex Value : %s", regexPattern))
          .asRuntimeException();
    }
  }
}
