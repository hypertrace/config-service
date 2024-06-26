package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.MatchOperator.MATCH_OPERATOR_GREATER_THAN;
import static ai.traceable.detection.exclusion.config.service.v1.MatchOperator.MATCH_OPERATOR_LESS_THAN;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition;
import ai.traceable.detection.exclusion.config.service.v1.AttributeValueType;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.EntityScope;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAbuseVelocityCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAsnCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionType;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationType;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpOrganisationCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationSeverity;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadataMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.LabelScope;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchOperator;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.UrlScope;
import ai.traceable.detection.exclusion.config.service.v1.UserIdCondition;
import ai.traceable.platform.utils.ip.IpValidationUtils;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
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
      case IP_ORGANISATION_CONDITION:
        validateIpOrganisationCondition(condition.getIpOrganisationCondition());
        break;
      case IP_ASN_CONDITION:
        validateIpAsnCondition(condition.getIpAsnCondition());
        break;
      case IP_ABUSE_VELOCITY_CONDITION:
        validateIpAbuseVelocityCondition(condition.getIpAbuseVelocityCondition());
        break;
      case REGION_CONDITION:
        validateRegionCondition(condition.getRegionCondition());
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid detection exclusion condition type : %s", condition.getConditionCase()));
    }
  }

  private void validateRegionCondition(RegionCondition regionCondition) {
    if (regionCondition.getRegionsList().isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "At least one region value should be provided for region condition : %s",
              regionCondition));
    }
    regionCondition
        .getRegionsList()
        .forEach(
            region -> {
              if (region.getCountryIsoCode().isBlank()) {
                throwInvalidArgumentException(
                    String.format(
                        "Region value cannot be empty for region condition : %s", regionCondition));
              }
            });
  }

  private void validateIpOrganisationCondition(IpOrganisationCondition ipOrganisationCondition) {
    RegexValidator.validateRegexes(ipOrganisationCondition.getIpOrganisationRegexesList());
  }

  private void validateIpAsnCondition(IpAsnCondition ipAsnCondition) {
    RegexValidator.validateRegexes(ipAsnCondition.getIpAsnRegexesList());
  }

  private void validateIpAbuseVelocityCondition(IpAbuseVelocityCondition ipAbuseVelocityCondition) {
    validateNonDefaultPresenceOrThrow(
        ipAbuseVelocityCondition, IpAbuseVelocityCondition.MAX_IP_ABUSE_VELOCITY_FIELD_NUMBER);
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
      throwInvalidArgumentException(
          String.format("Invalid IP Connection Type : %s", ipConnectionType));
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
      case URL_SCOPE:
        validateUrlScope(condition.getUrlScope());
        break;
      default:
        throwInvalidArgumentException(
            String.format("Invalid scopeConditionCase %s", condition.getScopeCase()));
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

  private void validateUrlScope(UrlScope urlScope) {
    validateNonDefaultPresenceOrThrow(urlScope, UrlScope.URL_REGEXES_FIELD_NUMBER);
    RegexValidator.validateRegexesWithNonWide(urlScope.getUrlRegexesList());
  }

  private void validateSpanAttributeMatchCondition(SpanAttributeMatchCondition condition) {
    if (!condition.hasKeyMatchCondition() || !condition.hasValueMatchCondition()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid spanAttributeMatchCondition for detection exclusion rule :%n %s",
              printMessage(condition)));
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
      throwInvalidArgumentException(String.format("Invalid IP location type : %s", ipLocationType));
    }
  }

  private void validateIpReputationCondition(IpReputationCondition condition) {
    switch (condition.getReputationCase()) {
      case MAX_IP_REPUTATION_SCORE:
        if (condition.getMaxIpReputationScore() > 100 || condition.getMaxIpReputationScore() < 0) {
          throwInvalidArgumentException(
              "IP reputation score should be >= 0 and <= 100 for ipReputationCondition");
        }
        break;
      case MAX_IP_REPUTATION_SEVERITY:
        IpReputationSeverity ipReputationSeverity = condition.getMaxIpReputationSeverity();
        if (ipReputationSeverity.equals(IpReputationSeverity.IP_REPUTATION_SEVERITY_UNSPECIFIED)
            || ipReputationSeverity.equals(IpReputationSeverity.UNRECOGNIZED)) {
          throwInvalidArgumentException(
              String.format("Invalid IP reputation severity : %s", ipReputationSeverity));
        }
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid ipReputationCondition for detection exclusion rule :%n %s",
                printMessage(condition)));
    }
  }

  private void validateUserIdCondition(UserIdCondition condition) {
    List<String> actorEntityIds = condition.getActorEntityIdsList();
    List<String> userIdRegexes = condition.getUserIdRegexesList();
    List<String> userIds = condition.getUserIdsList();
    if (userIdRegexes.isEmpty() && actorEntityIds.isEmpty() && userIds.isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid userIdCondition for detection exclusion rule :%n %s",
              printMessage(condition)));
    }

    if (!userIdRegexes.isEmpty()) {
      userIdRegexes.forEach(this::validateRegex);
    }
  }

  private void validateEventCondition(EventCondition condition) {
    List<CustomRuleEvent> customRuleEvents = condition.getCustomRuleEventsList();
    List<SystemDefinedEvent> systemDefinedEvents = condition.getSystemDefinedEventsList();
    if (customRuleEvents.isEmpty() && systemDefinedEvents.isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid eventCondition for detection exclusion rule :%n %s",
              printMessage(condition)));
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
      throwInvalidArgumentException(
          String.format("Invalid custom rule family : %s", customRuleFamily));
    }
    if (customRuleEvent.hasRuleId() && customRuleEvent.getRuleId().trim().isEmpty()) {
      throwInvalidArgumentException(
          String.format("RuleId provided is empty for custom rule family %s", customRuleFamily));
    }
  }

  private void validateSystemDefinedEvent(SystemDefinedEvent systemDefinedEvent) {
    SystemDefinedEventFamily systemDefinedEventFamily = systemDefinedEvent.getEventFamily();
    if (systemDefinedEventFamily.equals(
            SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_UNSPECIFIED)
        || systemDefinedEventFamily.equals(SystemDefinedEventFamily.UNRECOGNIZED)) {
      throwInvalidArgumentException(
          String.format("Invalid system defined event family : %s", systemDefinedEventFamily));
    }

    Optional<String> typeId = getSystemDefinedEventTypeId(systemDefinedEvent);
    if (typeId.isPresent() && typeId.get().trim().isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "TypeId provided is empty for system defined event family %s",
              systemDefinedEventFamily));
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
      throwInvalidArgumentException(
          String.format(
              "Invalid anomalousAttributeCondition for detection exclusion rule :%n %s",
              printMessage(condition)));
    }
    if (condition.hasKeyMatchCondition()) {
      validateMatchCondition(condition.getKeyMatchCondition());
    }
    if (condition.hasValueMatchCondition()) {
      validateMatchCondition(condition.getValueMatchCondition());
    }
    condition.getObservedTypesList().forEach(this::validateAttributeValueType);
    condition.getLearntTypesList().forEach(this::validateAttributeValueType);
  }

  private void validateAttributeValueType(AttributeValueType type) {
    if (type.equals(AttributeValueType.ATTRIBUTE_VALUE_TYPE_UNSPECIFIED)
        || type.equals(AttributeValueType.UNRECOGNIZED)) {
      throwInvalidArgumentException(String.format("Invalid type : %s", type));
    }
  }

  private void validateMatchCondition(MatchCondition matchCondition) {
    validateNonDefaultPresenceOrThrow(matchCondition, MatchCondition.OPERATOR_FIELD_NUMBER);

    if (!matchCondition.hasValue()) {
      throwInvalidArgumentException("Match condition should have a valid value");
    }

    if (matchCondition.getOperator().equals(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
        || matchCondition.getOperator().equals(MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)) {
      Value value = matchCondition.getValue();
      if (!value.hasStringValue()
          && !(value.hasListValue() && listValueContainsOnlyStrings(value.getListValue()))) {
        throwInvalidArgumentException(
            "Match condition value should be string or list of strings for regex matching");
      }
      if (isInvalidMathematicalOperation(matchCondition)) {
        throwInvalidArgumentException(
            String.format(
                "Numerical value should be present for match operator : %s",
                matchCondition.getOperator()));
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
    List<String> cidrIpRanges = condition.getCidrIpRangesList();
    List<String> ipAddresses = condition.getIpAddressesList();
    List<String> rawInputIpData = condition.getRawInputIpDataList();
    switch (condition.getIpAddressConditionType()) {
      case IP_ADDRESS_CONDITION_TYPE_ALL_INTERNAL:
      case IP_ADDRESS_CONDITION_TYPE_ALL_EXTERNAL:
        if (!cidrIpRanges.isEmpty() || !ipAddresses.isEmpty() || !rawInputIpData.isEmpty()) {
          throwInvalidArgumentException(
              String.format(
                  "RawInputIpData, cidrIpRanges and ipAddresses should be empty for ip address condition type : %s ",
                  condition.getIpAddressConditionType()));
        }
        break;
      default:
        validateIpAddressesAndRanges(condition, cidrIpRanges, ipAddresses, rawInputIpData);
    }
  }

  private void validateIpAddressesAndRanges(
      IpAddressCondition condition,
      List<String> cidrIpRanges,
      List<String> ipAddresses,
      List<String> rawInputIpData) {
    if (cidrIpRanges.isEmpty() && ipAddresses.isEmpty() && rawInputIpData.isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid ipAddressCondition for detection exclusion rule :%n %s",
              printMessage(condition)));
    }
    if ((!rawInputIpData.isEmpty()) && (!cidrIpRanges.isEmpty() || !ipAddresses.isEmpty())) {
      throwInvalidArgumentException(
          String.format(
              "IpAddressCondition should not have rawInputIpData and (cidrIpRanges or ipAddresses) simultaneously :%n %s",
              printMessage(condition)));
    }
    if (!ipAddresses.stream().allMatch(IpValidationUtils::isValidIpAddress)) {
      throwInvalidArgumentException("IpAddressCondition should have valid IP addresses");
    }
    if (!cidrIpRanges.stream().allMatch(IpValidationUtils::isValidSubnet)) {
      throwInvalidArgumentException("IpAddressCondition should have valid CIDR IP ranges");
    }
  }

  private boolean listValueContainsOnlyStrings(ListValue listValue) {
    return listValue.getValuesList().stream().allMatch(Value::hasStringValue);
  }

  private void validateRegex(String regexPattern) {
    Status status = RegexValidator.validateRegex(regexPattern);
    if (!status.isOk()) {
      throwInvalidArgumentException(String.format("Invalid Regex Value : %s", regexPattern));
    }
  }

  private void throwInvalidArgumentException(String description) {
    throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
  }

  private boolean isInvalidMathematicalOperation(MatchCondition matchCondition) {
    return (matchCondition.getOperator().equals(MATCH_OPERATOR_GREATER_THAN)
            || matchCondition.getOperator().equals(MATCH_OPERATOR_LESS_THAN))
        && !matchCondition.getValue().hasNumberValue();
  }
}
