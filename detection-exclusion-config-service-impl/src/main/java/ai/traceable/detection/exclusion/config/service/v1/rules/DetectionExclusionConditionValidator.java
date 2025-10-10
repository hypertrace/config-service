package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition.ConditionCase.ANOMALOUS_ATTRIBUTE_CONDITION;
import static ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition.ConditionCase.ATTRIBUTE_MATCH_CONDITION;
import static ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition.ConditionCase.EVENT_CONDITION;
import static ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition.ConditionCase.IP_ADDRESS_CONDITION;
import static ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION;
import static ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition.ConditionCase.REGION_CONDITION;
import static ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition.ConditionCase.SCOPE_CONDITION;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALERT;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALLOW;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_BLOCK;
import static ai.traceable.detection.exclusion.config.service.v1.IpLocationType.IP_LOCATION_TYPE_SCANNER;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_HOST;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_HTTP_METHOD;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_QUERY_PARAMETER;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_QUERY_PARAMS_COUNT;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_BODY;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_BODY_PARAMETER;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_BODY_SIZE;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_COOKIE;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_COOKIES_COUNT;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_HEADER;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_HEADERS_COUNT;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_BODY;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_BODY_SIZE;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_COOKIES_COUNT;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_HEADERS_COUNT;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_STATUS_CODE;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_URL;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_USER_AGENT;
import static ai.traceable.detection.exclusion.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS;
import static ai.traceable.detection.exclusion.config.service.v1.MatchOperator.MATCH_OPERATOR_GREATER_THAN;
import static ai.traceable.detection.exclusion.config.service.v1.MatchOperator.MATCH_OPERATOR_LESS_THAN;
import static ai.traceable.detection.exclusion.config.service.v1.MatchOperator.MATCH_OPERATOR_NOT_EQUAL;
import static ai.traceable.detection.exclusion.config.service.v1.ThreatActorIdentifier.THREAT_ACTOR_IDENTIFIER_UNSPECIFIED;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition;
import ai.traceable.detection.exclusion.config.service.v1.AttributeValueType;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.EmailDomainCondition;
import ai.traceable.detection.exclusion.config.service.v1.EntityScope;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
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
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadata;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadataMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.LabelScope;
import ai.traceable.detection.exclusion.config.service.v1.LhsRhsKeysCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchOperator;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition;
import ai.traceable.detection.exclusion.config.service.v1.RequestScannerTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.ThreatActorEvent;
import ai.traceable.detection.exclusion.config.service.v1.ThreatActorIdentifier;
import ai.traceable.detection.exclusion.config.service.v1.UrlScope;
import ai.traceable.detection.exclusion.config.service.v1.UserAgentCondition;
import ai.traceable.detection.exclusion.config.service.v1.UserIdCondition;
import ai.traceable.platform.utils.ip.IpValidationUtils;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DetectionExclusionConditionValidator {

  public static final Set<KeyMetadata> KEY_NULL_METADATA =
      Set.of(
          KEY_METADATA_URL,
          KEY_METADATA_HOST,
          KEY_METADATA_HTTP_METHOD,
          KEY_METADATA_USER_AGENT,
          KEY_METADATA_STATUS_CODE,
          KEY_METADATA_REQUEST_BODY,
          KEY_METADATA_RESPONSE_BODY,
          KEY_METADATA_REQUEST_BODY_SIZE,
          KEY_METADATA_RESPONSE_BODY_SIZE,
          KEY_METADATA_QUERY_PARAMS_COUNT,
          KEY_METADATA_REQUEST_HEADERS_COUNT,
          KEY_METADATA_RESPONSE_HEADERS_COUNT,
          KEY_METADATA_REQUEST_COOKIES_COUNT,
          KEY_METADATA_RESPONSE_COOKIES_COUNT);

  private static final Set<DetectionExclusionCondition.ConditionCase>
      VALID_CONDITIONS_FOR_EXCLUSION_TARGET_BLOCK_OR_ALLOW =
          Set.of(
              EVENT_CONDITION,
              SCOPE_CONDITION,
              IP_ADDRESS_CONDITION,
              IP_LOCATION_TYPE_CONDITION,
              REGION_CONDITION,
              ATTRIBUTE_MATCH_CONDITION,
              ANOMALOUS_ATTRIBUTE_CONDITION);

  private static final Set<KeyMetadata> VALID_METADATA_FOR_EXCLUSION_TARGET_BLOCK_OR_ALLOW =
      Set.of(
          KEY_METADATA_URL,
          KEY_METADATA_HOST,
          KEY_METADATA_HTTP_METHOD,
          KEY_METADATA_USER_AGENT,
          KEY_METADATA_REQUEST_BODY,
          KEY_METADATA_REQUEST_HEADER,
          KEY_METADATA_REQUEST_COOKIE,
          KEY_METADATA_QUERY_PARAMETER,
          KEY_METADATA_REQUEST_BODY_PARAMETER);

  private static final Set<MatchOperator>
      SUPPORTED_FIRST_LEVEL_OPERATORS_FOR_LHS_RHS_KEYS_CONDITION =
          Set.of(MATCH_OPERATOR_EQUALS, MATCH_OPERATOR_NOT_EQUAL);

  private static final Set<MatchOperator> UNSUPPORTED_OPERATORS_FOR_LHS_RHS_KEYS_CONDITION =
      Set.of(MATCH_OPERATOR_GREATER_THAN, MATCH_OPERATOR_LESS_THAN);

  void validateRuleCondition(
      List<ExclusionTarget> exclusionTargets, DetectionExclusionCondition condition) {
    boolean isBlockOrAllowTargetPresent =
        exclusionTargets.stream()
            .anyMatch(
                target ->
                    target.equals(EXCLUSION_TARGET_BLOCK) || target.equals(EXCLUSION_TARGET_ALLOW));
    boolean isAlertTargetPresent =
        exclusionTargets.stream().anyMatch(target -> target.equals(EXCLUSION_TARGET_ALERT));
    if (isBlockOrAllowTargetPresent
        && !VALID_CONDITIONS_FOR_EXCLUSION_TARGET_BLOCK_OR_ALLOW.contains(
            condition.getConditionCase())) {
      throwInvalidArgumentException(
          String.format(
              "Invalid detection exclusion condition type : %s with exclusion target set as block or allow",
              condition.getConditionCase()));
    }
    switch (condition.getConditionCase()) {
      case SCOPE_CONDITION:
        validateScopeCondition(isBlockOrAllowTargetPresent, condition.getScopeCondition());
        break;
      case ATTRIBUTE_MATCH_CONDITION:
        validateSpanAttributeMatchCondition(
            isBlockOrAllowTargetPresent, condition.getAttributeMatchCondition());
        break;
      case IP_LOCATION_TYPE_CONDITION:
        validateIpLocationTypeCondition(
            isBlockOrAllowTargetPresent, condition.getIpLocationTypeCondition());
        break;
      case IP_REPUTATION_CONDITION:
        validateIpReputationCondition(condition.getIpReputationCondition());
        break;
      case USER_ID_CONDITION:
        validateUserIdCondition(condition.getUserIdCondition());
        break;
      case EVENT_CONDITION:
        validateEventCondition(isAlertTargetPresent, condition.getEventCondition());
        break;
      case ANOMALOUS_ATTRIBUTE_CONDITION:
        validateAnomalousAttributeCondition(condition.getAnomalousAttributeCondition());
        break;
      case SOURCE_SCOPE_CONDITION:
        validateScopeCondition(isBlockOrAllowTargetPresent, condition.getSourceScopeCondition());
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
      case EMAIL_DOMAIN_CONDITION:
        validateEmailDomainCondition(condition.getEmailDomainCondition());
        break;
      case USER_AGENT_CONDITION:
        validateUserAgentCondition(condition.getUserAgentCondition());
        break;
      case REQUEST_SCANNER_TYPE_CONDITION:
        validateRequestScannerTypeCondition(condition.getRequestScannerTypeCondition());
        break;
      case LHS_RHS_KEYS_CONDITION:
        validateLhsRhsKeysCondition(condition.getLhsRhsKeysCondition());
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid detection exclusion condition type : %s", condition.getConditionCase()));
    }
  }

  private void validateLhsRhsKeysCondition(LhsRhsKeysCondition lhsRhsKeysCondition) {
    validateNonDefaultPresenceOrThrow(
        lhsRhsKeysCondition, LhsRhsKeysCondition.LHS_RHS_MATCH_OPERATOR_FIELD_NUMBER);
    if (!SUPPORTED_FIRST_LEVEL_OPERATORS_FOR_LHS_RHS_KEYS_CONDITION.contains(
        lhsRhsKeysCondition.getLhsRhsMatchOperator())) {
      throwInvalidArgumentException(
          "The first-level MatchOperator in LhsRhsKeysCondition must be either EQUALS or NOT_EQUAL.");
    }

    if (!lhsRhsKeysCondition.hasLhsKeyCondition() || !lhsRhsKeysCondition.hasRhsKeyCondition()) {
      throwInvalidArgumentException("Both LhsKeyCondition and RhsKeyCondition must be present.");
    }

    KeyMetadataMatchCondition lhsKeyMetadataMatchCondition =
        lhsRhsKeysCondition.getLhsKeyCondition();
    KeyMetadataMatchCondition rhsKeyMetadataMatchCondition =
        lhsRhsKeysCondition.getRhsKeyCondition();

    if (lhsKeyMetadataMatchCondition.equals(rhsKeyMetadataMatchCondition)) {
      throwInvalidArgumentException(
          "LhsKeyMetadataMatchCondition cannot be the same as RhsKeyMetadataMatchCondition.");
    }

    validateKeyMetadataMatchConditionOfLhsRhsCondition(lhsKeyMetadataMatchCondition);
    validateKeyMetadataMatchConditionOfLhsRhsCondition(rhsKeyMetadataMatchCondition);
  }

  private void validateMatchConditionOperatorForLhsOrRhsCondition(MatchOperator matchOperator) {
    if (UNSUPPORTED_OPERATORS_FOR_LHS_RHS_KEYS_CONDITION.contains(matchOperator)) {
      throwInvalidArgumentException(
          "GREATER_THAN and LESS_THAN match operators are unsupported in LhsRhsKeysCondition");
    }
  }

  private void validateRequestScannerTypeCondition(
      RequestScannerTypeCondition requestScannerTypeCondition) {
    validateNonDefaultPresenceOrThrow(
        requestScannerTypeCondition, RequestScannerTypeCondition.SCANNER_TYPES_FIELD_NUMBER);
    if (requestScannerTypeCondition.getScannerTypesList().stream().anyMatch(String::isBlank)) {
      throwInvalidArgumentException(
          String.format(
              "RequestScannerTypeCondition should not contain blank string : %s",
              requestScannerTypeCondition));
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
    RegexValidator.validateRegexesWithNonWide(emailRegexes);
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
    RegexValidator.validateRegexesWithNonWide(userAgentRegexes);
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

  private void validateScopeCondition(
      boolean isBlockOrAllowTargetPresent, ScopeCondition condition) {
    switch (condition.getScopeCase()) {
      case ENTITY_SCOPE:
        validateEntityScope(condition.getEntityScope());
        break;
      case LABEL_SCOPE:
        validateLabelScope(isBlockOrAllowTargetPresent, condition.getLabelScope());
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

  private void validateLabelScope(boolean isBlockOrAllowTargetPresent, LabelScope labelScope) {
    if (isBlockOrAllowTargetPresent) {
      throwInvalidArgumentException(
          String.format(
              "Label scope should no be present when exclusion target is block or allow : %s",
              labelScope));
    }
    validateNonDefaultPresenceOrThrow(labelScope, LabelScope.LABEL_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(labelScope, LabelScope.LABEL_IDS_FIELD_NUMBER);
  }

  private void validateUrlScope(UrlScope urlScope) {
    validateNonDefaultPresenceOrThrow(urlScope, UrlScope.URL_REGEXES_FIELD_NUMBER);
    RegexValidator.validateRegexesWithNonWide(urlScope.getUrlRegexesList());
  }

  private void validateSpanAttributeMatchCondition(
      boolean isBlockOrAllowTargetPresent, SpanAttributeMatchCondition condition) {
    if (!condition.hasKeyMatchCondition()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid spanAttributeMatchCondition for detection exclusion rule :%n %s",
              printMessage(condition)));
    }

    KeyMetadataMatchCondition keyMetadataMatchCondition = condition.getKeyMatchCondition();
    if (isBlockOrAllowTargetPresent) {
      if (!VALID_METADATA_FOR_EXCLUSION_TARGET_BLOCK_OR_ALLOW.contains(
          keyMetadataMatchCondition.getMetadata())) {
        throwInvalidArgumentException(
            String.format(
                "Invalid metadata type : %s for detection exclusion rule with exclusion target block or allow",
                keyMetadataMatchCondition.getMetadata()));
      }
    }
    validateNonDefaultPresenceOrThrow(
        keyMetadataMatchCondition, KeyMetadataMatchCondition.METADATA_FIELD_NUMBER);
    KeyMetadata metadata = keyMetadataMatchCondition.getMetadata();
    if (KEY_NULL_METADATA.contains(metadata)) {
      if (keyMetadataMatchCondition.hasMatchCondition()) {
        throwInvalidArgumentException(
            String.format(
                "Key match condition should not be present for key meta data : %s", metadata));
      }
      if (!condition.hasValueMatchCondition()) {
        throwInvalidArgumentException(
            String.format(
                "Value match condition should not be present for key meta data : %s", metadata));
      }
      validateMatchCondition(condition.getValueMatchCondition());
    } else {
      if (!keyMetadataMatchCondition.hasMatchCondition()) {
        throwInvalidArgumentException(
            String.format(
                "Key match condition should be present for key meta data : %s", metadata));
      }
      validateMatchCondition(keyMetadataMatchCondition.getMatchCondition());
      if (condition.hasValueMatchCondition()) {
        validateMatchCondition(condition.getValueMatchCondition());
      }
    }
  }

  private void validateIpLocationTypeCondition(
      boolean isBlockOrAllowTargetPresent, IpLocationTypeCondition condition) {
    validateNonDefaultPresenceOrThrow(
        condition, IpLocationTypeCondition.IP_LOCATION_TYPES_FIELD_NUMBER);
    condition
        .getIpLocationTypesList()
        .forEach(
            ipLocationType -> validateIpLocationType(isBlockOrAllowTargetPresent, ipLocationType));
  }

  private void validateIpLocationType(
      boolean isBlockOrAllowTargetPresent, IpLocationType ipLocationType) {
    if (ipLocationType.equals(IpLocationType.IP_LOCATION_TYPE_UNSPECIFIED)
        || ipLocationType.equals(IpLocationType.UNRECOGNIZED)) {
      throwInvalidArgumentException(String.format("Invalid IP location type : %s", ipLocationType));
    }
    if (isBlockOrAllowTargetPresent && ipLocationType.equals(IP_LOCATION_TYPE_SCANNER)) {
      throwInvalidArgumentException(
          String.format(
              "Invalid IP location type : %s for exclusion target block or alert", ipLocationType));
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

  private void validateEventCondition(boolean isAlertTargetPresent, EventCondition condition) {
    List<CustomRuleEvent> customRuleEvents = condition.getCustomRuleEventsList();
    List<SystemDefinedEvent> systemDefinedEvents = condition.getSystemDefinedEventsList();
    List<ThreatActorEvent> threatActorEvents = condition.getThreatActorEventsList();
    if (customRuleEvents.isEmpty()
        && systemDefinedEvents.isEmpty()
        && threatActorEvents.isEmpty()) {
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
    if (!threatActorEvents.isEmpty()) {
      validateThreatActorEvents(isAlertTargetPresent, threatActorEvents);
    }
  }

  private void validateThreatActorEvents(
      boolean isAlertTargetPresent, List<ThreatActorEvent> threatActorEvents) {
    if (isAlertTargetPresent) {
      throwInvalidArgumentException("Invalid alert exclusion target for threat actor events");
    }
    threatActorEvents.forEach(
        threatActorEvent -> {
          if (threatActorEvent
                  .getThreatActorIdentifier()
                  .equals(THREAT_ACTOR_IDENTIFIER_UNSPECIFIED)
              || threatActorEvent
                  .getThreatActorIdentifier()
                  .equals(ThreatActorIdentifier.UNRECOGNIZED)) {
            throwInvalidArgumentException(
                String.format(
                    "Invalid threat actor identifier : %s",
                    threatActorEvent.getThreatActorIdentifier()));
          }
        });
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
    if (!condition.hasKeyMatchCondition()
        && !condition.hasValueMatchCondition()
        && condition.getObservedTypesList().isEmpty()
        && condition.getLearntTypesList().isEmpty()) {
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

    if (!isValidValue(matchCondition.getValue())) {
      throwInvalidArgumentException("Match condition should have a valid value");
    }

    if (isInvalidMathematicalOperation(matchCondition)) {
      throwInvalidArgumentException(
          String.format(
              "Numerical value should be present for match operator : %s",
              matchCondition.getOperator()));
    }

    if (matchCondition.getOperator().equals(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
        || matchCondition.getOperator().equals(MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)) {
      Value value = matchCondition.getValue();
      if (!value.hasStringValue()) {
        throwInvalidArgumentException("Match condition value should be string for regex matching");
      }
      validateRegex(matchCondition.getValue().getStringValue());
    }
  }

  private boolean isValidValue(Value value) {
    return value.hasStringValue() || value.hasNumberValue() || value.hasBoolValue();
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

  private void validateKeyMetadataMatchConditionOfLhsRhsCondition(
      KeyMetadataMatchCondition keyMetadataMatchCondition) {
    validateNonDefaultPresenceOrThrow(
        keyMetadataMatchCondition, KeyMetadataMatchCondition.METADATA_FIELD_NUMBER);
    KeyMetadata keyMetadata = keyMetadataMatchCondition.getMetadata();
    if (KEY_NULL_METADATA.contains(keyMetadata) && keyMetadataMatchCondition.hasMatchCondition()) {
      throwInvalidArgumentException(
          String.format(
              "Key match condition should not be present for key null meta data : %s",
              keyMetadataMatchCondition.getMetadata()));
    }
    if (!KEY_NULL_METADATA.contains(keyMetadata)) {
      validateMatchConditionOperatorForLhsOrRhsCondition(
          keyMetadataMatchCondition.getMatchCondition().getOperator());
      validateMatchCondition(keyMetadataMatchCondition.getMatchCondition());
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

  public String getName(Message message) {
    return message.getDescriptorForType().getName();
  }
}
