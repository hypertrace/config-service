package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.ratelimiting.config.service.v2.DataSensitivityLevel.DATA_SENSITIVITY_LEVEL_UNSPECIFIED;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_HOST;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_HTTP_METHOD;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_QUERY_PARAMS_COUNT;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_REQUEST_BODY;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_REQUEST_BODY_SIZE;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_REQUEST_COOKIES_COUNT;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_REQUEST_HEADERS_COUNT;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_RESPONSE_BODY;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_RESPONSE_BODY_SIZE;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_RESPONSE_COOKIES_COUNT;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_RESPONSE_HEADERS_COUNT;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_STATUS_CODE;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_URL;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type.TYPE_USER_AGENT;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.platform.utils.ip.IpValidationUtils;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.EmailDomainCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAbuseVelocityCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAsnCondition;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionType;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.IpLocationType;
import ai.traceable.ratelimiting.config.service.v2.IpLocationTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.IpOrganisationCondition;
import ai.traceable.ratelimiting.config.service.v2.IpReputationCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.StringCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.ratelimiting.config.service.v2.RequestScannerTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityScope;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition.LabelScope;
import ai.traceable.ratelimiting.config.service.v2.UserAgentCondition;
import ai.traceable.ratelimiting.config.service.v2.UserIdCondition;
import com.google.protobuf.Message;
import io.grpc.Status;
import java.util.List;

public class ValidatorUtils {
  public static final List<Type> KEY_NULL_CONDITION_TYPES =
      List.of(
          TYPE_URL,
          TYPE_HOST,
          TYPE_HTTP_METHOD,
          TYPE_USER_AGENT,
          TYPE_STATUS_CODE,
          TYPE_RESPONSE_BODY,
          TYPE_REQUEST_BODY,
          TYPE_RESPONSE_BODY_SIZE,
          TYPE_REQUEST_BODY_SIZE,
          TYPE_QUERY_PARAMS_COUNT,
          TYPE_REQUEST_HEADERS_COUNT,
          TYPE_RESPONSE_HEADERS_COUNT,
          TYPE_REQUEST_COOKIES_COUNT,
          TYPE_RESPONSE_COOKIES_COUNT);

  public void validateLeafCondition(LeafCondition leafCondition) {
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
      case IP_ORGANISATION_CONDITION:
        validateIpOrganisationCondition(leafCondition.getIpOrganisationCondition());
        break;
      case IP_ASN_CONDITION:
        validateIpAsnCondition(leafCondition.getIpAsnCondition());
        break;
      case IP_ABUSE_VELOCITY_CONDITION:
        validateIpAbuseVelocityCondition(leafCondition.getIpAbuseVelocityCondition());
        break;
      case REQUEST_SCANNER_TYPE_CONDITION:
        validateRequestScannerTypeCondition(leafCondition.getRequestScannerTypeCondition());
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s", getName(leafCondition), printMessage(leafCondition)));
    }
  }

  private void validateRequestScannerTypeCondition(
      RequestScannerTypeCondition requestScannerTypeCondition) {
    validateNonDefaultPresenceOrThrow(
        requestScannerTypeCondition, RequestScannerTypeCondition.SCANNER_TYPES_FIELD_NUMBER);
    if (requestScannerTypeCondition.getScannerTypesList().stream().anyMatch(String::isBlank)) {
      throwInvalidArgumentException(
          String.format(
              "RequestScannerTypeCondition should not contain blank string : {}",
              requestScannerTypeCondition));
    }
  }

  public void validateKeyValueCondition(KeyValueCondition keyValueCondition) {
    validateNonDefaultPresenceOrThrow(keyValueCondition, KeyValueCondition.TYPE_FIELD_NUMBER);
    if (!keyValueCondition.hasKeyCondition() && !keyValueCondition.hasValueCondition()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(keyValueCondition), printMessage(keyValueCondition)));
    }
    if (KEY_NULL_CONDITION_TYPES.contains(keyValueCondition.getType())
        && (!keyValueCondition.hasValueCondition() || keyValueCondition.hasKeyCondition())) {
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

  public void validateCompositeCondition(CompositeCondition compositeCondition) {
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.CHILDREN_FIELD_NUMBER);
    compositeCondition.getChildrenList().forEach(this::validateCondition);
  }

  public void validateRegex(String regexPattern) {
    Status status = RegexValidator.validateRegex(regexPattern);
    if (!status.isOk()) {
      throwInvalidArgumentException(String.format("Invalid Regex pattern: %s", regexPattern));
    }
  }

  public void throwAlreadyExistsException(String description) {
    throw Status.ALREADY_EXISTS.withDescription(description).asRuntimeException();
  }

  public void throwInvalidArgumentException(String description) {
    throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
  }

  public void throwInvalidArgumentExceptionIf(boolean condition, String description) {
    if (condition) {
      throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
    }
  }

  public String getName(Message message) {
    return message.getDescriptorForType().getName();
  }

  public void validateRegionCondition(RegionCondition regionCondition) {
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

  public void validateIpAddressCondition(IpAddressCondition condition) {
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
      IpAddressCondition ipAddressCondition,
      List<String> cidrIpRanges,
      List<String> ipAddresses,
      List<String> rawInputIpData) {
    if (cidrIpRanges.isEmpty() && ipAddresses.isEmpty() && rawInputIpData.isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid ipAddressCondition for rate limit rule :%n %s",
              printMessage(ipAddressCondition)));
    }
    if ((!rawInputIpData.isEmpty()) && (!cidrIpRanges.isEmpty() || !ipAddresses.isEmpty())) {
      throwInvalidArgumentException(
          String.format(
              "IpAddressCondition should not have rawInputIpData and (cidrIpRanges or ipAddresses) simultaneously :%n %s",
              printMessage(ipAddressCondition)));
    }
    if (!ipAddresses.stream().allMatch(IpValidationUtils::isValidIpAddress)) {
      throwInvalidArgumentException("IpAddressCondition should have valid IP addresses");
    }
    if (!cidrIpRanges.stream().allMatch(IpValidationUtils::isValidSubnet)) {
      throwInvalidArgumentException("IpAddressCondition should have valid CIDR IP ranges");
    }
  }

  public void validateStringCondition(StringCondition stringCondition) {
    validateNonDefaultPresenceOrThrow(stringCondition, StringCondition.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(stringCondition, StringCondition.VALUE_FIELD_NUMBER);
    if (isInvalidMathematicalOperation(stringCondition)) {
      throwInvalidArgumentException(
          String.format(
              "Numerical value should be present for match operator : %s",
              stringCondition.getOperator()));
    }
    if (stringCondition.getOperator() == MATCH_OPERATOR_MATCHES_REGEX
        || stringCondition.getOperator() == MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX) {
      validateRegex(stringCondition.getValue());
    }
  }

  private boolean isInvalidMathematicalOperation(StringCondition stringCondition) {
    return (stringCondition.getOperator().equals(MatchOperator.MATCH_OPERATOR_GREATER_THAN)
            || stringCondition.getOperator().equals(MatchOperator.MATCH_OPERATOR_LESS_THAN))
        && !isNumber(stringCondition.getValue());
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

  private void validateEntityScope(EntityScope entityScope) {
    validateNonDefaultPresenceOrThrow(entityScope, EntityScope.ENTITY_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(entityScope, EntityScope.ENTITY_IDS_FIELD_NUMBER);
  }

  private void validateLabelScope(LabelScope labelScope) {
    validateNonDefaultPresenceOrThrow(labelScope, LabelScope.LABEL_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(labelScope, LabelScope.LABEL_IDS_FIELD_NUMBER);
  }

  private void validateDatatypeCondition(DatatypeCondition datatypeCondition) {
    if (datatypeCondition.getDatasetIdsList().isEmpty()
        && datatypeCondition.getDatatypeIdsList().isEmpty()
        && datatypeCondition.getDataSensitivityLevelsList().isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(datatypeCondition), printMessage(datatypeCondition)));
    }
    validateDataSensitivityLevels(datatypeCondition);
  }

  private void validateDataSensitivityLevels(DatatypeCondition datatypeCondition) {
    if (datatypeCondition.getDataSensitivityLevelsList().stream()
        .anyMatch(dataSensitivity -> dataSensitivity.equals(DATA_SENSITIVITY_LEVEL_UNSPECIFIED))) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(datatypeCondition), printMessage(datatypeCondition)));
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
    List<String> userIds = userIdCondition.getUserIdsList();
    if (actorEntityIds.isEmpty() && userIdRegexes.isEmpty() && userIds.isEmpty()) {
      throwInvalidArgumentException(
          String.format(
              "Invalid condition for type %s:%n %s",
              getName(userIdCondition), printMessage(userIdCondition)));
    }
    RegexValidator.validateRegexesWithNonWide(userIdRegexes);
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

  private void validateIpOrganisationCondition(IpOrganisationCondition ipOrganisationCondition) {
    RegexValidator.validateRegexesWithNonWide(
        ipOrganisationCondition.getIpOrganisationRegexesList());
  }

  private void validateIpAsnCondition(IpAsnCondition ipAsnCondition) {
    RegexValidator.validateRegexesWithNonWide(ipAsnCondition.getIpAsnRegexesList());
  }

  private void validateIpAbuseVelocityCondition(IpAbuseVelocityCondition ipAbuseVelocityCondition) {
    validateNonDefaultPresenceOrThrow(
        ipAbuseVelocityCondition, IpAbuseVelocityCondition.MIN_IP_ABUSE_VELOCITY_FIELD_NUMBER);
  }

  private boolean isNumber(String value) {
    try {
      Double.parseDouble(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
