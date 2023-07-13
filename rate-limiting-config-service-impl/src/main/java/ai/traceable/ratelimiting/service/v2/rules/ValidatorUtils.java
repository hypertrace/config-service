package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.ratelimiting.config.service.v2.DataSensitivityLevel.DATA_SENSITIVITY_LEVEL_UNSPECIFIED;
import static ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.platform.utils.ip.IpValidationUtils;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.EmailDomainCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionType;
import ai.traceable.ratelimiting.config.service.v2.IpConnectionTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.IpLocationType;
import ai.traceable.ratelimiting.config.service.v2.IpLocationTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.IpReputationCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.UserAgentCondition;
import ai.traceable.ratelimiting.config.service.v2.UserIdCondition;
import com.google.protobuf.Message;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import io.grpc.Status;
import java.util.List;

public class ValidatorUtils {
  private static final String UTF_8_REGEX_PREFIX = "(*UTF8)";

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
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid Case in %s:%n %s", getName(leafCondition), printMessage(leafCondition)));
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

  public void validateRegexes(List<String> regexes) {
    regexes.forEach(this::validateRegex);
  }

  public void validateRegex(String regexPattern) {
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

  public void throwInvalidArgumentException(String description) {
    throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
  }

  public String getName(Message message) {
    return message.getDescriptorForType().getName();
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

  private void validateStringCondition(KeyValueCondition.StringCondition stringCondition) {
    validateNonDefaultPresenceOrThrow(
        stringCondition, KeyValueCondition.StringCondition.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        stringCondition, KeyValueCondition.StringCondition.VALUE_FIELD_NUMBER);
    if (stringCondition.getOperator() == MATCH_OPERATOR_MATCHES_REGEX
        || stringCondition.getOperator()
            == KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX) {
      validateRegex(stringCondition.getValue());
    }
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
    List<String> cidrIpRanges = ipAddressCondition.getCidrIpRangesList();
    List<String> ipAddresses = ipAddressCondition.getIpAddressesList();
    List<String> rawInputIpData = ipAddressCondition.getRawInputIpDataList();
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

    if (!userIdRegexes.isEmpty()) {
      validateRegexes(userIdRegexes);
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
      validateRegexes(emailRegexes);
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
      validateRegexes(userAgentRegexes);
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
}
