package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.customsignature.config.service.v1.IpAddressExpressionType.IP_ADDRESS_EXPRESSION_TYPE_ALL_EXTERNAL;
import static ai.traceable.customsignature.config.service.v1.IpAddressExpressionType.IP_ADDRESS_EXPRESSION_TYPE_ALL_INTERNAL;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_BODY;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_BODY_PARAMETER_VALUE;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_BODY_SIZE;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_COOKIES_COUNT;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_COOKIE_VALUE;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_HEADERS_COUNT;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_HEADER_VALUE;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_HOST;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_HTTP_METHOD;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_PARAMETER_VALUE;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_QUERY_PARAMETER_VALUE;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_STATUS_CODE;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_URL;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_USER_AGENT;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_EQUALS;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_GREATER_THAN;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_LESS_THAN;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_NOT_EQUAL;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SEC_RULE;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SEC_RULE_DIRECTIVES_WITH_CHAIN_KEYWORDS_REGEX;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SEC_RULE_ID_REGEX;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSecRule;
import ai.traceable.customsignature.config.service.v1.EmailDomainExpression;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.IpAbuseVelocityExpression;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.IpAddressExpressionType;
import ai.traceable.customsignature.config.service.v1.IpAsnExpression;
import ai.traceable.customsignature.config.service.v1.IpConnectionType;
import ai.traceable.customsignature.config.service.v1.IpConnectionTypeExpression;
import ai.traceable.customsignature.config.service.v1.IpOrganisationExpression;
import ai.traceable.customsignature.config.service.v1.IpReputationExpression;
import ai.traceable.customsignature.config.service.v1.IpType;
import ai.traceable.customsignature.config.service.v1.IpTypeExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.LhsRhsKeysExpression;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RequestScannerTypeExpression;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import ai.traceable.customsignature.config.service.v1.StringCondition;
import ai.traceable.customsignature.config.service.v1.UserAgentExpression;
import ai.traceable.customsignature.config.service.v1.UserIdExpression;
import ai.traceable.platform.utils.ip.IpValidationUtils;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ClauseGroupValidator {

  private static final String UTF_8_REGEX_PREFIX = "(*UTF8)";

  private static final Set<MatchKey> INVALID_RESPONSE_MATCH_KEYS =
      Set.of(
          MATCH_KEY_URL,
          MATCH_KEY_QUERY_PARAMS_COUNT,
          MATCH_KEY_HOST,
          MATCH_KEY_HTTP_METHOD,
          MATCH_KEY_USER_AGENT);

  private static final Set<MatchOperator> NUMERIC_MATCH_OPERATORS =
      Set.of(MATCH_OPERATOR_LESS_THAN, MATCH_OPERATOR_GREATER_THAN);

  private static final Set<MatchOperator> SUPPORTED_FIRST_LEVEL_OPERATORS_FOR_LHS_RHS_EXPRESSIONS =
      Set.of(
          MATCH_OPERATOR_EQUALS,
          MATCH_OPERATOR_NOT_EQUAL,
          MATCH_OPERATOR_CONTAINS,
          MATCH_OPERATOR_NOT_CONTAIN);

  private static final Set<MatchOperator> UNSUPPORTED_OPERATORS_FOR_LHS_RHS_EXPRESSIONS =
      Set.of(MATCH_OPERATOR_LESS_THAN, MATCH_OPERATOR_GREATER_THAN);

  public static final Set<MatchKey> KEY_NULL_MATCH_KEYS =
      Set.of(
          MATCH_KEY_URL,
          MATCH_KEY_HOST,
          MATCH_KEY_HTTP_METHOD,
          MATCH_KEY_USER_AGENT,
          MATCH_KEY_STATUS_CODE,
          MATCH_KEY_HEADER_VALUE,
          MATCH_KEY_PARAMETER_VALUE,
          MATCH_KEY_QUERY_PARAMETER_VALUE,
          MATCH_KEY_BODY_PARAMETER_VALUE,
          MATCH_KEY_COOKIE_VALUE,
          MATCH_KEY_BODY,
          MATCH_KEY_BODY_SIZE,
          MATCH_KEY_QUERY_PARAMS_COUNT,
          MATCH_KEY_HEADERS_COUNT,
          MATCH_KEY_COOKIES_COUNT);

  private static final Set<MatchKey> NUMERIC_MATCH_KEYS =
      Set.of(
          MATCH_KEY_BODY_SIZE,
          MATCH_KEY_QUERY_PARAMS_COUNT,
          MATCH_KEY_HEADERS_COUNT,
          MATCH_KEY_COOKIES_COUNT);

  public Status validateClauseGroup(ClauseGroup clauseGroup, EventType eventType) {
    if (clauseGroup.getClauseOperator() == ClauseOperator.CLAUSE_OPERATOR_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Definition clause group should have a valid clause operator.");
    }

    if (clauseGroup.getClausesList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Definition clause group should have at least one clause.");
    }

    Optional<Status> errorStatus =
        clauseGroup.getClausesList().stream()
            .map(clause -> validateClause(clause, eventType))
            .filter(status -> status != Status.OK)
            .findFirst();
    if (errorStatus.isPresent()) {
      return errorStatus.get();
    }

    // custom signature rule containing SecRule clause should not have OR operator or nested clauses
    // since that's not yet supported in platform
    // examples of such rules:
    //  - (SecRuleClause) OR (KeyValueExpression)
    // - (SecRuleClause) AND (KeyValueExpression OR IpAddressExpression)
    if (containsSecRuleClause(clauseGroup)
        && (containsNestedClause(clauseGroup)
            || clauseGroup.getClauseOperator().equals(ClauseOperator.CLAUSE_OPERATOR_OR))) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Definition clause group with sec rule clause "
              + "should not have nested clauses or OR operator.");
    }

    return Status.OK;
  }

  public Status validateClause(Clause clause, EventType eventType) {
    switch (clause.getClauseCase()) {
      case MATCH_EXPRESSION:
        return validateMatchExpression(clause.getMatchExpression(), eventType, false);
      case KEY_VALUE_EXPRESSION:
        return validateKeyValueExpression(clause.getKeyValueExpression());
      case ATTRIBUTE_KEY_VALUE_EXPRESSION:
        return validateAttributeKeyValueExpression(clause.getAttributeKeyValueExpression());
      case CUSTOM_SEC_RULE:
        return validateCustomSecRule(clause.getCustomSecRule());
      case IP_ADDRESS_EXPRESSION:
        return validateIpAddressExpression(clause.getIpAddressExpression(), eventType);
      case IP_TYPE_EXPRESSION:
        return validateIpTypeExpression(clause.getIpTypeExpression());
      case IP_REPUTATION_EXPRESSION:
        return validateIpReputationExpression(clause.getIpReputationExpression());
      case IP_CONNECTION_TYPE_EXPRESSION:
        return validateIpConnectionTypeExpression(clause.getIpConnectionTypeExpression());
      case IP_ORGANISATION_EXPRESSION:
        return validateIpOrganisationExpression(clause.getIpOrganisationExpression());
      case IP_ASN_EXPRESSION:
        return validateIpAsnExpression(clause.getIpAsnExpression());
      case IP_ABUSE_VELOCITY_EXPRESSION:
        return validateIpAbuseVelocityExpression(clause.getIpAbuseVelocityExpression());
      case REGION_EXPRESSION:
        return validateRegionExpression(clause.getRegionExpression());
      case USER_ID_EXPRESSION:
        return validateUserIdExpression(clause.getUserIdExpression());
      case EMAIL_DOMAIN_EXPRESSION:
        return validateEmailDomainExpression(clause.getEmailDomainExpression());
      case USER_AGENT_EXPRESSION:
        return validateUserAgentExpression(clause.getUserAgentExpression());
      case REQUEST_SCANNER_TYPE_EXPRESSION:
        return validateRequestScannerTypeExpression(clause.getRequestScannerTypeExpression());
      case SCOPE_EXPRESSION:
        return validateScopeExpression(clause.getScopeExpression());
      case LHS_RHS_KEYS_EXPRESSION:
        return validateLhsRhsKeysExpression(clause.getLhsRhsKeysExpression(), eventType);
      case CLAUSE_GROUP:
        return validateClauseGroup(clause.getClauseGroup(), eventType);
      default:
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("Invalid Custom Signature Rule Clause expression %s ", clause));
    }
  }

  private Status validateScopeExpression(ScopeExpression scopeExpression) {
    switch (scopeExpression.getScopeCase()) {
      case ENTITY_SCOPE:
        ScopeExpression.EntityScope entityScope = scopeExpression.getEntityScope();
        validateNonDefaultPresenceOrThrow(
            entityScope, ScopeExpression.EntityScope.ENTITY_TYPE_FIELD_NUMBER);
        validateNonDefaultPresenceOrThrow(
            entityScope, ScopeExpression.EntityScope.ENTITY_IDS_FIELD_NUMBER);
        break;

      case LABEL_SCOPE:
        ScopeExpression.LabelScope labelScope = scopeExpression.getLabelScope();
        validateNonDefaultPresenceOrThrow(
            labelScope, ScopeExpression.LabelScope.LABEL_TYPE_FIELD_NUMBER);
        validateNonDefaultPresenceOrThrow(
            labelScope, ScopeExpression.LabelScope.LABEL_IDS_FIELD_NUMBER);
        break;

      case URL_SCOPE:
        ScopeExpression.UrlScope urlScope = scopeExpression.getUrlScope();
        validateNonDefaultPresenceOrThrow(
            urlScope, ScopeExpression.UrlScope.URL_REGEXES_FIELD_NUMBER);
        RegexValidator.validateRegexesWithNonWide(urlScope.getUrlRegexesList());
        break;

      case SCOPE_NOT_SET:
        return Status.INVALID_ARGUMENT.withDescription("Scope not set in ScopeExpression");
    }
    return Status.OK;
  }

  private Status validateRequestScannerTypeExpression(
      RequestScannerTypeExpression requestScannerTypeExpression) {
    validateNonDefaultPresenceOrThrow(
        requestScannerTypeExpression, RequestScannerTypeExpression.SCANNER_TYPES_FIELD_NUMBER);
    if (requestScannerTypeExpression.getScannerTypesList().isEmpty()
        || requestScannerTypeExpression.getScannerTypesList().stream().anyMatch(String::isBlank)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Invalid request scanner type expression : %s", requestScannerTypeExpression));
    }
    return Status.OK;
  }

  private Status validateUserAgentExpression(UserAgentExpression userAgentExpression) {
    List<String> userAgents = userAgentExpression.getUserAgentsList();
    List<String> userAgentRegexes = userAgentExpression.getUserAgentRegexesList();
    if (userAgents.isEmpty() && userAgentRegexes.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Invalid expression for type %s:%n %s",
              getName(userAgentExpression), printMessage(userAgentExpression)));
    }
    RegexValidator.validateRegexesWithNonWide(userAgentRegexes);
    return Status.OK;
  }

  private Status validateEmailDomainExpression(EmailDomainExpression emailDomainExpression) {
    List<String> emailDomains = emailDomainExpression.getEmailDomainsList();
    List<String> emailRegexes = emailDomainExpression.getEmailRegexesList();
    if (emailDomains.isEmpty() && emailRegexes.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Invalid expression for type %s:%n %s",
              getName(emailDomainExpression), printMessage(emailDomainExpression)));
    }
    RegexValidator.validateRegexesWithNonWide(emailRegexes);
    return Status.OK;
  }

  private Status validateUserIdExpression(UserIdExpression userIdExpression) {
    List<String> userIdRegexes = userIdExpression.getUserIdRegexesList();
    List<String> userIds = userIdExpression.getUserIdsList();
    if (userIdRegexes.isEmpty() && userIds.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Invalid expression for type %s:%n %s",
              getName(userIdExpression), printMessage(userIdExpression)));
    }
    RegexValidator.validateRegexesWithNonWide(userIdRegexes);
    return Status.OK;
  }

  private Status validateRegionExpression(RegionExpression regionExpression) {
    if (regionExpression.getRegionIdentifiersList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "At least one region id should be provided for region expression : %s",
              regionExpression));
    }
    for (RegionExpression.Region region : regionExpression.getRegionIdentifiersList()) {
      if (region.getCountryIsoCode().isEmpty()) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "Region value cannot be empty for region expression : %s", regionExpression));
      }
    }
    return Status.OK;
  }

  private Status validateIpAbuseVelocityExpression(
      IpAbuseVelocityExpression ipAbuseVelocityExpression) {
    validateNonDefaultPresenceOrThrow(
        ipAbuseVelocityExpression, IpAbuseVelocityExpression.MIN_IP_ABUSE_VELOCITY_FIELD_NUMBER);
    return Status.OK;
  }

  private Status validateIpAsnExpression(IpAsnExpression ipAsnExpression) {
    RegexValidator.validateRegexesWithNonWide(ipAsnExpression.getIpAsnRegexesList());
    return Status.OK;
  }

  private Status validateIpOrganisationExpression(
      IpOrganisationExpression ipOrganisationExpression) {
    RegexValidator.validateRegexesWithNonWide(
        ipOrganisationExpression.getIpOrganisationRegexesList());
    return Status.OK;
  }

  private Status validateIpConnectionTypeExpression(
      IpConnectionTypeExpression ipConnectionTypeExpression) {
    validateNonDefaultPresenceOrThrow(
        ipConnectionTypeExpression, IpConnectionTypeExpression.IP_CONNECTION_TYPES_FIELD_NUMBER);
    for (IpConnectionType ipConnectionType :
        ipConnectionTypeExpression.getIpConnectionTypesList()) {
      Status status = validateIpConnectionType(ipConnectionType);
      if (status != Status.OK) {
        return status;
      }
    }
    return Status.OK;
  }

  private Status validateIpConnectionType(IpConnectionType ipConnectionType) {
    if (ipConnectionType.equals(IpConnectionType.IP_CONNECTION_TYPE_UNSPECIFIED)
        || ipConnectionType.equals(IpConnectionType.UNRECOGNIZED)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format("Invalid IP Connection Type : %s", ipConnectionType));
    }
    return Status.OK;
  }

  private Status validateIpReputationExpression(IpReputationExpression ipReputationExpression) {
    validateNonDefaultPresenceOrThrow(
        ipReputationExpression, IpReputationExpression.MIN_IP_REPUTATION_SEVERITY_FIELD_NUMBER);
    return Status.OK;
  }

  private Status validateIpAddressExpression(
      IpAddressExpression ipAddressExpression, EventType eventType) {
    List<String> cidrIpRanges = ipAddressExpression.getCidrIpRangesList();
    List<String> ipAddresses = ipAddressExpression.getIpAddressesList();
    List<String> rawInputIpData = ipAddressExpression.getRawInputIpDataList();
    IpAddressExpressionType expressionType = ipAddressExpression.getIpAddressExpressionType();
    if (expressionType.equals(IP_ADDRESS_EXPRESSION_TYPE_ALL_EXTERNAL)
        || expressionType.equals(IP_ADDRESS_EXPRESSION_TYPE_ALL_INTERNAL)) {
      if (!cidrIpRanges.isEmpty() || !ipAddresses.isEmpty() || !rawInputIpData.isEmpty()) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "RawInputIpData, cidrIpRanges and ipAddresses should be empty for ip address expression type : %s ",
                ipAddressExpression.getIpAddressExpressionType()));
      }

      if (eventType == EventType.EVENT_TYPE_ALLOW
          || eventType == EventType.EVENT_TYPE_DETECTION_AND_BLOCKING) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "Allow/Blocking action is unsupported for ip address expression type: %s",
                ipAddressExpression.getIpAddressExpressionType()));
      }

      return Status.OK;
    }
    return validateIpAddressesAndRanges(
        ipAddressExpression, cidrIpRanges, ipAddresses, rawInputIpData);
  }

  private Status validateIpAddressesAndRanges(
      IpAddressExpression ipAddressExpression,
      List<String> cidrIpRanges,
      List<String> ipAddresses,
      List<String> rawInputIpData) {
    if (cidrIpRanges.isEmpty() && ipAddresses.isEmpty() && rawInputIpData.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Invalid ipAddressExpression for custom signature rule :%n %s",
              printMessage(ipAddressExpression)));
    }
    if ((!rawInputIpData.isEmpty()) && (!cidrIpRanges.isEmpty() || !ipAddresses.isEmpty())) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "IpAddressExpression should not have rawInputIpData and (cidrIpRanges or ipAddresses) simultaneously :%n %s",
              printMessage(ipAddressExpression)));
    }
    if (!ipAddresses.stream().allMatch(IpValidationUtils::isValidIpAddress)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IpAddressExpression should have valid IP addresses");
    }
    if (!cidrIpRanges.stream().allMatch(IpValidationUtils::isValidSubnet)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IpAddressExpression should have valid CIDR IP ranges");
    }
    return Status.OK;
  }

  private Status validateIpTypeExpression(IpTypeExpression ipTypeExpression) {
    validateNonDefaultPresenceOrThrow(ipTypeExpression, IpTypeExpression.IP_TYPES_FIELD_NUMBER);
    for (IpType ipType : ipTypeExpression.getIpTypesList()) {
      Status status = validateIpType(ipType);
      if (status != Status.OK) {
        return status;
      }
    }
    return Status.OK;
  }

  private Status validateIpType(IpType ipType) {
    if (ipType.equals(IpType.IP_TYPE_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid IP Type");
    }
    return Status.OK;
  }

  private Status validateMatchExpression(
      MatchExpression matchExpression, EventType eventType, boolean isLhsRhsExpression) {
    MatchKey matchKey = matchExpression.getMatchKey();
    MatchOperator matchOperator = matchExpression.getMatchOperator();

    if (MatchKey.MATCH_KEY_UNSPECIFIED.equals(matchKey)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule match expression should have a valid match key.");
    }

    if (MatchOperator.MATCH_OPERATOR_UNSPECIFIED.equals(matchOperator)) {
      // if it's either a vanilla match expression or
      // a lhs rhs based match expression with a match key that is not in the KEY_NULL_MATCH_KEYS
      // list
      if (!isLhsRhsExpression || !KEY_NULL_MATCH_KEYS.contains(matchKey)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Custom Signature Rule match expression should have a valid match operator.");
      }
    }

    if (matchExpression.getMatchCategory().equals(MatchCategory.MATCH_CATEGORY_RESPONSE)
        && INVALID_RESPONSE_MATCH_KEYS.contains(matchKey)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Invalid match key : %s for match category : %s for custom signature rule",
              matchKey, matchExpression.getMatchCategory()));
    }

    if (hasInvalidBlockingConditionForCookieOrHeaderValues(matchKey, matchOperator, eventType)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Do not match exactly and Do not match pattern operators are unsupported "
              + "for cookie and header values for blocking");
    }

    if (matchExpression.getMatchValue().isEmpty() && !matchExpression.hasValue()) {
      // if it's either a vanilla match expression or
      // a lhs rhs based match expression with a match key that is not in the KEY_NULL_MATCH_KEYS
      // list
      if (!isLhsRhsExpression || !KEY_NULL_MATCH_KEYS.contains(matchKey)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Both matchValue and Value cannot be empty.");
      }
    }

    if (isInvalidMathematicalOperation(matchExpression)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Custom Signature Rule match expression should have an integer match value for match operator: %s, match key: %s",
              matchOperator, matchKey));
    }

    if (MATCH_OPERATOR_MATCHES_REGEX.equals(matchOperator)
        || MATCH_OPERATOR_NOT_MATCH_REGEX.equals(matchOperator)) {
      if (matchExpression.hasValue()) {
        return validateRegex(matchExpression.getValue().getStringValue());
      }
      return validateRegex(matchExpression.getMatchValue());
    }

    if (matchExpression.hasValue() && !validateValue(matchExpression.getValue())) {
      return Status.INVALID_ARGUMENT.withDescription("Value in match expression is invalid");
    }

    return Status.OK;
  }

  private boolean hasInvalidBlockingConditionForCookieOrHeaderValues(
      MatchKey matchKey, MatchOperator matchOperator, EventType eventType) {
    return (matchKey == MATCH_KEY_COOKIE_VALUE || matchKey == MATCH_KEY_HEADER_VALUE)
        && eventType == EventType.EVENT_TYPE_DETECTION_AND_BLOCKING
        && (matchOperator == MATCH_OPERATOR_NOT_EQUAL
            || matchOperator == MATCH_OPERATOR_NOT_MATCH_REGEX);
  }

  private boolean isInvalidMathematicalOperation(MatchExpression matchExpression) {
    boolean isNumericMatchKeyOrMatchOperator =
        NUMERIC_MATCH_OPERATORS.contains(matchExpression.getMatchOperator())
            || NUMERIC_MATCH_KEYS.contains(matchExpression.getMatchKey());
    if (matchExpression.hasValue()) {
      return isNumericMatchKeyOrMatchOperator && !isInteger(matchExpression.getValue());
    }
    return isNumericMatchKeyOrMatchOperator && !isInteger(matchExpression.getMatchValue());
  }

  private Status validateKeyValueExpression(KeyValueExpression keyValueExpression) {
    if (KeyValueTag.KEY_VALUE_TAG_UNSPECIFIED.equals(keyValueExpression.getTag())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule key-value expression should have a valid tag.");
    }
    return validateExpression(
        false,
        keyValueExpression.getMatchKey(),
        keyValueExpression.getKeyMatchOperator(),
        keyValueExpression.getMatchValue(),
        keyValueExpression.getValueMatchOperator());
  }

  private Status validateAttributeKeyValueExpression(
      AttributeKeyValueExpression attributeKeyValueExpression) {
    Status status;

    if (!attributeKeyValueExpression.hasKeyCondition()) {
      status =
          validateExpression(
              true,
              attributeKeyValueExpression.getMatchKey(),
              attributeKeyValueExpression.getKeyMatchOperator(),
              attributeKeyValueExpression.getMatchValue(),
              attributeKeyValueExpression.getValueMatchOperator());

      if (status != Status.OK) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "Invalid attribute key value expression : %s", attributeKeyValueExpression));
      }

      return Status.OK;
    }

    status = validateStringCondition(attributeKeyValueExpression.getKeyCondition());
    if (status != Status.OK) {
      return status;
    }
    if (attributeKeyValueExpression.hasValueCondition()) {
      return validateStringCondition(attributeKeyValueExpression.getValueCondition());
    }

    return Status.OK;
  }

  private Status validateStringCondition(StringCondition stringCondition) {
    validateNonDefaultPresenceOrThrow(stringCondition, StringCondition.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(stringCondition, StringCondition.VALUE_FIELD_NUMBER);
    if (MATCH_OPERATOR_MATCHES_REGEX.equals(stringCondition.getOperator())
        || MATCH_OPERATOR_NOT_MATCH_REGEX.equals(stringCondition.getOperator())) {
      return validateRegex(stringCondition.getValue());
    }
    return Status.OK;
  }

  private Status validateExpression(
      boolean isEmptyValueAllowed,
      String matchKey,
      MatchOperator keyMatchOperator,
      String matchValue,
      MatchOperator valueMatchOperator) {
    if (matchKey.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule expression should have a valid match key.");
    }
    if (MatchOperator.MATCH_OPERATOR_UNSPECIFIED.equals(keyMatchOperator)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule expression should have a valid key match operator.");
    }
    if (!isEmptyValueAllowed && matchValue.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule expression should have a valid match value.");
    }
    if (!matchValue.isEmpty()
        && MatchOperator.MATCH_OPERATOR_UNSPECIFIED.equals(valueMatchOperator)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule expression should have a valid value match operator.");
    }
    if (MATCH_OPERATOR_MATCHES_REGEX.equals(valueMatchOperator)
        || MATCH_OPERATOR_NOT_MATCH_REGEX.equals(valueMatchOperator)) {
      return validateRegex(matchValue);
    }
    return Status.OK;
  }

  private Status validateRegex(String regexPattern) {
    if (regexPattern.startsWith(UTF_8_REGEX_PREFIX)) {
      return Status.OK;
    }
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern.compile(regexPattern);
      return Status.OK;
    } catch (PatternSyntaxException e) {
      return Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription("Invalid Regex Value for the custom signature rule expression");
    }
  }

  private Status validateCustomSecRule(CustomSecRule rule) {
    String inputSecRule = rule.getInputSecRule();
    if (!inputSecRule.startsWith(SEC_RULE)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Sec Rule should be start with keyword SecRule");
    }

    if (!SEC_RULE_ID_REGEX.matcher(inputSecRule).find()) {
      return Status.INVALID_ARGUMENT.withDescription("Sec Rule actions should start with \"id: ");
    }

    if (checkChainKeywords(inputSecRule)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Chain keyword should be there between any 2 directives containing SecAction or SecRule or SecRuleScript");
    }
    if (!rule.getSanitisedSecRule().isBlank()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Sanitized Sec Rule should be empty in create/update request");
    }
    return Status.OK;
  }

  private Status validateLhsRhsKeysExpression(
      LhsRhsKeysExpression lhsRhsKeysExpression, EventType eventType) {
    Status status = validateLhsRhsMatchOperator(lhsRhsKeysExpression);
    if (status != Status.OK) {
      return status;
    }

    if (lhsRhsKeysExpression.hasLhsKeyExpression() || lhsRhsKeysExpression.hasRhsKeyExpression()) {
      return validateDeprecatedLhsRhsFields(lhsRhsKeysExpression, eventType);
    }

    status = validateLhsRhsFieldsPresence(lhsRhsKeysExpression);
    if (status != Status.OK) {
      return status;
    }

    status = validateLhsRhsNotEqual(lhsRhsKeysExpression);
    if (status != Status.OK) {
      return status;
    }

    status = validateLhsOrRhsExpression(lhsRhsKeysExpression, eventType, true);
    if (status != Status.OK) {
      return status;
    }

    return validateLhsOrRhsExpression(lhsRhsKeysExpression, eventType, false);
  }

  private Status validateLhsRhsMatchOperator(LhsRhsKeysExpression lhsRhsKeysExpression) {
    validateNonDefaultPresenceOrThrow(
        lhsRhsKeysExpression, LhsRhsKeysExpression.MATCH_OPERATOR_FIELD_NUMBER);
    if (!SUPPORTED_FIRST_LEVEL_OPERATORS_FOR_LHS_RHS_EXPRESSIONS.contains(
        lhsRhsKeysExpression.getMatchOperator())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "The first-level MatchOperator in LhsRhsKeysExpression must be one of: EQUALS, NOT_EQUAL, CONTAINS, or NOT_CONTAIN.");
    }
    return Status.OK;
  }

  private Status validateDeprecatedLhsRhsFields(
      LhsRhsKeysExpression lhsRhsKeysExpression, EventType eventType) {
    if ((!lhsRhsKeysExpression.hasLhsKeyExpression() && lhsRhsKeysExpression.hasRhsKeyExpression())
        || (lhsRhsKeysExpression.hasLhsKeyExpression()
            && !lhsRhsKeysExpression.hasRhsKeyExpression())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "LhsKeyExpression and RhsKeyExpression must either be both present or absent together.");
    }

    Status status;
    MatchExpression lhsKeyExpression = lhsRhsKeysExpression.getLhsKeyExpression();
    MatchExpression rhsKeyExpression = lhsRhsKeysExpression.getRhsKeyExpression();

    if (lhsKeyExpression.equals(rhsKeyExpression)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "LhsKeyExpression cannot be the same as RhsKeyExpression.");
    }

    status = validateUnsupportedOperatorsForLhsRhsExpressions(lhsKeyExpression.getMatchOperator());
    if (status != Status.OK) {
      return status;
    }
    status = validateUnsupportedOperatorsForLhsRhsExpressions(rhsKeyExpression.getMatchOperator());
    if (status != Status.OK) {
      return status;
    }

    if (!KEY_NULL_MATCH_KEYS.contains(lhsKeyExpression.getMatchKey())) {
      status = validateMatchExpression(lhsKeyExpression, eventType, true);
      if (status != Status.OK) {
        return status;
      }
    }

    if (!KEY_NULL_MATCH_KEYS.contains(rhsKeyExpression.getMatchKey())) {
      status = validateMatchExpression(rhsKeyExpression, eventType, true);
      return status;
    }

    return Status.OK;
  }

  private Status validateLhsRhsFieldsPresence(LhsRhsKeysExpression lhsRhsKeysExpression) {
    boolean hasLhsExpression =
        lhsRhsKeysExpression.hasKeyLhsExpression()
            || lhsRhsKeysExpression.hasAttributeLhsExpression();
    boolean hasRhsExpression =
        lhsRhsKeysExpression.hasKeyRhsExpression()
            || lhsRhsKeysExpression.hasAttributeRhsExpression();
    if (!hasLhsExpression || !hasRhsExpression) {
      return Status.INVALID_ARGUMENT.withDescription(
          "LhsExpression and RhsExpression must either be both present or absent together.");
    }
    return Status.OK;
  }

  private Status validateLhsRhsNotEqual(LhsRhsKeysExpression lhsRhsKeysExpression) {
    if (lhsRhsKeysExpression.hasKeyLhsExpression()
        && lhsRhsKeysExpression.hasKeyRhsExpression()
        && lhsRhsKeysExpression
            .getKeyLhsExpression()
            .equals(lhsRhsKeysExpression.getKeyRhsExpression())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "KeyLhsExpression and KeyRhsExpression cannot be the same.");
    }
    if (lhsRhsKeysExpression.hasAttributeLhsExpression()
        && lhsRhsKeysExpression.hasAttributeRhsExpression()
        && lhsRhsKeysExpression
            .getAttributeLhsExpression()
            .equals(lhsRhsKeysExpression.getAttributeRhsExpression())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "AttributeLhsExpression and AttributeRhsExpression cannot be the same.");
    }
    return Status.OK;
  }

  private Status validateLhsOrRhsExpression(
      LhsRhsKeysExpression lhsRhsKeysExpression, EventType eventType, boolean isLhsExpression) {
    Status status;

    if (isLhsExpression
        ? lhsRhsKeysExpression.hasKeyLhsExpression()
        : lhsRhsKeysExpression.hasKeyRhsExpression()) {
      MatchExpression matchExpression =
          isLhsExpression
              ? lhsRhsKeysExpression.getKeyLhsExpression()
              : lhsRhsKeysExpression.getKeyRhsExpression();
      status = validateUnsupportedOperatorsForLhsRhsExpressions(matchExpression.getMatchOperator());
      if (status != Status.OK) {
        return status;
      }

      if (!KEY_NULL_MATCH_KEYS.contains(matchExpression.getMatchKey())) {
        status = validateMatchExpression(matchExpression, eventType, true);
        return status;
      }

      return Status.OK;

    } else if (isLhsExpression
        ? lhsRhsKeysExpression.hasAttributeLhsExpression()
        : lhsRhsKeysExpression.hasAttributeRhsExpression()) {
      StringCondition attributeExpression =
          isLhsExpression
              ? lhsRhsKeysExpression.getAttributeLhsExpression()
              : lhsRhsKeysExpression.getAttributeRhsExpression();
      status = validateUnsupportedOperatorsForLhsRhsExpressions(attributeExpression.getOperator());
      if (status != Status.OK) {
        return status;
      }
      status = validateStringCondition(attributeExpression);
      return status;
    }

    return Status.OK;
  }

  private Status validateUnsupportedOperatorsForLhsRhsExpressions(MatchOperator matchOperator) {
    if (UNSUPPORTED_OPERATORS_FOR_LHS_RHS_EXPRESSIONS.contains(matchOperator)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GREATER_THAN and LESS_THAN match operators are unsupported in case of LhsRhsExpression.");
    }
    return Status.OK;
  }

  private boolean checkChainKeywords(String inputSecRule) {
    Matcher matcher = SEC_RULE_DIRECTIVES_WITH_CHAIN_KEYWORDS_REGEX.matcher(inputSecRule);
    return matcher.matches();
  }

  private boolean isInteger(String value) {
    try {
      Integer.parseInt(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isInteger(Value value) {
    if (value.hasStringValue()) {
      return isInteger(value.getStringValue());
    }

    if (value.hasNumberValue()) {
      double numberValue = value.getNumberValue();
      return numberValue == (int) numberValue && !Double.isInfinite(numberValue);
    }

    return false;
  }

  private boolean containsSecRuleClause(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream()
        .anyMatch(
            clause ->
                clause.hasCustomSecRule()
                    || (clause.hasClauseGroup() && containsSecRuleClause(clause.getClauseGroup())));
  }

  private boolean containsNestedClause(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream().anyMatch(Clause::hasClauseGroup);
  }

  private String getName(Message message) {
    return message.getDescriptorForType().getName();
  }

  private boolean validateValue(Value value) {
    return value.hasStringValue() || value.hasNumberValue();
  }
}
