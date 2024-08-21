package ai.traceable.malicioussources.config.service.rules;

import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.EmailDomainCondition;
import ai.traceable.malicioussources.config.service.v1.EmailFraudScore;
import ai.traceable.malicioussources.config.service.v1.EmailFraudScoreLevel;
import ai.traceable.malicioussources.config.service.v1.EventSeverity;
import ai.traceable.malicioussources.config.service.v1.IpAddressCondition;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.IpReputationCondition;
import ai.traceable.malicioussources.config.service.v1.IpReputationSeverity;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.RegionCondition;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import ai.traceable.platform.utils.ip.IpValidationUtils;
import io.grpc.Status;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public class MaliciousSourcesRulesValidator implements RulesValidator {

  @Override
  public Status validate(
      CreateMaliciousSourcesRuleRequest request, List<MaliciousSourcesRule> existingRules) {
    if (request.getRuleInfo().getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "MaliciousSourcesRuleInfo in Malicious Source rule should have a valid name");
    }
    Optional<MaliciousSourcesRule> existingRuleWithSameName =
        getRuleForName(request.getRuleInfo().getName(), existingRules);
    if (existingRuleWithSameName.isPresent()) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format("Rule with name %s already exists", request.getRuleInfo().getName()));
    }
    Status status = validate(request.getRuleInfo());
    if (!status.isOk()) {
      return status;
    }
    status = validate(request.getRuleScope());
    if (!status.isOk()) {
      return status;
    }
    List<MaliciousSourcesRule> blockAllExceptRulesSupplier =
        existingRules.stream()
            .filter(
                rule ->
                    rule.getRuleInfo()
                        .getRuleAction()
                        .getActionType()
                        .equals(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT))
            .collect(Collectors.toUnmodifiableList());
    if (RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT.equals(
            request.getRuleInfo().getRuleAction().getActionType())
        && isDuplicateBlockAllExceptCreate(
            request.getRuleScope().getEnvironmentScope().getEnvironmentIdsList(),
            () -> blockAllExceptRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to create duplicate rule for action "
              + RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT);
    }
    return Status.OK;
  }

  @Override
  public Status validate(
      UpdateMaliciousSourcesRuleRequest request, List<MaliciousSourcesRule> existingRules) {
    MaliciousSourcesRule maliciousSourcesRule = request.getRule();
    if (maliciousSourcesRule.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Update Malicious Sources rule should have a valid id");
    }
    if (request.getRule().getRuleInfo().getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "MaliciousSourcesRuleInfo in Malicious Source rule should have a valid name");
    }
    Optional<MaliciousSourcesRule> existingRuleWithSameName =
        getRuleForName(request.getRule().getRuleInfo().getName(), existingRules);
    if (existingRuleWithSameName.isPresent()
        && !existingRuleWithSameName.get().getId().equals(request.getRule().getId())) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Rule with name %s already exists", request.getRule().getRuleInfo().getName()));
    }
    Status status = validate(maliciousSourcesRule.getRuleInfo());
    if (!status.isOk()) {
      return status;
    }
    status = validate(maliciousSourcesRule.getRuleScope());
    if (!status.isOk()) {
      return status;
    }
    List<MaliciousSourcesRule> blockAllExceptRulesSupplier =
        existingRules.stream()
            .filter(
                rule ->
                    rule.getRuleInfo()
                        .getRuleAction()
                        .getActionType()
                        .equals(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT))
            .collect(Collectors.toUnmodifiableList());
    if (RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT.equals(
            maliciousSourcesRule.getRuleInfo().getRuleAction().getActionType())
        && isDuplicateBlockAllExceptUpdate(
            maliciousSourcesRule.getId(),
            maliciousSourcesRule.getRuleScope().getEnvironmentScope().getEnvironmentIdsList(),
            () -> blockAllExceptRulesSupplier)) {
      return Status.ALREADY_EXISTS.withDescription(
          "Trying to change rule action to "
              + RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT
              + ". Rule of this type already exists");
    }
    return Status.OK;
  }

  @Override
  public Status validate(DeleteMaliciousSourcesRuleRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Delete Malicious Source rule should have a valid id");
    }
    return Status.OK;
  }

  private Status validate(MaliciousSourcesRuleInfo ruleInfo) {
    Status status = validate(ruleInfo.getRuleAction());
    if (!status.isOk()) {
      return status;
    }
    if (ruleInfo.getConditionsCount() == 0) {
      return Status.NOT_FOUND.withDescription(
          "MaliciousSourcesRule should have at least one valid condition");
    }
    RuleActionType actionType = ruleInfo.getRuleAction().getActionType();
    for (MaliciousSourcesRuleCondition condition : ruleInfo.getConditionsList()) {
      status = validate(condition, actionType);
      if (!status.isOk()) {
        return status;
      }
    }
    return Status.OK;
  }

  private Status validate(MaliciousSourcesRuleCondition ruleCondition, RuleActionType actionType) {
    switch (ruleCondition.getConditionCase()) {
      case IP_LOCATION_TYPE_CONDITION:
        return validate(ruleCondition.getIpLocationTypeCondition(), actionType);
      case IP_REPUTATION_CONDITION:
        return validate(ruleCondition.getIpReputationCondition());
      case IP_RANGE_CONDITION:
        return validate(ruleCondition.getIpRangeCondition());
      case REGION_CONDITION:
        return validate(ruleCondition.getRegionCondition(), actionType);
      case EMAIL_DOMAIN_CONDITION:
        return validate(ruleCondition.getEmailDomainCondition(), actionType);
      default:
        return Status.INVALID_ARGUMENT.withDescription(
            "MaliciousSourcesRuleCondition should have a valid condition");
    }
  }

  private Status validate(IpAddressCondition ipAddressCondition) {
    if (ipAddressCondition.getCidrIpRangesCount() == 0
        && ipAddressCondition.getIpAddressesCount() == 0) {
      return Status.NOT_FOUND.withDescription(
          "IpAddressCondition in Malicious Sources rule must have at least one CIDR or IP address");
    }

    if (!ipAddressCondition.getIpAddressesList().stream()
        .allMatch(IpValidationUtils::isValidIpAddress)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IpAddressCondition in Malicious Sources rule should have a valid IP address");
    }

    if (!ipAddressCondition.getCidrIpRangesList().stream()
        .allMatch(IpValidationUtils::isIpAddressRangeInCIDRWithHostBitsZero)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IpAddressCondition in Malicious Sources rule should have a valid CIDR address with host bits zero");
    }

    return Status.OK;
  }

  private Status validate(IpReputationCondition ipReputationCondition) {
    switch (ipReputationCondition.getReputationCase()) {
      case MIN_IP_REPUTATION_SCORE:
        if (ipReputationCondition.getMinIpReputationScore() < 0) {
          return Status.INVALID_ARGUMENT.withDescription(
              "IpReputationCondition in Malicious Sources rule should have IpReputationScore > 0");
        }
        return Status.OK;
      case MIN_IP_REPUTATION_SEVERITY:
        if (ipReputationCondition.getMinIpReputationSeverity()
            == IpReputationSeverity.IP_REPUTATION_SEVERITY_UNSPECIFIED) {
          return Status.INVALID_ARGUMENT.withDescription(
              "IpReputationCondition in Malicious Sources rule should have a valid IP Reputation Severity");
        }
        return Status.OK;
      default:
        return Status.NOT_FOUND.withDescription(
            "IpReputationCondition in Malicious Sources rule should have either IpReputationScore or IpReputationSeverity");
    }
  }

  private Status validate(RegionCondition regionCondition, RuleActionType actionType) {
    if (regionCondition.getRegionsCount() == 0) {
      return Status.NOT_FOUND.withDescription(
          "RegionCondition in Malicious Sources rule should not have empty Region list");
    }
    if (actionType.equals(RuleActionType.RULE_ACTION_TYPE_ALLOW)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Malicious Source rule with condition type {} should not have rule action type {}",
              regionCondition,
              actionType));
    }

    Set<String> fourLettersIsoCountriesCodes = Locale.getISOCountries(Locale.IsoCountryCode.PART3);
    Set<String> threeLettersIsoCountriesCodes =
        Locale.getISOCountries(Locale.IsoCountryCode.PART1_ALPHA3);
    Set<String> twoLettersIsoCountriesCodes =
        Locale.getISOCountries(Locale.IsoCountryCode.PART1_ALPHA2);

    if (regionCondition.getRegionsList().stream()
        .anyMatch(
            region -> {
              String isoCode = region.getCountryIsoCode();
              return !fourLettersIsoCountriesCodes.contains(isoCode)
                  && !threeLettersIsoCountriesCodes.contains(isoCode)
                  && !twoLettersIsoCountriesCodes.contains(isoCode);
            })) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Region in Malicious Sources rule should have valid ISO code");
    }
    return Status.OK;
  }

  private Status validate(
      IpLocationTypeCondition ipLocationTypeCondition, RuleActionType actionType) {
    if (ipLocationTypeCondition.getIpLocationTypesList().isEmpty()) {
      return Status.NOT_FOUND.withDescription(
          "IpLocationTypeCondition in Malicious Sources rule should have a empty IP location type list");
    }
    if (actionType.equals(RuleActionType.RULE_ACTION_TYPE_ALLOW)
        || actionType.equals(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Malicious Source rule with condition type {} should not have rule action type {}",
              ipLocationTypeCondition,
              actionType));
    }
    if (ipLocationTypeCondition.getIpLocationTypesCount() != 0
        && ipLocationTypeCondition.getIpLocationTypesList().stream()
            .anyMatch(type -> type == IpLocationType.IP_LOCATION_TYPE_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "IpLocationTypeCondition in Malicious Sources rule should have a valid IP location type");
    }
    return Status.OK;
  }

  private Status validate(EmailDomainCondition emailDomainCondition, RuleActionType actionType) {
    if (!emailDomainCondition.getDataLeakedEmail()
        && !emailDomainCondition.getDisposableEmailDomain()
        && !emailDomainCondition.hasEmailFraudScore()
        && emailDomainCondition.getEmailRegexesCount() == 0
        && emailDomainCondition.getEmailDomainsCount() == 0) {
      return Status.NOT_FOUND.withDescription(
          "EmailDomainCondition in Malicious Sources rule should have at least one condition");
    }
    if (actionType.equals(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Malicious Source rule with condition type {} should not have rule action type {}",
              emailDomainCondition,
              actionType));
    }
    Status status;
    if (emailDomainCondition.hasEmailFraudScore()) {
      status = validate(emailDomainCondition.getEmailFraudScore());
      if (!status.isOk()) {
        return status;
      }
    }
    for (String regex : emailDomainCondition.getEmailRegexesList()) {
      status = validateRegex(regex);
      if (!status.isOk()) {
        return status;
      }
    }
    return Status.OK;
  }

  private Status validate(MaliciousSourcesRuleScope scope) {
    if (scope.hasEnvironmentScope()) {
      List<String> environmentIdList = scope.getEnvironmentScope().getEnvironmentIdsList();
      if (environmentIdList.isEmpty()) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Environment scope should have at least one environment");
      } else {
        return environmentIdList.stream().anyMatch(String::isEmpty)
            ? Status.INVALID_ARGUMENT.withDescription("Environment id should not be empty string.")
            : Status.OK;
      }
    } else {
      return Status.OK;
    }
  }

  private Status validate(MaliciousSourcesRuleAction ruleAction) {
    if (ruleAction.getActionType() == RuleActionType.RULE_ACTION_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "MaliciousSourcesRuleAction in Malicious Sources rule should have a valid action type");
    }
    if (ruleAction.getActionType() != RuleActionType.RULE_ACTION_TYPE_ALLOW
        && ruleAction.getEventSeverity() == EventSeverity.EVENT_SEVERITY_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "MaliciousSourcesRuleAction in Malicious Sources rule with action other than ALLOW should have a valid severity");
    }
    return Status.OK;
  }

  private Status validateRegex(String regexPattern) {
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern.compile(regexPattern);
      return Status.OK;
    } catch (PatternSyntaxException e) {
      return Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription("Invalid Regex Value for the Email Domain Condition");
    }
  }

  private Status validate(EmailFraudScore emailFraudScore) {
    switch (emailFraudScore.getFraudScoreCase()) {
      case MIN_EMAIL_FRAUD_SCORE:
        if (emailFraudScore.getMinEmailFraudScore() < 0) {
          return Status.INVALID_ARGUMENT.withDescription(
              "EmailDomainCondition Malicious Sources rule should have EmailFraudScore > 0");
        }
        return Status.OK;
      case MIN_EMAIL_FRAUD_SCORE_LEVEL:
        if (emailFraudScore.getMinEmailFraudScoreLevel()
            == EmailFraudScoreLevel.EMAIL_FRAUD_SCORE_LEVEL_UNSPECIFIED) {
          return Status.INVALID_ARGUMENT.withDescription(
              "EmailDomainCondition Malicious Sources rule should have a valid EmailFraudScoreLevel");
        }
        return Status.OK;
      default:
        return Status.NOT_FOUND.withDescription(
            "EmailDomainCondition Malicious Sources rule should have either min EmailFraudScore or EmailFraudScoreLevel");
    }
  }

  private boolean isDuplicateBlockAllExceptCreate(
      List<String> environmentIds,
      Supplier<List<MaliciousSourcesRule>> blockAllExceptRulesSupplier) {
    return blockAllExceptRulesSupplier.get().stream()
        .anyMatch(rule -> isDuplicateBlockAllExceptRule(rule, environmentIds));
  }

  private boolean isDuplicateBlockAllExceptUpdate(
      String id,
      List<String> environmentIds,
      Supplier<List<MaliciousSourcesRule>> blockAllExceptRulesSupplier) {
    List<MaliciousSourcesRule> maliciousSourcesRules = blockAllExceptRulesSupplier.get();
    return maliciousSourcesRules.stream()
        .anyMatch(
            rule ->
                !rule.getId().equals(id) && isDuplicateBlockAllExceptRule(rule, environmentIds));
  }

  private boolean isDuplicateBlockAllExceptRule(
      MaliciousSourcesRule rule, List<String> environmentIds) {
    List<String> ruleEnvironmentIds =
        rule.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    return // Not allowing all environments block all except in case a rule already exists
    environmentIds.isEmpty()
        || // Not allowing any block all except rule if already an all environments one exists
        ruleEnvironmentIds.isEmpty()
        || // Only allowing another block all except rule if it has no environment intersection
        // with an existing rule
        ruleEnvironmentIds.stream().anyMatch(environmentIds::contains);
  }

  private Optional<MaliciousSourcesRule> getRuleForName(
      String ruleName, List<MaliciousSourcesRule> rules) {
    return rules.stream().filter(rule -> rule.getRuleInfo().getName().equals(ruleName)).findFirst();
  }
}
