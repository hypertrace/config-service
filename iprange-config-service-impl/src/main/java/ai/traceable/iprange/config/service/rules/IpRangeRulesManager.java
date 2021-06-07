package ai.traceable.iprange.config.service.rules;

import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_CONFIG_NAMESPACE;
import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.iprange.config.service.utils.IpValidationUtils;
import ai.traceable.iprange.config.service.utils.UuidGenerator;
import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class IpRangeRulesManager implements RulesManager {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final IpRangeRuleConverter ipRangeRuleConverter;
  private final UuidGenerator uuidGenerator;
  private final IpValidationUtils ipValidationUtils;

  @Inject
  IpRangeRulesManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      IpRangeRuleConverter ipRangeRuleConverter,
      UuidGenerator uuidGenerator,
      IpValidationUtils ipValidationUtils) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.ipRangeRuleConverter = ipRangeRuleConverter;
    this.uuidGenerator = uuidGenerator;
    this.ipValidationUtils = ipValidationUtils;
  }

  @Override
  public List<IpRangeRule> getIpRangeRules(RequestContext requestContext, GetRulesFilter filter)
      throws RuntimeException {
    GetAllConfigsRequest getAllRuleConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceNamespace(IPRANGE_RULE_CONFIG_NAMESPACE)
            .setResourceName(IPRANGE_RULE_CONFIG_RESOURCE_NAME)
            .build();

    List<ContextSpecificConfig> contextSpecificConfigs =
        requestContext.call(
            () ->
                configServiceBlockingStub
                    .getAllConfigs(getAllRuleConfigsRequest)
                    .getContextSpecificConfigsList());

    List<IpRangeRule> ipRangeRules = new ArrayList<>();

    contextSpecificConfigs.forEach(
        contextSpecificConfig -> {
          try {
            IpRangeRule ipRangeRule =
                ipRangeRuleConverter.convert(contextSpecificConfig.getConfig());
            ipRangeRules.add(ipRangeRule);
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(
                String.format(
                    "Unable to convert config to ip range rule for rule id: %s",
                    contextSpecificConfig.getContext()),
                e);
          }
        });

    if (filter != GetRulesFilter.getDefaultInstance()) {
      return ipRangeRules.stream()
          .filter(
              rule -> {
                boolean ruleIdAccept =
                    filter.getRuleIdsCount() == 0 || filter.getRuleIdsList().contains(rule.getId());

                boolean ruleActionTypeAccept =
                    !(filter.hasRuleAction()
                        && rule.getRuleDetails().getRuleAction() != filter.getRuleAction());

                boolean disabledAccept =
                    !(filter.hasDisabled() && rule.getDisabled() != filter.getDisabled());

                boolean internalAccept =
                    !(filter.hasInternal() && rule.getInternal() != filter.getInternal());
                return ruleIdAccept && ruleActionTypeAccept && disabledAccept && internalAccept;
              })
          .collect(ImmutableList.toImmutableList());
    }
    return ImmutableList.copyOf(ipRangeRules);
  }

  @Override
  public IpRangeRule createIpRangeRule(
      RequestContext requestContext, CreateIpRangeRuleRequest createRuleRequest) {

    Object[] parsedRawIpRange =
        parseRawIpRange(createRuleRequest.getRuleDetails().getRawInputIpDataList());
    Set<String> ipAddresses = (Set<String>) parsedRawIpRange[0];
    Set<String> ipRanges = (Set<String>) parsedRawIpRange[1];

    String ruleId = this.uuidGenerator.generateId();

    IpRangeRule ipRangeRule =
        IpRangeRule.newBuilder()
            .setId(ruleId)
            .setRuleDetails(createRuleRequest.getRuleDetails())
            .addAllIpRanges(ipRanges)
            .addAllIpAddresses(ipAddresses)
            .build();

    return upsertConfig(requestContext, ipRangeRule);
  }

  @Override
  public IpRangeRule updateIpRangeRule(
      RequestContext requestContext, UpdateIpRangeRuleRequest updateRuleRequest) {
    String ruleId = updateRuleRequest.getId();
    if (!doesIpRangeRuleExist(requestContext, ruleId)) {
      throw new NoSuchElementException(
          String.format("Unable to update as ip range rule with id = %s does not exist", ruleId));
    }

    Object[] parsedRawIpRange =
        parseRawIpRange(updateRuleRequest.getRuleDetails().getRawInputIpDataList());
    Set<String> ipAddresses = (Set<String>) parsedRawIpRange[0];
    Set<String> ipRanges = (Set<String>) parsedRawIpRange[1];

    IpRangeRule ipRangeRule =
        IpRangeRule.newBuilder()
            .setId(ruleId)
            .setRuleDetails(updateRuleRequest.getRuleDetails())
            .setDisabled(updateRuleRequest.getDisabled())
            .setInternal(updateRuleRequest.getInternal())
            .addAllIpRanges(ipRanges)
            .addAllIpAddresses(ipAddresses)
            .build();

    return upsertConfig(requestContext, ipRangeRule);
  }

  @Override
  public void deleteIpRangeRule(RequestContext requestContext, String id) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceNamespace(IPRANGE_RULE_CONFIG_NAMESPACE)
            .setResourceName(IPRANGE_RULE_CONFIG_RESOURCE_NAME)
            .setContext(id)
            .build();

    requestContext.call(() -> configServiceBlockingStub.deleteConfig(deleteConfigRequest));
  }

  private IpRangeRule upsertConfig(RequestContext requestContext, IpRangeRule ipRangeRule) {
    UpsertConfigRequest upsertConfigRequest;
    try {
      upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setResourceNamespace(IPRANGE_RULE_CONFIG_NAMESPACE)
              .setResourceName(IPRANGE_RULE_CONFIG_RESOURCE_NAME)
              .setConfig(ipRangeRuleConverter.convert(ipRangeRule))
              .setContext(ipRangeRule.getId())
              .build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          String.format("Unable to convert ip range rule %s to config object", ipRangeRule), e);
    }

    UpsertConfigResponse response;
    try {
      response =
          requestContext.call(() -> configServiceBlockingStub.upsertConfig(upsertConfigRequest));
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Unable to insert ip range rule config in data for request %s", ipRangeRule),
          e);
    }

    try {
      return ipRangeRuleConverter.convert(response.getConfig());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          String.format("Unable to convert config response: %s back to ip range rule", response),
          e);
    }
  }

  private boolean doesIpRangeRuleExist(RequestContext requestContext, String ruleId) {
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .addContexts(ruleId)
              .setResourceNamespace(IPRANGE_RULE_CONFIG_NAMESPACE)
              .setResourceName(IPRANGE_RULE_CONFIG_RESOURCE_NAME)
              .build();

      Optional<Value> parsedValue =
          Optional.ofNullable(
                  requestContext.call(
                      () -> configServiceBlockingStub.getConfig(getConfigRequest).getConfig()))
              .filter(valuae -> valuae.getKindCase() != Value.KindCase.KIND_NOT_SET);
      return parsedValue.isPresent();
    } catch (Exception e) {
      return false;
    }
  }

  private Object[] parseRawIpRange(List<String> rawIpRanges) {
    Set<String> ipAddresses = new HashSet<>();
    Set<String> ipRanges = new HashSet<>();
    rawIpRanges.forEach(
        rawIpRange -> {
          if (ipValidationUtils.isValidIp(rawIpRange)) {
            ipAddresses.add(rawIpRange);
          } else if (ipValidationUtils.isValidSubnet(rawIpRange)) {
            ipRanges.add(rawIpRange);
          } else {
            throw new IllegalArgumentException(
                "IP range rule should have valid IP addresses and/or valid IP ranges in CIDR format");
          }
        });
    return new Object[] {ipAddresses, ipRanges};
  }
}
