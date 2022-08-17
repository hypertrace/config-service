package ai.traceable.iprange.config.service.rules;

import ai.traceable.iprange.config.service.utils.IpValidationUtils;
import ai.traceable.iprange.config.service.utils.UuidGenerator;
import ai.traceable.iprange.config.service.v1.*;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class IpRangeRulesManager implements RulesManager {
  private final IpRangeRulesStore ipRangeRulesStore;
  private final UuidGenerator uuidGenerator;
  private final IpValidationUtils ipValidationUtils;
  private final Clock clock;

  @Inject
  IpRangeRulesManager(
      IpRangeRulesStore ipRangeRulesStore,
      UuidGenerator uuidGenerator,
      IpValidationUtils ipValidationUtils,
      Clock clock) {
    this.ipRangeRulesStore = ipRangeRulesStore;
    this.uuidGenerator = uuidGenerator;
    this.ipValidationUtils = ipValidationUtils;
    this.clock = clock;
  }

  @Override
  public List<IpRangeRule> getIpRangeRules(RequestContext requestContext, GetRulesFilter filter) {

    List<IpRangeRule> ipRangeRules =
        ipRangeRulesStore.getAllObjects(requestContext).stream()
            .map(ConfigObject::getData)
            .collect(Collectors.toList());

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
            .setRuleDetails(parseRuleDetails(createRuleRequest.getRuleDetails()))
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
            .setRuleDetails(parseRuleDetails(updateRuleRequest.getRuleDetails()))
            .setDisabled(updateRuleRequest.getDisabled())
            .setInternal(updateRuleRequest.getInternal())
            .addAllIpRanges(ipRanges)
            .addAllIpAddresses(ipAddresses)
            .build();

    return upsertConfig(requestContext, ipRangeRule);
  }

  @Override
  public IpRangeRule deleteIpRangeRule(RequestContext requestContext, String id) {

    return ipRangeRulesStore
        .deleteObject(requestContext, id)
        .map(ConfigObject::getData)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  private IpRangeRule upsertConfig(RequestContext requestContext, IpRangeRule ipRangeRule) {
    return ipRangeRulesStore.upsertObject(requestContext, ipRangeRule).getData();
  }

  private boolean doesIpRangeRuleExist(RequestContext requestContext, String ruleId) {
    Optional<IpRangeRule> optionalRule = ipRangeRulesStore.getData(requestContext, ruleId);
    return optionalRule.isPresent();
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

  private IpRangeRuleDetails parseRuleDetails(IpRangeRuleDetails ipRangeRuleDetails) {
    if (ipRangeRuleDetails.getExpirationDetails().hasExpirationDuration()
        && !ipRangeRuleDetails.getExpirationDetails().hasExpirationTimestampMillis()) {
      IpRangeRuleDetails.Builder builder = ipRangeRuleDetails.toBuilder();
      builder
          .getExpirationDetailsBuilder()
          .setExpirationTimestampMillis(
              clock.millis()
                  + Duration.parse(
                          ipRangeRuleDetails.getExpirationDetails().getExpirationDuration())
                      .toMillis());
      return builder.build();
    }
    return ipRangeRuleDetails;
  }
}
