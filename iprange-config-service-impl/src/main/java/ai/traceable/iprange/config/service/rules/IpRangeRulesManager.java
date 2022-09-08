package ai.traceable.iprange.config.service.rules;

import ai.traceable.config.utils.IpAddressParsingUtils;
import ai.traceable.config.utils.IpAddressParsingUtils.IpParsingResults;
import ai.traceable.iprange.config.service.utils.UuidGenerator;
import ai.traceable.iprange.config.service.v1.*;
import com.google.inject.Inject;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class IpRangeRulesManager implements RulesManager {
  private final IpRangeRulesStore ipRangeRulesStore;
  private final UuidGenerator uuidGenerator;
  private final IpAddressParsingUtils ipAddressParsingUtils;
  private final Clock clock;

  @Inject
  IpRangeRulesManager(
      IpRangeRulesStore ipRangeRulesStore,
      UuidGenerator uuidGenerator,
      IpAddressParsingUtils ipAddressParsingUtils,
      Clock clock) {
    this.ipRangeRulesStore = ipRangeRulesStore;
    this.uuidGenerator = uuidGenerator;
    this.ipAddressParsingUtils = ipAddressParsingUtils;
    this.clock = clock;
  }

  @Override
  public List<IpRangeRule> getIpRangeRules(RequestContext requestContext, GetRulesFilter filter) {

    if (filter.equals(GetRulesFilter.getDefaultInstance())) {
      return ipRangeRulesStore.getAllConfigData(requestContext);
    }
    return ipRangeRulesStore.getAllConfigData(requestContext, filter);
  }

  @Override
  public IpRangeRule createIpRangeRule(
      RequestContext requestContext, CreateIpRangeRuleRequest createRuleRequest) {
    IpParsingResults parsedRawIpRange =
        ipAddressParsingUtils.parseRawIpRange(
            createRuleRequest.getRuleDetails().getRawInputIpDataList());

    String ruleId = this.uuidGenerator.generateId();

    IpRangeRule ipRangeRule =
        IpRangeRule.newBuilder()
            .setId(ruleId)
            .setRuleDetails(parseRuleDetails(createRuleRequest.getRuleDetails()))
            .addAllIpRanges(parsedRawIpRange.getIpRanges())
            .addAllIpAddresses(parsedRawIpRange.getIpAddresses())
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

    IpParsingResults parsedRawIpRange =
        ipAddressParsingUtils.parseRawIpRange(
            updateRuleRequest.getRuleDetails().getRawInputIpDataList());

    IpRangeRule ipRangeRule =
        IpRangeRule.newBuilder()
            .setId(ruleId)
            .setRuleDetails(parseRuleDetails(updateRuleRequest.getRuleDetails()))
            .setDisabled(updateRuleRequest.getDisabled())
            .setInternal(updateRuleRequest.getInternal())
            .addAllIpRanges(parsedRawIpRange.getIpRanges())
            .addAllIpAddresses(parsedRawIpRange.getIpAddresses())
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
