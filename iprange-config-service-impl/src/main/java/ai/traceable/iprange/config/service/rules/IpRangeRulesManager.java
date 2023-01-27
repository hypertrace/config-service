package ai.traceable.iprange.config.service.rules;

import static ai.traceable.platform.utils.ip.IpAddressParsingUtils.parseRawIpRange;

import ai.traceable.iprange.config.service.utils.UuidGenerator;
import ai.traceable.iprange.config.service.v1.*;
import ai.traceable.platform.utils.ip.IpAddressParsingUtils.IpParsingResults;
import com.google.inject.Inject;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class IpRangeRulesManager implements RulesManager {
  private final IpRangeRulesStore ipRangeRulesStore;
  private final UuidGenerator uuidGenerator;
  private final Clock clock;

  @Inject
  IpRangeRulesManager(
      IpRangeRulesStore ipRangeRulesStore, UuidGenerator uuidGenerator, Clock clock) {
    this.ipRangeRulesStore = ipRangeRulesStore;
    this.uuidGenerator = uuidGenerator;
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
        parseRawIpRange(createRuleRequest.getRuleDetails().getRawInputIpDataList());

    String ruleId = this.uuidGenerator.generateId();

    IpRangeRule ipRangeRule =
        IpRangeRule.newBuilder()
            .setId(ruleId)
            .setRuleDetails(parseRuleDetails(createRuleRequest.getRuleDetails()))
            .addAllIpRanges(parsedRawIpRange.getIpRanges())
            .addAllIpAddresses(parsedRawIpRange.getIpAddresses())
            .setRuleScope(createRuleRequest.getRuleScope())
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
        parseRawIpRange(updateRuleRequest.getRuleDetails().getRawInputIpDataList());

    IpRangeRule ipRangeRule =
        IpRangeRule.newBuilder()
            .setId(ruleId)
            .setRuleDetails(parseRuleDetails(updateRuleRequest.getRuleDetails()))
            .setDisabled(updateRuleRequest.getDisabled())
            .setInternal(updateRuleRequest.getInternal())
            .addAllIpRanges(parsedRawIpRange.getIpRanges())
            .addAllIpAddresses(parsedRawIpRange.getIpAddresses())
            .setRuleScope(updateRuleRequest.getRuleScope())
            .build();

    return upsertConfig(requestContext, ipRangeRule);
  }

  @Override
  public Optional<IpRangeRule> deleteIpRangeRule(RequestContext requestContext, String id) {
    return ipRangeRulesStore
        .deleteObject(requestContext, id)
        .map(DeletedConfigObject::getDeletedData)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
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
