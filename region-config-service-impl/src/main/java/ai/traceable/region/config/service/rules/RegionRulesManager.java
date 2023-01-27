package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.utils.UuidGenerator;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRule.Builder;
import ai.traceable.region.config.service.v1.RegionRule.ExpirationDetails;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class RegionRulesManager implements RulesManager {
  private final Clock clock;

  private final RegionRulesStore regionRulesStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  RegionRulesManager(Clock clock, RegionRulesStore regionRulesStore, UuidGenerator uuidGenerator) {
    this.clock = clock;
    this.regionRulesStore = regionRulesStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<RegionRule> getRegionRules(
      RequestContext requestContext, GetRegionRulesFilter filter) {
    if (filter.equals(GetRegionRulesFilter.getDefaultInstance())) {
      return regionRulesStore.getAllConfigData(requestContext);
    }
    return regionRulesStore.getAllConfigData(requestContext, filter);
  }

  @Override
  public Optional<RegionRule> createRegionRule(
      RequestContext requestContext, CreateRegionRuleRequest createRuleRequest) {
    String ruleId = this.uuidGenerator.generateId();
    RegionRule regionRule;
    if (createRuleRequest.hasExpirationDetails()) {
      regionRule = createRegionRuleWithExpirationDetails(createRuleRequest, ruleId);
    } else {
      regionRule = createDefaultRegionRule(createRuleRequest, ruleId);
    }
    return upsertConfig(requestContext, regionRule);
  }

  @Override
  public Optional<RegionRule> updateRegionRule(
      RequestContext requestContext, UpdateRegionRuleRequest request) {
    String ruleId = request.getId();
    if (!doesRegionRuleExist(requestContext, ruleId)) {
      return Optional.empty();
    }

    Builder regionRuleBuilder = RegionRule.newBuilder();
    regionRuleBuilder
        .setId(ruleId)
        .setName(request.getName())
        .setDescription(request.getDescription())
        .addAllRegionId(request.getRegionIdList())
        .setActionType(request.getActionType())
        .setDisabled(request.getDisabled())
        .setInternal(request.getInternal())
        .setRuleScope(request.getRuleScope())
        .setEventSeverity(request.getEventSeverity())
        .setConditions(request.getConditions());
    if (request.hasExpirationDetails()) {
      updateExpirationDetails(regionRuleBuilder, request.getExpirationDetails().getDuration());
    }
    RegionRule regionRule = regionRuleBuilder.build();

    return upsertConfig(requestContext, regionRule);
  }

  @Override
  public Optional<RegionRule> deleteRegionRule(RequestContext requestContext, String id) {
    return regionRulesStore
        .deleteObject(requestContext, id)
        .map(DeletedConfigObject::getDeletedData)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }

  private Optional<RegionRule> upsertConfig(RequestContext requestContext, RegionRule regionRule) {
    try {
      return Optional.of(regionRulesStore.upsertObject(requestContext, regionRule).getData());
    } catch (Exception exception) {
      log.error("Unable to upsert region rule {}", regionRule, exception);
      return Optional.empty();
    }
  }

  private RegionRule createRegionRuleWithExpirationDetails(
      CreateRegionRuleRequest createRuleRequest, String ruleId) {
    String duration = createRuleRequest.getExpirationDetails().getDuration();
    RegionRule defaultRegionRule = createDefaultRegionRule(createRuleRequest, ruleId);
    return RegionRule.newBuilder(defaultRegionRule)
        .setExpirationDetails(
            ExpirationDetails.newBuilder()
                .setDuration(duration)
                .setTimestampMillis(clock.millis() + Duration.parse(duration).toMillis())
                .build())
        .setRuleScope(createRuleRequest.getRuleScope())
        .build();
  }

  private RegionRule createDefaultRegionRule(
      CreateRegionRuleRequest createRuleRequest, String ruleId) {
    return RegionRule.newBuilder()
        .setId(ruleId)
        .addAllRegionId(createRuleRequest.getRegionIdList())
        .setName(createRuleRequest.getName())
        .setDescription(createRuleRequest.getDescription())
        .setActionType(createRuleRequest.getActionType())
        .setRuleScope(createRuleRequest.getRuleScope())
        .setEventSeverity(createRuleRequest.getEventSeverity())
        .setConditions(createRuleRequest.getConditions())
        .build();
  }

  private void updateExpirationDetails(Builder regionRuleBuilder, String duration) {
    regionRuleBuilder.setExpirationDetails(
        ExpirationDetails.newBuilder()
            .setDuration(duration)
            .setTimestampMillis(System.currentTimeMillis() + Duration.parse(duration).toMillis())
            .build());
  }

  private boolean doesRegionRuleExist(RequestContext requestContext, String ruleId) {
    Optional<RegionRule> regionRuleOptional = regionRulesStore.getData(requestContext, ruleId);
    return regionRuleOptional.isPresent();
  }
}
