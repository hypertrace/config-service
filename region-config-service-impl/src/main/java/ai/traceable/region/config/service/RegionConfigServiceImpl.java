package ai.traceable.region.config.service;

import ai.traceable.activity.event.SecurityConfigurationAction;
import ai.traceable.activity.event.SecurityConfigurationChange;
import ai.traceable.activity.event.SecurityConfigurationType;
import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.region.config.service.regions.RegionStore;
import ai.traceable.region.config.service.rules.RulesManager;
import ai.traceable.region.config.service.rules.RulesValidator;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.CreateRegionRuleResponse;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleResponse;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.GetDetailedRegionsResponse;
import ai.traceable.region.config.service.v1.GetRegionRequest;
import ai.traceable.region.config.service.v1.GetRegionResponse;
import ai.traceable.region.config.service.v1.GetRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionsResponse;
import ai.traceable.region.config.service.v1.Region;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceImplBase;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import ai.traceable.region.config.service.v1.UpdateRegionRuleResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class RegionConfigServiceImpl extends RegionConfigServiceImplBase {
  private final RegionStore regionStore;
  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private ActivityEventProducer activityEventProducer;
  private final boolean shouldPublishActivityEvents;

  @Inject
  RegionConfigServiceImpl(
      RegionStore regionStore,
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      RegionConfigServiceConfig config,
      ActivityEventProducer activityEventProducer) {
    this.regionStore = regionStore;
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
    this.activityEventProducer = activityEventProducer;
    this.shouldPublishActivityEvents = config.shouldPublishActivityEvents();
  }

  @Override
  public void getRegions(
      GetRegionsRequest request, StreamObserver<GetRegionsResponse> responseObserver) {

    List<Region> countries =
        regionStore.getCountries(
            request.hasFilter() ? request.getFilter().getIdList() : Collections.emptyList());

    responseObserver.onNext(GetRegionsResponse.newBuilder().addAllRegion(countries).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getDetailedRegions(
      GetDetailedRegionsRequest request,
      StreamObserver<GetDetailedRegionsResponse> responseObserver) {
    List<DetailedRegion> regions =
        regionStore.getDetailedRegions(
            request.hasFilter() ? request.getFilter().getIdList() : Collections.emptyList());

    responseObserver.onNext(GetDetailedRegionsResponse.newBuilder().addAllRegion(regions).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getRegion(
      GetRegionRequest request, StreamObserver<GetRegionResponse> responseObserver) {
    if (request.getId().isEmpty()) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("GetRegion API should have a valid id")
              .asException());
      return;
    }

    Optional<Region> maybeRegion = regionStore.getRegion(request.getId());
    if (maybeRegion.isEmpty()) {
      responseObserver.onError(Status.NOT_FOUND.asException());
      return;
    }

    Region region = maybeRegion.get();
    responseObserver.onNext(GetRegionResponse.newBuilder().setRegion(region).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getAllRegionRules(
      GetAllRegionRulesRequest request,
      StreamObserver<GetAllRegionRulesResponse> responseObserver) {
    List<RegionRule> regionRules = rulesManager.getRegionRules();

    responseObserver.onNext(GetAllRegionRulesResponse.newBuilder().addAllRule(regionRules).build());
    responseObserver.onCompleted();
  }

  @Override
  public void createRegionRule(
      CreateRegionRuleRequest request, StreamObserver<CreateRegionRuleResponse> responseObserver) {
    Status status = rulesValidator.validate(request);
    if (!status.isOk()) {
      responseObserver.onError(status.asException());
      return;
    }

    Optional<RegionRule> maybeRegionRule = rulesManager.createRegionRule(request);
    if (maybeRegionRule.isEmpty()) {
      responseObserver.onError(Status.INTERNAL.asException());
      return;
    }

    RegionRule regionRule = maybeRegionRule.get();
    responseObserver.onNext(CreateRegionRuleResponse.newBuilder().setRule(regionRule).build());
    responseObserver.onCompleted();

    if (shouldPublishActivityEvents) {
      activityEventProducer.publishSecurityConfigurationChangeEvent(
          RequestContext.CURRENT.get(),
          buildSecurityConfigurationChangeEvent(regionRule, SecurityConfigurationAction.ADD));
    }
  }

  @Override
  public void updateRegionRule(
      UpdateRegionRuleRequest request, StreamObserver<UpdateRegionRuleResponse> responseObserver) {
    Status status = rulesValidator.validate(request);
    if (!status.isOk()) {
      responseObserver.onError(status.asException());
      return;
    }

    Optional<RegionRule> maybeUpdatedRegionRule = rulesManager.updateRegionRule(request.getRule());
    if (maybeUpdatedRegionRule.isEmpty()) {
      responseObserver.onError(Status.INTERNAL.asException());
      return;
    }

    RegionRule updatedRegionRule = maybeUpdatedRegionRule.get();
    responseObserver.onNext(
        UpdateRegionRuleResponse.newBuilder().setRule(updatedRegionRule).build());
    responseObserver.onCompleted();

    if (shouldPublishActivityEvents) {
      activityEventProducer.publishSecurityConfigurationChangeEvent(
          RequestContext.CURRENT.get(),
          buildSecurityConfigurationChangeEvent(
              updatedRegionRule, SecurityConfigurationAction.UPDATE));
    }
  }

  @Override
  public void deleteRegionRule(
      DeleteRegionRuleRequest request, StreamObserver<DeleteRegionRuleResponse> responseObserver) {
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        responseObserver.onError(status.asException());
        return;
      }

      String ruleId = request.getId();
      RegionRule deletedRegionRuleConfig =
          rulesManager.deleteRegionRule(RequestContext.CURRENT.get(), ruleId);
      responseObserver.onNext(DeleteRegionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
      if (shouldPublishActivityEvents) {
        activityEventProducer.publishSecurityConfigurationChangeEvent(
            RequestContext.CURRENT.get(),
            buildSecurityConfigurationChangeEvent(
                deletedRegionRuleConfig, SecurityConfigurationAction.REMOVE));
      }
    } catch (Exception e) {
      log.error("Unable to delete region rule with id {} :", request.getId(), e);
      responseObserver.onError(e);
    }
  }

  private SecurityConfigurationChange buildSecurityConfigurationChangeEvent(
      RegionRule regionRuleConfig, SecurityConfigurationAction securityConfigurationAction) {
    return SecurityConfigurationChange.newBuilder()
        .setRuleId(regionRuleConfig.getId())
        .setRuleName(regionRuleConfig.getName())
        .setSecurityConfigurationType(SecurityConfigurationType.LOCATION_RULE)
        .setSecurityConfigurationAction(securityConfigurationAction)
        .build();
  }
}
