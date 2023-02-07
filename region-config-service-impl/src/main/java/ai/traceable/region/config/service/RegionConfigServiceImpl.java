package ai.traceable.region.config.service;

import ai.traceable.activity.event.SecurityConfigurationAction;
import ai.traceable.activity.event.SecurityConfigurationChange;
import ai.traceable.activity.event.SecurityConfigurationType;
import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.region.config.service.regions.IpqsRegionStore;
import ai.traceable.region.config.service.regions.NeustarRegionStore;
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
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.GetRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionsResponse;
import ai.traceable.region.config.service.v1.Region;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceImplBase;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionsFilter;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import ai.traceable.region.config.service.v1.UpdateRegionRuleResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class RegionConfigServiceImpl extends RegionConfigServiceImplBase {
  private final RegionStore neustarRegionStore;
  private final RegionStore ipqsRegionStore;
  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private ActivityEventProducer activityEventProducer;
  private final boolean shouldPublishActivityEvents;
  private final FeatureCachingClient featureCachingClient;

  @Inject
  RegionConfigServiceImpl(
      NeustarRegionStore neustarRegionStore,
      IpqsRegionStore ipqsRegionStore,
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      RegionConfigServiceConfig config,
      ActivityEventProducer activityEventProducer,
      FeatureCachingClient featureCachingClient) {
    this.neustarRegionStore = neustarRegionStore;
    this.ipqsRegionStore = ipqsRegionStore;
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
    this.activityEventProducer = activityEventProducer;
    this.shouldPublishActivityEvents = config.shouldPublishActivityEvents();
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  public void getRegions(
      GetRegionsRequest request, StreamObserver<GetRegionsResponse> responseObserver) {

    RegionStore regionStore = getRegionStore(RequestContext.CURRENT.get());
    List<Region> countries;

    if (request.hasFilter()) {
      RegionsFilter filter = request.getFilter();
      Status status = rulesValidator.validate(filter);
      if (!status.isOk()) {
        responseObserver.onError(status.asException());
        return;
      }
      countries = regionStore.getCountries(filter.getIdList(), filter.getRegionIdentifierList());
    } else {
      countries = regionStore.getCountries(Collections.emptyList(), Collections.emptyList());
    }

    responseObserver.onNext(GetRegionsResponse.newBuilder().addAllRegion(countries).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getDetailedRegions(
      GetDetailedRegionsRequest request,
      StreamObserver<GetDetailedRegionsResponse> responseObserver) {

    RegionStore regionStore = getRegionStore(RequestContext.CURRENT.get());
    List<DetailedRegion> regions;

    if (request.hasFilter()) {
      RegionsFilter filter = request.getFilter();
      Status status = rulesValidator.validate(filter);
      if (!status.isOk()) {
        responseObserver.onError(status.asException());
        return;
      }
      regions =
          regionStore.getDetailedRegions(filter.getIdList(), filter.getRegionIdentifierList());
    } else {
      regions = regionStore.getDetailedRegions(Collections.emptyList(), Collections.emptyList());
    }

    responseObserver.onNext(GetDetailedRegionsResponse.newBuilder().addAllRegion(regions).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getRegion(
      GetRegionRequest request, StreamObserver<GetRegionResponse> responseObserver) {

    Status status = rulesValidator.validate(request);
    if (!status.isOk()) {
      responseObserver.onError(status.asException());
      return;
    }

    RegionStore regionStore = getRegionStore(RequestContext.CURRENT.get());
    Optional<Region> maybeRegion =
        regionStore.getRegion(request.getId(), request.getRegionIdentifier());

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
    RequestContext requestContext = RequestContext.CURRENT.get();
    List<RegionRule> regionRules = rulesManager.getRegionRules(requestContext, request.getFilter());
    regionRules = populateRegionMapping(regionRules, requestContext);

    responseObserver.onNext(GetAllRegionRulesResponse.newBuilder().addAllRule(regionRules).build());
    responseObserver.onCompleted();
  }

  @Override
  public void createRegionRule(
      CreateRegionRuleRequest request, StreamObserver<CreateRegionRuleResponse> responseObserver) {

    RequestContext requestContext = RequestContext.CURRENT.get();
    Status status = rulesValidator.validate(request, getAllRegionsRulesSupplier(requestContext));
    if (!status.isOk()) {
      responseObserver.onError(status.asException());
      return;
    }

    Optional<RegionRule> regionRuleOptional =
        rulesManager.createRegionRule(requestContext, request);
    if (regionRuleOptional.isEmpty()) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(String.format("Unable to create region rule %s", request.getName()))
              .asException());
      return;
    }

    RegionRule regionRule = regionRuleOptional.get();
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
    RequestContext requestContext = RequestContext.CURRENT.get();
    Status status = rulesValidator.validate(request, getAllRegionsRulesSupplier(requestContext));
    if (!status.isOk()) {
      responseObserver.onError(status.asException());
      return;
    }

    Optional<RegionRule> regionRuleOptional =
        rulesManager.updateRegionRule(requestContext, request);
    if (regionRuleOptional.isEmpty()) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format("Unable to update region rule with id %s", request.getId()))
              .asException());
      return;
    }

    RegionRule regionRule = regionRuleOptional.get();
    responseObserver.onNext(UpdateRegionRuleResponse.newBuilder().setRule(regionRule).build());
    responseObserver.onCompleted();

    if (shouldPublishActivityEvents) {
      activityEventProducer.publishSecurityConfigurationChangeEvent(
          RequestContext.CURRENT.get(),
          buildSecurityConfigurationChangeEvent(regionRule, SecurityConfigurationAction.UPDATE));
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
      Optional<RegionRule> deletedRegionRuleConfig =
          rulesManager.deleteRegionRule(RequestContext.CURRENT.get(), ruleId);
      responseObserver.onNext(DeleteRegionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
      if (shouldPublishActivityEvents && deletedRegionRuleConfig.isPresent()) {
        activityEventProducer.publishSecurityConfigurationChangeEvent(
            RequestContext.CURRENT.get(),
            buildSecurityConfigurationChangeEvent(
                deletedRegionRuleConfig.get(), SecurityConfigurationAction.REMOVE));
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

  private Supplier<List<RegionRule>> getAllRegionsRulesSupplier(RequestContext requestContext) {
    return () ->
        rulesManager.getRegionRules(requestContext, GetRegionRulesFilter.getDefaultInstance());
  }

  private RegionStore getRegionStore(RequestContext requestContext) {
    if (featureCachingClient.isIpqsEnabledForRegionToIpMapping(requestContext)) {
      return ipqsRegionStore;
    }
    return neustarRegionStore;
  }

  private List<RegionRule> populateRegionMapping(
      List<RegionRule> regionRules, RequestContext requestContext) {
    Set<String> regionIds =
        regionRules.stream()
            .flatMap(regionRule -> regionRule.getRegionIdList().stream())
            .collect(Collectors.toUnmodifiableSet());
    RegionStore regionStore = getRegionStore(requestContext);
    Map<String, String> regionMapping =
        regionStore.getCountries(new ArrayList<>(regionIds), Collections.emptyList()).stream()
            .collect(Collectors.toUnmodifiableMap(Region::getId, Region::getName, (v1, v2) -> v1));

    return regionRules.stream()
        .map(regionRule -> populateRegionMapping(regionRule, regionMapping))
        .collect(Collectors.toUnmodifiableList());
  }

  private RegionRule populateRegionMapping(
      RegionRule regionRule, Map<String, String> regionMapping) {
    List<String> regionIds = regionRule.getRegionIdList();
    Map<String, String> regionIdToNameMap =
        regionIds.stream()
            .filter(regionMapping::containsKey)
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(), regionMapping::get, (v1, v2) -> v1));

    return regionRule.toBuilder().putAllRegionIdToNameMap(regionIdToNameMap).build();
  }
}
