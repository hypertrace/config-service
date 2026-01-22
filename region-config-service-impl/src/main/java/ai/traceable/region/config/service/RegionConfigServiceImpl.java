package ai.traceable.region.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.region.config.service.regions.IpqsRegionStore;
import ai.traceable.region.config.service.regions.IpqsResolvedWithNeustarRegionStore;
import ai.traceable.region.config.service.regions.NeustarRegionStore;
import ai.traceable.region.config.service.regions.RegionStore;
import ai.traceable.region.config.service.rules.RulesManager;
import ai.traceable.region.config.service.rules.RulesValidator;
import ai.traceable.region.config.service.v1.BulkDeleteRegionRulesRequest;
import ai.traceable.region.config.service.v1.BulkDeleteRegionRulesResponse;
import ai.traceable.region.config.service.v1.Country;
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
import ai.traceable.region.config.service.v1.RegionRuleRecord;
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
  private final FeatureCachingClient featureCachingClient;
  private final IpqsResolvedWithNeustarRegionStore ipqsResolvedWithNeustarRegionStore;
  private final boolean ipqsNeustarResolutionEnabled; // only considered when ipqs is enabled

  @Inject
  RegionConfigServiceImpl(
      NeustarRegionStore neustarRegionStore,
      IpqsRegionStore ipqsRegionStore,
      IpqsResolvedWithNeustarRegionStore ipqsResolvedWithNeustarRegionStore,
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      RegionConfigServiceConfig config,
      FeatureCachingClient featureCachingClient) {
    this.neustarRegionStore = neustarRegionStore;
    this.ipqsRegionStore = ipqsRegionStore;
    this.ipqsResolvedWithNeustarRegionStore = ipqsResolvedWithNeustarRegionStore;
    this.ipqsNeustarResolutionEnabled = config.getIpqsNeustarResolutionEnabled();
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
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
    List<RegionRuleRecord> ruleRecords =
        rulesManager.getRegionRuleRecords(requestContext, request.getFilter());
    ruleRecords = populateRegionMapping(ruleRecords, requestContext);
    List<RegionRule> regionRules =
        ruleRecords.stream().map(RegionRuleRecord::getRule).collect(Collectors.toList());
    responseObserver.onNext(
        GetAllRegionRulesResponse.newBuilder()
            .addAllRule(regionRules)
            .addAllRuleRecords(ruleRecords)
            .build());
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
      rulesManager.deleteRegionRule(RequestContext.CURRENT.get(), ruleId);
      responseObserver.onNext(DeleteRegionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to delete region rule with id {} :", request.getId(), e);
      responseObserver.onError(e);
    }
  }

  private Supplier<List<RegionRule>> getAllRegionsRulesSupplier(RequestContext requestContext) {
    return () ->
        rulesManager.getRegionRules(requestContext, GetRegionRulesFilter.getDefaultInstance());
  }

  private RegionStore getRegionStore(RequestContext requestContext) {
    if (featureCachingClient.isIpqsEnabledForRegionToIpMapping(requestContext)) {
      return ipqsNeustarResolutionEnabled ? ipqsResolvedWithNeustarRegionStore : ipqsRegionStore;
    }
    return neustarRegionStore;
  }

  private List<RegionRuleRecord> populateRegionMapping(
      List<RegionRuleRecord> ruleRecords, RequestContext requestContext) {
    Set<String> regionIds =
        ruleRecords.stream()
            .map(RegionRuleRecord::getRule)
            .flatMap(regionRule -> regionRule.getRegionIdList().stream())
            .collect(Collectors.toUnmodifiableSet());
    RegionStore regionStore = getRegionStore(requestContext);
    Map<String, Country> regionMapping =
        regionStore.getCountries(new ArrayList<>(regionIds), Collections.emptyList()).stream()
            .collect(
                Collectors.toUnmodifiableMap(Region::getId, Region::getCountry, (v1, v2) -> v1));

    return ruleRecords.stream()
        .map(ruleRecord -> populateRegionMapping(ruleRecord, regionMapping))
        .collect(Collectors.toUnmodifiableList());
  }

  private RegionRuleRecord populateRegionMapping(
      RegionRuleRecord ruleRecord, Map<String, Country> regionMapping) {
    RegionRule enrichedRegionRule = populateRegionMapping(ruleRecord.getRule(), regionMapping);
    return ruleRecord.toBuilder().setRule(enrichedRegionRule).build();
  }

  private RegionRule populateRegionMapping(
      RegionRule regionRule, Map<String, Country> regionMapping) {
    List<String> regionIds = regionRule.getRegionIdList();
    Map<String, String> regionIdToNameMap =
        regionIds.stream()
            .filter(regionMapping::containsKey)
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    regionId -> regionMapping.get(regionId).getName(),
                    (v1, v2) -> v1));

    Map<String, Country> regionIdToCountryMap =
        regionIds.stream()
            .filter(regionMapping::containsKey)
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(), regionMapping::get, (v1, v2) -> v1));

    return regionRule.toBuilder()
        .putAllRegionIdToNameMap(regionIdToNameMap)
        .putAllRegionIdToCountryMap(regionIdToCountryMap)
        .build();
  }

  @Override
  public void bulkDeleteRegionRules(
      BulkDeleteRegionRulesRequest request,
      StreamObserver<BulkDeleteRegionRulesResponse> responseObserver) {
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error("Bulk Delete Region Rules Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }
      rulesManager.bulkDeleteRegionRules(RequestContext.CURRENT.get(), request.getIdsList());
      responseObserver.onNext(BulkDeleteRegionRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to bulk delete region rules with ids {} :", request.getIdsList(), e);
      responseObserver.onError(e);
    }
  }
}
