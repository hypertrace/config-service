package ai.traceable.region.config.service;

import ai.traceable.region.config.service.regions.RegionStore;
import ai.traceable.region.config.service.rules.RulesManager;
import ai.traceable.region.config.service.rules.RulesValidator;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.CreateRegionRuleResponse;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleResponse;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
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
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RegionConfigServiceImpl extends RegionConfigServiceImplBase {
  private final RegionStore regionStore;
  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;

  @Inject
  RegionConfigServiceImpl(
      RegionStore regionStore, RulesValidator rulesValidator, RulesManager rulesManager) {
    this.regionStore = regionStore;
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
  }

  @Override
  public void getRegions(
      GetRegionsRequest request, StreamObserver<GetRegionsResponse> responseObserver) {

    List<Region> countries = regionStore.getCountries();

    responseObserver.onNext(GetRegionsResponse.newBuilder().addAllRegion(countries).build());
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
  }

  @Override
  public void deleteRegionRule(
      DeleteRegionRuleRequest request, StreamObserver<DeleteRegionRuleResponse> responseObserver) {
    Status status = rulesValidator.validate(request);
    if (!status.isOk()) {
      responseObserver.onError(status.asException());
      return;
    }

    String ruleId = request.getId();
    boolean isDeleted = rulesManager.deleteRegionRule(ruleId);
    if (isDeleted) {
      responseObserver.onNext(DeleteRegionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(String.format("unable to delete region rule %s", ruleId))
              .asRuntimeException());
    }
  }
}
