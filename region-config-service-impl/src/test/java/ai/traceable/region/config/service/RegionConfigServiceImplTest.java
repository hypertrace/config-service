package ai.traceable.region.config.service;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import ai.traceable.region.config.service.v1.UpdateRegionRuleResponse;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RegionConfigServiceImplTest {
  private static final String TENANT_ID = "tenant1";

  private RegionStore regionStore;
  private RulesValidator rulesValidator;
  private RulesManager rulesManager;

  private RegionConfigServiceImpl regionConfigService;

  @BeforeEach
  void setup() {
    regionStore = mock(RegionStore.class);
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    regionConfigService = new RegionConfigServiceImpl(regionStore, rulesValidator, rulesManager);
  }

  @Nested
  class GetRegions {
    @Test
    void shouldGetCountries() {
      StreamObserver<GetRegionsResponse> responseObserver = mock(StreamObserver.class);
      List<Region> regions = List.of(Region.newBuilder().setId("id").setName("name").build());
      when(regionStore.getCountries(Collections.emptyList())).thenReturn(regions);

      Runnable runnable =
          () ->
              regionConfigService.getRegions(
                  GetRegionsRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(GetRegionsResponse.newBuilder().addAllRegion(regions).build());
      verify(responseObserver, times(1)).onCompleted();
    }
  }

  @Nested
  class GetDetailedRegions {
    @Test
    void shouldGetDetailedRegions() {
      StreamObserver<GetDetailedRegionsResponse> responseObserver = mock(StreamObserver.class);
      List<DetailedRegion> regions =
          List.of(DetailedRegion.newBuilder().setId("id").setName("name").build());
      when(regionStore.getDetailedRegions(Collections.emptyList())).thenReturn(regions);

      Runnable runnable =
          () ->
              regionConfigService.getDetailedRegions(
                  GetDetailedRegionsRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(GetDetailedRegionsResponse.newBuilder().addAllRegion(regions).build());
      verify(responseObserver, times(1)).onCompleted();
    }
  }

  @Nested
  class GetRegion {
    @Test
    void shouldGetRegion() {
      StreamObserver<GetRegionResponse> responseObserver = mock(StreamObserver.class);
      Region region = Region.newBuilder().setId("id").setName("name").build();
      when(regionStore.getRegion("id")).thenReturn(Optional.of(region));

      Runnable runnable =
          () ->
              regionConfigService.getRegion(
                  GetRegionRequest.newBuilder().setId("id").build(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(GetRegionResponse.newBuilder().setRegion(region).build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return not found for missing region id in request")
    void should_error_noRegionId() {
      StreamObserver<GetRegionResponse> responseObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              regionConfigService.getRegion(
                  GetRegionRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return not found for invalid region id")
    void should_error_invalidRegionId() {
      StreamObserver<GetRegionResponse> responseObserver = mock(StreamObserver.class);
      when(regionStore.getRegion("id")).thenReturn(Optional.empty());

      Runnable runnable =
          () ->
              regionConfigService.getRegion(
                  GetRegionRequest.newBuilder().setId("id").build(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.NOT_FOUND));
    }
  }

  @Nested
  class GetAllRegionRules {
    @Test
    void shouldGetAllRegionRules() {
      RegionRule regionRule1 = RegionRule.newBuilder().setId("id-1").build();
      RegionRule regionRule2 = RegionRule.newBuilder().setId("id-2").build();
      when(rulesManager.getRegionRules()).thenReturn(List.of(regionRule1, regionRule2));

      StreamObserver<GetAllRegionRulesResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () ->
              regionConfigService.getAllRegionRules(
                  GetAllRegionRulesRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetAllRegionRulesResponse.newBuilder()
                  .addAllRule(List.of(regionRule1, regionRule2))
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }
  }

  @Nested
  class CreateRegionRule {
    @Test
    void shouldCreateRegionRule() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      RegionRule regionRule =
          RegionRule.newBuilder()
              .setId("id-1")
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      when(rulesValidator.validate(createRegionRuleRequest)).thenReturn(Status.OK);
      when(rulesManager.createRegionRule(createRegionRuleRequest))
          .thenReturn(Optional.of(regionRule));

      StreamObserver<CreateRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () -> regionConfigService.createRegionRule(createRegionRuleRequest, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(CreateRegionRuleResponse.newBuilder().setRule(regionRule).build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid argument status invalid request")
    void should_fail_createRegionRule_invalidRequest() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      when(rulesValidator.validate(createRegionRuleRequest)).thenReturn(Status.INVALID_ARGUMENT);
      when(rulesManager.createRegionRule(createRegionRuleRequest)).thenReturn(Optional.empty());

      StreamObserver<CreateRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () -> regionConfigService.createRegionRule(createRegionRuleRequest, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return internal error unable to create")
    void should_fail_createRegionRule_unableToCreate() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      when(rulesValidator.validate(createRegionRuleRequest)).thenReturn(Status.OK);
      when(rulesManager.createRegionRule(createRegionRuleRequest)).thenReturn(Optional.empty());

      StreamObserver<CreateRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () -> regionConfigService.createRegionRule(createRegionRuleRequest, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INTERNAL));
    }
  }

  @Nested
  class UpdateRegionRule {
    @Test
    void shouldUpdateRegionRule() {
      RegionRule updatedRegionRule =
          RegionRule.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder().setRule(updatedRegionRule).build();

      when(rulesValidator.validate(updateRegionRuleRequest)).thenReturn(Status.OK);
      when(rulesManager.updateRegionRule(updatedRegionRule))
          .thenReturn(Optional.of(updatedRegionRule));

      StreamObserver<UpdateRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () -> regionConfigService.updateRegionRule(updateRegionRuleRequest, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(UpdateRegionRuleResponse.newBuilder().setRule(updatedRegionRule).build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid argument status invalid request")
    void should_fail_updateRegionRule_invalidRequest() {
      RegionRule updatedRegionRule =
          RegionRule.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder().setRule(updatedRegionRule).build();

      when(rulesValidator.validate(updateRegionRuleRequest)).thenReturn(Status.INVALID_ARGUMENT);
      when(rulesManager.updateRegionRule(updatedRegionRule)).thenReturn(Optional.empty());

      StreamObserver<UpdateRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () -> regionConfigService.updateRegionRule(updateRegionRuleRequest, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return internal error unable to update")
    void should_fail_updateRegionRule_unableToUpdate() {
      RegionRule updatedRegionRule =
          RegionRule.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder().setRule(updatedRegionRule).build();

      when(rulesValidator.validate(updateRegionRuleRequest)).thenReturn(Status.OK);
      when(rulesManager.updateRegionRule(updatedRegionRule)).thenReturn(Optional.empty());

      StreamObserver<UpdateRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () -> regionConfigService.updateRegionRule(updateRegionRuleRequest, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INTERNAL));
    }
  }

  @Nested
  class DeleteRegionRule {
    @Test
    void shouldDeleteRegionRule() {
      DeleteRegionRuleRequest deleteRegionRuleRequest =
          DeleteRegionRuleRequest.newBuilder().setId("id").build();

      when(rulesValidator.validate(deleteRegionRuleRequest)).thenReturn(Status.OK);
      when(rulesManager.deleteRegionRule("id")).thenReturn(true);

      StreamObserver<DeleteRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () -> regionConfigService.deleteRegionRule(deleteRegionRuleRequest, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1)).onNext(DeleteRegionRuleResponse.getDefaultInstance());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid argument status invalid request")
    void should_fail_deleteRegionRule_invalidRequest() {
      DeleteRegionRuleRequest deleteRegionRuleRequest =
          DeleteRegionRuleRequest.getDefaultInstance();

      when(rulesValidator.validate(deleteRegionRuleRequest)).thenReturn(Status.INVALID_ARGUMENT);

      StreamObserver<DeleteRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () -> regionConfigService.deleteRegionRule(deleteRegionRuleRequest, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return internal error unable to update")
    void should_fail_deleteRegionRule_unableToDelete() {
      DeleteRegionRuleRequest deleteRegionRuleRequest =
          DeleteRegionRuleRequest.newBuilder().setId("id").build();

      when(rulesValidator.validate(deleteRegionRuleRequest)).thenReturn(Status.OK);
      when(rulesManager.deleteRegionRule("id")).thenReturn(false);

      StreamObserver<DeleteRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      Runnable runnable =
          () -> regionConfigService.deleteRegionRule(deleteRegionRuleRequest, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INTERNAL));
    }
  }
}
