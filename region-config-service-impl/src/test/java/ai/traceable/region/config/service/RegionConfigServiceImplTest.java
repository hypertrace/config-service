package ai.traceable.region.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.activity.event.SecurityConfigurationAction;
import ai.traceable.activity.event.SecurityConfigurationChange;
import ai.traceable.activity.event.SecurityConfigurationType;
import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.region.config.service.regions.IpqsRegionStore;
import ai.traceable.region.config.service.regions.NeustarRegionStore;
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
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RegionConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-id";

  private NeustarRegionStore neustarRegionStore;
  private IpqsRegionStore ipqsRegionStore;
  private RulesValidator rulesValidator;
  private RulesManager rulesManager;

  private RegionConfigServiceImpl regionConfigService;
  private ActivityEventProducer mockActivityEventProducer;
  private RequestContext requestContext;
  private FeatureCachingClient featureCachingClient;

  @BeforeEach
  void setup() {
    neustarRegionStore = mock(NeustarRegionStore.class);
    ipqsRegionStore = mock(IpqsRegionStore.class);
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    mockActivityEventProducer = mock(ActivityEventProducer.class);
    RegionConfigServiceConfig mockCustomSignatureConfigServiceConfig =
        mock(RegionConfigServiceConfig.class);
    when(mockCustomSignatureConfigServiceConfig.shouldPublishActivityEvents()).thenReturn(true);
    featureCachingClient = mock(FeatureCachingClient.class);

    regionConfigService =
        new RegionConfigServiceImpl(
            neustarRegionStore,
            ipqsRegionStore,
            rulesValidator,
            rulesManager,
            mockCustomSignatureConfigServiceConfig,
            mockActivityEventProducer,
            featureCachingClient);
    requestContext = RequestContext.forTenantId(TENANT_ID);
  }

  @Nested
  class GetRegions {
    @Test
    void shouldGetCountries() {
      StreamObserver<GetRegionsResponse> responseObserver = mock(StreamObserver.class);
      List<Region> regions = List.of(Region.newBuilder().setId("id").setName("name").build());
      when(neustarRegionStore.getCountries(Collections.emptyList())).thenReturn(regions);

      Runnable runnable =
          () ->
              regionConfigService.getRegions(
                  GetRegionsRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(GetRegionsResponse.newBuilder().addAllRegion(regions).build());
      verify(responseObserver, times(1)).onCompleted();

      regions = List.of(Region.newBuilder().setId("id2").setName("name2").build());
      when(ipqsRegionStore.getCountries(Collections.emptyList())).thenReturn(regions);
      when(featureCachingClient.isIpqsEnabledForRegionToIpMapping(requestContext)).thenReturn(true);
      runnable =
          () ->
              regionConfigService.getRegions(
                  GetRegionsRequest.getDefaultInstance(), responseObserver);
      requestContext.run(runnable);

      verify(responseObserver, times(1))
          .onNext(GetRegionsResponse.newBuilder().addAllRegion(regions).build());
    }
  }

  @Nested
  class GetDetailedRegions {
    @Test
    void shouldGetDetailedRegions() {
      StreamObserver<GetDetailedRegionsResponse> responseObserver = mock(StreamObserver.class);
      List<DetailedRegion> regions =
          List.of(DetailedRegion.newBuilder().setId("id").setName("name").build());
      when(neustarRegionStore.getDetailedRegions(Collections.emptyList())).thenReturn(regions);

      Runnable runnable =
          () ->
              regionConfigService.getDetailedRegions(
                  GetDetailedRegionsRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(GetDetailedRegionsResponse.newBuilder().addAllRegion(regions).build());
      verify(responseObserver, times(1)).onCompleted();

      regions = List.of(DetailedRegion.newBuilder().setId("id2").setName("name2").build());
      when(ipqsRegionStore.getDetailedRegions(Collections.emptyList())).thenReturn(regions);
      when(featureCachingClient.isIpqsEnabledForRegionToIpMapping(requestContext)).thenReturn(true);

      runnable =
          () ->
              regionConfigService.getDetailedRegions(
                  GetDetailedRegionsRequest.getDefaultInstance(), responseObserver);
      requestContext.run(runnable);

      verify(responseObserver, times(1))
          .onNext(GetDetailedRegionsResponse.newBuilder().addAllRegion(regions).build());
    }
  }

  @Nested
  class GetRegion {
    @Test
    void shouldGetRegion() {
      StreamObserver<GetRegionResponse> responseObserver = mock(StreamObserver.class);
      Region region = Region.newBuilder().setId("id").setName("name").build();
      when(neustarRegionStore.getRegion("id")).thenReturn(Optional.of(region));

      Runnable runnable =
          () ->
              regionConfigService.getRegion(
                  GetRegionRequest.newBuilder().setId("id").build(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(GetRegionResponse.newBuilder().setRegion(region).build());
      verify(responseObserver, times(1)).onCompleted();

      region = Region.newBuilder().setId("id2").setName("name2").build();
      when(ipqsRegionStore.getRegion("id2")).thenReturn(Optional.of(region));
      when(featureCachingClient.isIpqsEnabledForRegionToIpMapping(requestContext)).thenReturn(true);

      runnable =
          () ->
              regionConfigService.getRegion(
                  GetRegionRequest.newBuilder().setId("id2").build(), responseObserver);
      requestContext.run(runnable);

      verify(responseObserver, times(1))
          .onNext(GetRegionResponse.newBuilder().setRegion(region).build());
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
      when(neustarRegionStore.getRegion("id")).thenReturn(Optional.empty());

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
      RegionRule regionRule1 =
          RegionRule.newBuilder().setId("id-1").addRegionId("region-id-1").build();
      RegionRule regionRule2 =
          RegionRule.newBuilder().setId("id-2").addRegionId("region-id-2").build();
      when(rulesManager.getRegionRules(eq(requestContext), any()))
          .thenReturn(List.of(regionRule1, regionRule2));
      when(neustarRegionStore.getCountries(any()))
          .thenReturn(
              List.of(
                  Region.newBuilder().setId("region-id-1").setName("region-1").build(),
                  Region.newBuilder().setId("region-id-2").setName("region-2").build()));

      StreamObserver<GetAllRegionRulesResponse> responseObserver = mock(StreamObserver.class);

      requestContext.call(
          () -> {
            regionConfigService.getAllRegionRules(
                GetAllRegionRulesRequest.getDefaultInstance(), responseObserver);
            return null;
          });

      regionRule1 = regionRule1.toBuilder().putRegionIdToNameMap("region-id-1", "region-1").build();

      regionRule2 = regionRule2.toBuilder().putRegionIdToNameMap("region-id-2", "region-2").build();

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

      when(rulesValidator.validate(eq(createRegionRuleRequest), any())).thenReturn(Status.OK);
      when(rulesManager.createRegionRule(requestContext, createRegionRuleRequest))
          .thenReturn(Optional.of(regionRule));

      StreamObserver<CreateRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      requestContext.call(
          () -> {
            regionConfigService.createRegionRule(createRegionRuleRequest, responseObserver);
            return null;
          });

      verify(responseObserver, times(1))
          .onNext(CreateRegionRuleResponse.newBuilder().setRule(regionRule).build());
      verify(responseObserver, times(1)).onCompleted();

      verify(mockActivityEventProducer, times(1))
          .publishSecurityConfigurationChangeEvent(
              any(RequestContext.class),
              eq(
                  SecurityConfigurationChange.newBuilder()
                      .setRuleId("id-1")
                      .setRuleName("name")
                      .setSecurityConfigurationType(SecurityConfigurationType.LOCATION_RULE)
                      .setSecurityConfigurationAction(SecurityConfigurationAction.ADD)
                      .build()));
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

      when(rulesValidator.validate(eq(createRegionRuleRequest), any()))
          .thenReturn(Status.INVALID_ARGUMENT);
      when(rulesManager.createRegionRule(requestContext, createRegionRuleRequest))
          .thenReturn(Optional.empty());

      StreamObserver<CreateRegionRuleResponse> responseObserver = mock(StreamObserver.class);
      requestContext.call(
          () -> {
            regionConfigService.createRegionRule(createRegionRuleRequest, responseObserver);
            return Optional.empty();
          });
      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INVALID_ARGUMENT));
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
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      when(rulesValidator.validate(eq(updateRegionRuleRequest), any())).thenReturn(Status.OK);
      when(rulesManager.updateRegionRule(requestContext, updateRegionRuleRequest))
          .thenReturn(Optional.of(updatedRegionRule));

      StreamObserver<UpdateRegionRuleResponse> responseObserver = mock(StreamObserver.class);

      requestContext.call(
          () -> {
            regionConfigService.updateRegionRule(updateRegionRuleRequest, responseObserver);
            return null;
          });
      verify(responseObserver, times(1))
          .onNext(UpdateRegionRuleResponse.newBuilder().setRule(updatedRegionRule).build());
      verify(responseObserver, times(1)).onCompleted();

      verify(mockActivityEventProducer, times(1))
          .publishSecurityConfigurationChangeEvent(
              any(RequestContext.class),
              eq(
                  SecurityConfigurationChange.newBuilder()
                      .setRuleId("id")
                      .setRuleName("name")
                      .setSecurityConfigurationType(SecurityConfigurationType.LOCATION_RULE)
                      .setSecurityConfigurationAction(SecurityConfigurationAction.UPDATE)
                      .build()));
    }

    @Test
    @DisplayName("should return invalid argument status invalid request")
    void should_fail_updateRegionRule_invalidRequest() {
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      when(rulesValidator.validate(eq(updateRegionRuleRequest), any()))
          .thenReturn(Status.INVALID_ARGUMENT);
      when(rulesManager.updateRegionRule(requestContext, updateRegionRuleRequest))
          .thenReturn(Optional.empty());

      StreamObserver<UpdateRegionRuleResponse> responseObserver = mock(StreamObserver.class);

      requestContext.call(
          () -> {
            regionConfigService.updateRegionRule(updateRegionRuleRequest, responseObserver);
            return Optional.empty();
          });
      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return internal status error on invalid rule id")
    void should_fail_updateRegionRule_invalidId() {
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      when(rulesValidator.validate(eq(updateRegionRuleRequest), any())).thenReturn(Status.OK);
      when(rulesManager.updateRegionRule(requestContext, updateRegionRuleRequest))
          .thenReturn(Optional.empty());

      StreamObserver<UpdateRegionRuleResponse> responseObserver = mock(StreamObserver.class);

      requestContext.call(
          () -> {
            regionConfigService.updateRegionRule(updateRegionRuleRequest, responseObserver);
            return Optional.empty();
          });
      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.INTERNAL));
    }
  }

  @Nested
  class DeleteRegionRule {
    @Test
    void shouldDeleteRegionRule() throws InvalidProtocolBufferException {
      DeleteRegionRuleRequest deleteRegionRuleRequest =
          DeleteRegionRuleRequest.newBuilder().setId("id").build();

      RegionRule regionRule = RegionRule.newBuilder().setId("id").build();
      when(rulesValidator.validate(deleteRegionRuleRequest)).thenReturn(Status.OK);
      when(rulesManager.deleteRegionRule(any(), eq("id"))).thenReturn(Optional.of(regionRule));

      StreamObserver<DeleteRegionRuleResponse> responseObserver = mock(StreamObserver.class);

      requestContext.call(
          () -> {
            regionConfigService.deleteRegionRule(deleteRegionRuleRequest, responseObserver);
            return null;
          });
      verify(responseObserver, times(1)).onNext(DeleteRegionRuleResponse.getDefaultInstance());
      verify(responseObserver, times(1)).onCompleted();

      verify(mockActivityEventProducer, times(1))
          .publishSecurityConfigurationChangeEvent(
              any(RequestContext.class),
              eq(
                  SecurityConfigurationChange.newBuilder()
                      .setRuleId("id")
                      .setRuleName(regionRule.getName())
                      .setSecurityConfigurationType(SecurityConfigurationType.LOCATION_RULE)
                      .setSecurityConfigurationAction(SecurityConfigurationAction.REMOVE)
                      .build()));
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
  }
}
