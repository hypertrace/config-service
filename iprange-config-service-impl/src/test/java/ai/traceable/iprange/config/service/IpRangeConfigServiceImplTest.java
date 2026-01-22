package ai.traceable.iprange.config.service;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import ai.traceable.iprange.config.service.rules.RulesManager;
import ai.traceable.iprange.config.service.rules.RulesValidator;
import ai.traceable.iprange.config.service.rules.migration.IpRangeRulesMigrationManager;
import ai.traceable.iprange.config.service.v1.*;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class IpRangeConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-ip-range-test";
  private RulesValidator rulesValidator;
  private RulesManager rulesManager;
  private IpRangeConfigServiceImpl ipRangeConfigService;
  private Supplier<List<IpRangeRule>> blockAllExceptRulesSupplier;

  @BeforeEach
  void setup() {
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    this.blockAllExceptRulesSupplier = Mockito.mock(Supplier.class);
    ipRangeConfigService =
        new IpRangeConfigServiceImpl(
            rulesValidator, rulesManager, mock(IpRangeRulesMigrationManager.class));
  }

  @Nested
  class GetIpRangeRules {
    @Test
    @DisplayName("should fetch all ip range rules and filter them using given filter")
    void shouldGetAllIpRangeRules() {
      IpRangeRule ipRangeRule1 = IpRangeRule.newBuilder().setId("Tester-1").build();
      IpRangeRule ipRangeRule2 = IpRangeRule.newBuilder().setId("Tester-2").build();
      IpRangeRuleRecord record1 = IpRangeRuleRecord.newBuilder().setRule(ipRangeRule1).build();
      IpRangeRuleRecord record2 = IpRangeRuleRecord.newBuilder().setRule(ipRangeRule2).build();

      StreamObserver<GetIpRangeRulesResponse> responseStreamObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              ipRangeConfigService.getIpRangeRules(
                  GetIpRangeRulesRequest.getDefaultInstance(), responseStreamObserver);

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
      verify(responseStreamObserver, times(1)).onNext(GetIpRangeRulesResponse.newBuilder().build());
      verify(responseStreamObserver, times(1)).onCompleted();

      reset(responseStreamObserver);
      when(rulesManager.getIpRangeRuleRecords(any(), any())).thenReturn(List.of(record1, record2));

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
      verify(responseStreamObserver, times(1))
          .onNext(
              GetIpRangeRulesResponse.newBuilder()
                  .addAllRules(List.of(ipRangeRule1, ipRangeRule2))
                  .addAllRuleRecords(List.of(record1, record2))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should throw a runtime exception from getIpRange if gets one")
    void propagateRuntimeException_inGetIpRange() {
      StreamObserver<GetIpRangeRulesResponse> responseStreamObserver = mock(StreamObserver.class);
      when(rulesManager.getIpRangeRuleRecords(any(), any())).thenThrow(RuntimeException.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.getIpRangeRules(
                  GetIpRangeRulesRequest.getDefaultInstance(), responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.UNKNOWN));
    }
  }

  @Nested
  class CreateIpRangeRule {
    @Test
    @DisplayName("should create a IP Range Rule if everything is fine")
    void shouldCreateIpRangeRule_onValidRequest() {
      RuleEffectWithModifications ruleEffectWithModifications =
          RuleEffectWithModifications.newBuilder()
              .setAgentRuleEffect(
                  AgentRuleEffect.newBuilder()
                      .addAgentModifications(
                          AgentModification.newBuilder()
                              .setHeaderInjection(
                                  HeaderInjection.newBuilder()
                                      .setHeaderLocation(
                                          PredicateLocation.PREDICATE_LOCATION_REQUEST)
                                      .setHeaderName("name")
                                      .setValue(FieldValue.newBuilder().setStaticValue("value")))))
              .build();
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .addEffects(ruleEffectWithModifications)
              .build();

      IpRangeRule ipRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(ipRangeRuleDetails)
              .addAllIpAddresses(Arrays.asList("1.2.3.4"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(ipRangeRuleDetails).build();

      when(rulesValidator.validate(eq(createIpRangeRuleRequest), any())).thenReturn(Status.OK);
      when(rulesManager.createIpRangeRule(any(), eq(createIpRangeRuleRequest)))
          .thenReturn(ipRangeRule);

      StreamObserver<CreateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              ipRangeConfigService.createIpRangeRule(
                  createIpRangeRuleRequest, responseStreamObserver);

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(CreateIpRangeRuleResponse.newBuilder().setRule(ipRangeRule).build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid arguments if validator rejects the arguments")
    void should_fail_CreateIpRangeRule_onInvalidRequest() {
      when(rulesValidator.validate(eq(CreateIpRangeRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.INVALID_ARGUMENT);

      StreamObserver<CreateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              ipRangeConfigService.createIpRangeRule(
                  CreateIpRangeRuleRequest.getDefaultInstance(), responseStreamObserver);

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return internal error unable to create")
    void should_fail_createIpRangeRule_unableToCreate() {
      when(rulesValidator.validate(eq(CreateIpRangeRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.createIpRangeRule(any(), eq(CreateIpRangeRuleRequest.getDefaultInstance())))
          .thenReturn(null);

      StreamObserver<CreateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              ipRangeConfigService.createIpRangeRule(
                  CreateIpRangeRuleRequest.getDefaultInstance(), responseStreamObserver);

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));
    }

    @Test
    @DisplayName("should throw a runtime exception from create Ip Range Rule if gets one")
    void propagateRuntimeException_inCreateIpRange() {
      StreamObserver<CreateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);
      when(rulesValidator.validate(eq(CreateIpRangeRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.createIpRangeRule(any(), eq(CreateIpRangeRuleRequest.getDefaultInstance())))
          .thenThrow(RuntimeException.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.createIpRangeRule(
                  CreateIpRangeRuleRequest.getDefaultInstance(), responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == RuntimeException.class));
    }

    @Test
    @DisplayName(
        "should throw a invalid arguments exception from create Ip Range Rule if manager throws it")
    void propagateIllegalArgumentException_inCreateIpRange() {
      StreamObserver<CreateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);
      when(rulesValidator.validate(eq(CreateIpRangeRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.createIpRangeRule(any(), eq(CreateIpRangeRuleRequest.getDefaultInstance())))
          .thenThrow(IllegalArgumentException.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.createIpRangeRule(
                  CreateIpRangeRuleRequest.getDefaultInstance(), responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == IllegalArgumentException.class));
    }
  }

  @Nested
  class UpdateIpRangeRule {
    @Test
    @DisplayName("should update a IP Range Rule if everything is fine")
    void shouldUpdateIpRangeRule_onValidRequest() {
      RuleEffectWithModifications ruleEffectWithModifications =
          RuleEffectWithModifications.newBuilder()
              .setAgentRuleEffect(
                  AgentRuleEffect.newBuilder()
                      .addAgentModifications(
                          AgentModification.newBuilder()
                              .setHeaderInjection(
                                  HeaderInjection.newBuilder()
                                      .setHeaderLocation(
                                          PredicateLocation.PREDICATE_LOCATION_REQUEST)
                                      .setHeaderName("name")
                                      .setValue(FieldValue.newBuilder().setStaticValue("value")))))
              .build();
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .addEffects(ruleEffectWithModifications)
              .build();

      IpRangeRule ipRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(ipRangeRuleDetails)
              .addAllIpAddresses(Arrays.asList("1.2.3.4"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("First-test")
              .setRuleDetails(ipRangeRuleDetails)
              .setDisabled(true)
              .build();

      when(rulesValidator.validate(eq(updateIpRangeRuleRequest), any())).thenReturn(Status.OK);
      when(rulesManager.updateIpRangeRule(any(), eq(updateIpRangeRuleRequest)))
          .thenReturn(ipRangeRule);

      StreamObserver<UpdateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              ipRangeConfigService.updateIpRangeRule(
                  updateIpRangeRuleRequest, responseStreamObserver);

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(UpdateIpRangeRuleResponse.newBuilder().setRule(ipRangeRule).build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid arguments if validator rejects the arguments")
    void should_fail_UpdateIpRangeRule_onInvalidRequest() {
      when(rulesValidator.validate(eq(UpdateIpRangeRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.INVALID_ARGUMENT);

      StreamObserver<UpdateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              ipRangeConfigService.updateIpRangeRule(
                  UpdateIpRangeRuleRequest.getDefaultInstance(), responseStreamObserver);

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return internal error unable to update")
    void should_fail_updateIpRangeRule_unableToUpdate() {
      when(rulesValidator.validate(eq(UpdateIpRangeRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.updateIpRangeRule(any(), eq(UpdateIpRangeRuleRequest.getDefaultInstance())))
          .thenReturn(null);

      StreamObserver<UpdateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              ipRangeConfigService.updateIpRangeRule(
                  UpdateIpRangeRuleRequest.getDefaultInstance(), responseStreamObserver);

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));
    }

    @Test
    @DisplayName("should throw a runtime exception from update Ip Range Rule if gets one")
    void propagateRuntimeException_inUpdateIpRange() {
      StreamObserver<UpdateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);
      when(rulesValidator.validate(eq(UpdateIpRangeRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.updateIpRangeRule(any(), eq(UpdateIpRangeRuleRequest.getDefaultInstance())))
          .thenThrow(RuntimeException.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.updateIpRangeRule(
                  UpdateIpRangeRuleRequest.getDefaultInstance(), responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == RuntimeException.class));
    }

    @Test
    @DisplayName(
        "should throw a invalid arguments exception from update Ip Range Rule if manager throws it")
    void propagateIllegalArgumentException_inCreateIpRange() {
      StreamObserver<UpdateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);
      when(rulesValidator.validate(eq(UpdateIpRangeRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.updateIpRangeRule(any(), eq(UpdateIpRangeRuleRequest.getDefaultInstance())))
          .thenThrow(IllegalArgumentException.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.updateIpRangeRule(
                  UpdateIpRangeRuleRequest.getDefaultInstance(), responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == IllegalArgumentException.class));
    }

    @Test
    @DisplayName(
        "should throw a noSuchElement exception from when Ip Range Rule with given id does not exist")
    void propagateNoSuchElementException_inCreateIpRange() {
      StreamObserver<UpdateIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);
      when(rulesValidator.validate(eq(UpdateIpRangeRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.updateIpRangeRule(any(), eq(UpdateIpRangeRuleRequest.getDefaultInstance())))
          .thenThrow(NoSuchElementException.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.updateIpRangeRule(
                  UpdateIpRangeRuleRequest.getDefaultInstance(), responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == NoSuchElementException.class));
    }
  }

  @Nested
  class DeleteIpRangeRule {
    @Test
    @DisplayName("should delete for a valid request")
    void shouldDeleteIpRangeRule() throws InvalidProtocolBufferException {
      DeleteIpRangeRuleRequest deleteIpRangeRuleRequest =
          DeleteIpRangeRuleRequest.newBuilder().setId("id").build();

      when(rulesValidator.validate(deleteIpRangeRuleRequest)).thenReturn(Status.OK);

      when(rulesManager.deleteIpRangeRule(any(), eq("id")))
          .thenReturn(Optional.of(IpRangeRule.newBuilder().setId("id").build()));

      StreamObserver<DeleteIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.deleteIpRangeRule(
                  deleteIpRangeRuleRequest, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onNext(DeleteIpRangeRuleResponse.getDefaultInstance());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid argument status invalid request")
    void should_fail_deleteIpRangeRule_invalidRequest() {
      DeleteIpRangeRuleRequest deleteIpRangeRuleRequest =
          DeleteIpRangeRuleRequest.getDefaultInstance();

      when(rulesValidator.validate(deleteIpRangeRuleRequest)).thenReturn(Status.INVALID_ARGUMENT);

      StreamObserver<DeleteIpRangeRuleResponse> requestStreamObserver = mock(StreamObserver.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.deleteIpRangeRule(
                  deleteIpRangeRuleRequest, requestStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
      verify(requestStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should throw a runtime error when it occurs inside manager")
    void propagateRuntimeException_inDeleteIpRange() throws InvalidProtocolBufferException {
      DeleteIpRangeRuleRequest deleteIpRangeRuleRequest =
          DeleteIpRangeRuleRequest.newBuilder().setId("id").build();

      when(rulesValidator.validate(deleteIpRangeRuleRequest)).thenReturn(Status.OK);
      doThrow(RuntimeException.class).when(rulesManager).deleteIpRangeRule(any(), eq("id"));

      StreamObserver<DeleteIpRangeRuleResponse> responseStreamObserver = mock(StreamObserver.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.deleteIpRangeRule(
                  deleteIpRangeRuleRequest, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == RuntimeException.class));
    }
  }

  @Nested
  class BulkDeleteIpRangeRules {
    @Test
    @DisplayName("should bulk delete for a valid request")
    void shouldBulkDeleteIpRangeRules() {
      BulkDeleteIpRangeRulesRequest bulkDeleteRequest =
          BulkDeleteIpRangeRulesRequest.newBuilder().addIds("id1").addIds("id2").build();

      when(rulesValidator.validate(bulkDeleteRequest)).thenReturn(Status.OK);

      StreamObserver<BulkDeleteIpRangeRulesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.bulkDeleteIpRangeRules(
                  bulkDeleteRequest, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(rulesManager, times(1)).bulkDeleteIpRangeRules(any(), eq(List.of("id1", "id2")));
      verify(responseStreamObserver, times(1))
          .onNext(BulkDeleteIpRangeRulesResponse.getDefaultInstance());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid argument status for invalid request")
    void should_fail_bulkDeleteIpRangeRules_invalidRequest() {
      BulkDeleteIpRangeRulesRequest bulkDeleteRequest =
          BulkDeleteIpRangeRulesRequest.getDefaultInstance();

      when(rulesValidator.validate(bulkDeleteRequest)).thenReturn(Status.INVALID_ARGUMENT);

      StreamObserver<BulkDeleteIpRangeRulesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.bulkDeleteIpRangeRules(
                  bulkDeleteRequest, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should throw a runtime error when it occurs inside manager")
    void propagateRuntimeException_inBulkDeleteIpRange() {
      BulkDeleteIpRangeRulesRequest bulkDeleteRequest =
          BulkDeleteIpRangeRulesRequest.newBuilder().addIds("id1").build();

      when(rulesValidator.validate(bulkDeleteRequest)).thenReturn(Status.OK);
      doThrow(RuntimeException.class)
          .when(rulesManager)
          .bulkDeleteIpRangeRules(any(), eq(List.of("id1")));

      StreamObserver<BulkDeleteIpRangeRulesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              ipRangeConfigService.bulkDeleteIpRangeRules(
                  bulkDeleteRequest, responseStreamObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == RuntimeException.class));
    }
  }
}
