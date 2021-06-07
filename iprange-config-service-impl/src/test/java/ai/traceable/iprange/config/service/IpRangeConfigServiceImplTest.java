package ai.traceable.iprange.config.service;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import ai.traceable.iprange.config.service.rules.RulesManager;
import ai.traceable.iprange.config.service.rules.RulesValidator;
import ai.traceable.iprange.config.service.v1.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class IpRangeConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-ip-range-test";
  private RulesValidator rulesValidator;
  private RulesManager rulesManager;
  private IpRangeConfigServiceImpl ipRangeConfigService;

  @BeforeEach
  void setup() {
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    ipRangeConfigService = new IpRangeConfigServiceImpl(rulesValidator, rulesManager);
  }

  @Nested
  class GetIpRangeRules {
    @Test
    @DisplayName("should fetch all ip range rules and filter them using given filter")
    void shouldGetAllIpRangeRules() {
      IpRangeRule ipRangeRule1 = IpRangeRule.newBuilder().setId("Tester-1").build();
      IpRangeRule ipRangeRule2 = IpRangeRule.newBuilder().setId("Tester-2").build();

      StreamObserver<GetIpRangeRulesResponse> responseStreamObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              ipRangeConfigService.getIpRangeRules(
                  GetIpRangeRulesRequest.getDefaultInstance(), responseStreamObserver);

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
      verify(responseStreamObserver, times(1)).onNext(GetIpRangeRulesResponse.newBuilder().build());
      verify(responseStreamObserver, times(1)).onCompleted();

      reset(responseStreamObserver);
      when(rulesManager.getIpRangeRules(any(), any()))
          .thenReturn(List.of(ipRangeRule1, ipRangeRule2));

      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
      verify(responseStreamObserver, times(1))
          .onNext(
              GetIpRangeRulesResponse.newBuilder()
                  .addAllRules(List.of(ipRangeRule1, ipRangeRule2))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should throw a runtime exception from getIpRange if gets one")
    void propagateRuntimeException_inGetIpRange() {
      StreamObserver<GetIpRangeRulesResponse> responseStreamObserver = mock(StreamObserver.class);
      when(rulesManager.getIpRangeRules(any(), any())).thenThrow(RuntimeException.class);
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
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
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

      when(rulesValidator.validate(createIpRangeRuleRequest)).thenReturn(Status.OK);
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
      when(rulesValidator.validate(CreateIpRangeRuleRequest.getDefaultInstance()))
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
      when(rulesValidator.validate(CreateIpRangeRuleRequest.getDefaultInstance()))
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
      when(rulesValidator.validate(CreateIpRangeRuleRequest.getDefaultInstance()))
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
      when(rulesValidator.validate(CreateIpRangeRuleRequest.getDefaultInstance()))
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
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
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

      when(rulesValidator.validate(updateIpRangeRuleRequest)).thenReturn(Status.OK);
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
      when(rulesValidator.validate(UpdateIpRangeRuleRequest.getDefaultInstance()))
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
      when(rulesValidator.validate(UpdateIpRangeRuleRequest.getDefaultInstance()))
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
      when(rulesValidator.validate(UpdateIpRangeRuleRequest.getDefaultInstance()))
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
      when(rulesValidator.validate(UpdateIpRangeRuleRequest.getDefaultInstance()))
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
      when(rulesValidator.validate(UpdateIpRangeRuleRequest.getDefaultInstance()))
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
    void shouldDeleteIpRangeRule() {
      DeleteIpRangeRuleRequest deleteIpRangeRuleRequest =
          DeleteIpRangeRuleRequest.newBuilder().setId("id").build();

      when(rulesValidator.validate(deleteIpRangeRuleRequest)).thenReturn(Status.OK);
      doNothing().when(rulesManager).deleteIpRangeRule(any(), eq("id"));

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
    void propagateRuntimeException_inDeleteIpRange() {
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
}
