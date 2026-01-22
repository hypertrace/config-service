package ai.traceable.malicioussources.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.malicioussources.config.service.rules.RulesManager;
import ai.traceable.malicioussources.config.service.rules.RulesValidator;
import ai.traceable.malicioussources.config.service.rules.migration.MaliciousSourcesMigrationManager;
import ai.traceable.malicioussources.config.service.v1.BulkDeleteMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.BulkDeleteMaliciousSourcesRulesResponse;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleResponse;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleResponse;
import ai.traceable.malicioussources.config.service.v1.EventSeverity;
import ai.traceable.malicioussources.config.service.v1.ExpirationDetails;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesResponse;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleRecord;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleResponse;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MaliciousSourcesConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-malicious-sources-test";
  @Mock private RulesValidator rulesValidator;
  @Mock private RulesManager rulesManager;
  @Mock private MaliciousSourcesMigrationManager migrationManager;
  @InjectMocks private MaliciousSourcesConfigServiceImpl maliciousSourcesConfigService;

  @Nested
  class GetMaliciousSourcesRules {
    @Test
    @DisplayName("should fetch all malicious sources rules and filter them using given filter")
    void shouldGetAllMaliciousSourcesRules() {
      MaliciousSourcesRule maliciousSourcesRule1 =
          MaliciousSourcesRule.newBuilder().setId("Tester-1").build();
      MaliciousSourcesRule maliciousSourcesRule2 =
          MaliciousSourcesRule.newBuilder().setId("Tester-2").build();
      MaliciousSourcesRuleRecord record1 =
          MaliciousSourcesRuleRecord.newBuilder().setRule(maliciousSourcesRule1).build();
      MaliciousSourcesRuleRecord record2 =
          MaliciousSourcesRuleRecord.newBuilder().setRule(maliciousSourcesRule2).build();

      StreamObserver<GetMaliciousSourcesRulesResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              maliciousSourcesConfigService.getMaliciousSourcesRules(
                  GetMaliciousSourcesRulesRequest.getDefaultInstance(), responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onNext(GetMaliciousSourcesRulesResponse.newBuilder().build());
      verify(responseStreamObserver, times(1)).onCompleted();

      reset(responseStreamObserver);
      when(rulesManager.getMaliciousSourcesRuleRecords(any(), any()))
          .thenReturn(List.of(record1, record2));
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              GetMaliciousSourcesRulesResponse.newBuilder()
                  .addAllRules(List.of(maliciousSourcesRule1, maliciousSourcesRule2))
                  .addAllRuleRecords(List.of(record1, record2))
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should throw a runtime exception from getMaliciousSources if gets one")
    void propagateRuntimeException_inGetMaliciousSourcesRange() {
      StreamObserver<GetMaliciousSourcesRulesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      when(rulesManager.getMaliciousSourcesRuleRecords(any(), any()))
          .thenThrow(RuntimeException.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.getMaliciousSourcesRules(
                  GetMaliciousSourcesRulesRequest.getDefaultInstance(), responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);
      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.UNKNOWN));
    }
  }

  @Nested
  class CreateMaliciousSourcesRule {
    @Test
    @DisplayName("should create a MaliciousSources Rule if everything is fine")
    void shouldCreateMaliciousSourcesRule_onValidRequest() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALERT)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  com.google.protobuf.Duration.newBuilder().setSeconds(10).build())
                              .setExpirationTimestamp(Timestamp.newBuilder().setSeconds(20).build())
                              .build())
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .build();
      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      when(rulesValidator.validate(eq(createMaliciousSourcesRuleRequest), any()))
          .thenReturn(Status.OK);
      when(rulesManager.createMaliciousSourcesRule(any(), eq(createMaliciousSourcesRuleRequest)))
          .thenReturn(maliciousSourcesRule);

      StreamObserver<CreateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              maliciousSourcesConfigService.createMaliciousSourcesRule(
                  createMaliciousSourcesRuleRequest, responseStreamObserver);

      RequestContext.forTenantId(TENANT_ID).run(runnable);
      verify(responseStreamObserver, times(1))
          .onNext(
              CreateMaliciousSourcesRuleResponse.newBuilder()
                  .setRule(maliciousSourcesRule)
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid arguments if validator rejects the arguments")
    void should_fail_CreateMaliciousSourcesRule_onInvalidRequest() {
      when(rulesValidator.validate(
              eq(CreateMaliciousSourcesRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.INVALID_ARGUMENT);

      StreamObserver<CreateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              maliciousSourcesConfigService.createMaliciousSourcesRule(
                  CreateMaliciousSourcesRuleRequest.getDefaultInstance(), responseStreamObserver);

      RequestContext.forTenantId(TENANT_ID).run(runnable);
      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return internal error unable to create")
    void should_fail_createMaliciousSourcesRangeRule_unableToCreate() {
      when(rulesValidator.validate(
              eq(CreateMaliciousSourcesRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.INTERNAL);

      StreamObserver<CreateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              maliciousSourcesConfigService.createMaliciousSourcesRule(
                  CreateMaliciousSourcesRuleRequest.getDefaultInstance(), responseStreamObserver);

      RequestContext.forTenantId(TENANT_ID).run(runnable);
      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));
    }

    @Test
    @DisplayName("should throw a runtime exception from create Malicious Sources Rule if gets one")
    void propagateRuntimeException_inCreateMaliciousSources() {
      StreamObserver<CreateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);
      when(rulesValidator.validate(
              eq(CreateMaliciousSourcesRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.createMaliciousSourcesRule(
              any(), eq(CreateMaliciousSourcesRuleRequest.getDefaultInstance())))
          .thenThrow(RuntimeException.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.createMaliciousSourcesRule(
                  CreateMaliciousSourcesRuleRequest.getDefaultInstance(), responseStreamObserver);

      RequestContext.forTenantId(TENANT_ID).run(runnable);
      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == RuntimeException.class));
    }

    @Test
    @DisplayName(
        "should throw a invalid arguments exception from create Malicious Sources Rule if manager throws it")
    void propagateIllegalArgumentException_inCreateMaliciousSourcesRange() {
      StreamObserver<CreateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);
      when(rulesValidator.validate(
              eq(CreateMaliciousSourcesRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.createMaliciousSourcesRule(
              any(), eq(CreateMaliciousSourcesRuleRequest.getDefaultInstance())))
          .thenThrow(IllegalArgumentException.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.createMaliciousSourcesRule(
                  CreateMaliciousSourcesRuleRequest.getDefaultInstance(), responseStreamObserver);

      RequestContext.forTenantId(TENANT_ID).run(runnable);
      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == IllegalArgumentException.class));
    }
  }

  @Nested
  class UpdateMaliciousSourcesRule {
    @Test
    @DisplayName("should update a Malicious Sources if everything is fine")
    void shouldUpdateMaliciousSourcesRule_onValidRequest() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALERT)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  com.google.protobuf.Duration.newBuilder().setSeconds(10).build())
                              .setExpirationTimestamp(Timestamp.newBuilder().setSeconds(20).build())
                              .build())
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .build();
      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();

      when(rulesValidator.validate(eq(updateMaliciousSourcesRuleRequest), any()))
          .thenReturn(Status.OK);
      when(rulesManager.updateMaliciousSourcesRule(any(), eq(updateMaliciousSourcesRuleRequest)))
          .thenReturn(maliciousSourcesRule);

      StreamObserver<UpdateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              maliciousSourcesConfigService.updateMaliciousSourcesRule(
                  updateMaliciousSourcesRuleRequest, responseStreamObserver);

      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onNext(
              UpdateMaliciousSourcesRuleResponse.newBuilder()
                  .setRule(maliciousSourcesRule)
                  .build());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid arguments if validator rejects the arguments")
    void should_fail_UpdateMaliciousSourcesRule_onInvalidRequest() {
      when(rulesValidator.validate(
              eq(UpdateMaliciousSourcesRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.INVALID_ARGUMENT);

      StreamObserver<UpdateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              maliciousSourcesConfigService.updateMaliciousSourcesRule(
                  UpdateMaliciousSourcesRuleRequest.getDefaultInstance(), responseStreamObserver);

      RequestContext.forTenantId(TENANT_ID).run(runnable);
      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should return internal error unable to update")
    void should_fail_updateMaliciousSourcesRule_unableToUpdate() {
      when(rulesValidator.validate(
              eq(UpdateMaliciousSourcesRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.INTERNAL);

      StreamObserver<UpdateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              maliciousSourcesConfigService.updateMaliciousSourcesRule(
                  UpdateMaliciousSourcesRuleRequest.getDefaultInstance(), responseStreamObserver);

      RequestContext.forTenantId(TENANT_ID).run(runnable);
      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));
    }

    @Test
    @DisplayName("should throw a runtime exception from update Malicious Sources Rule if gets one")
    void propagateRuntimeException_inUpdateMaliciousSources() {
      StreamObserver<UpdateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);
      when(rulesValidator.validate(
              eq(UpdateMaliciousSourcesRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.updateMaliciousSourcesRule(
              any(), eq(UpdateMaliciousSourcesRuleRequest.getDefaultInstance())))
          .thenThrow(RuntimeException.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.updateMaliciousSourcesRule(
                  UpdateMaliciousSourcesRuleRequest.getDefaultInstance(), responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == RuntimeException.class));
    }

    @Test
    @DisplayName(
        "should throw a invalid arguments exception from update Malicious Sources Rule if manager throws it")
    void propagateIllegalArgumentException_inCreateMaliciousSources() {
      StreamObserver<UpdateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);
      when(rulesValidator.validate(
              eq(UpdateMaliciousSourcesRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.updateMaliciousSourcesRule(
              any(), eq(UpdateMaliciousSourcesRuleRequest.getDefaultInstance())))
          .thenThrow(IllegalArgumentException.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.updateMaliciousSourcesRule(
                  UpdateMaliciousSourcesRuleRequest.getDefaultInstance(), responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == IllegalArgumentException.class));
    }

    @Test
    @DisplayName(
        "should throw a noSuchElement exception from when Malicious Sources Rule with given id does not exist")
    void propagateNoSuchElementException_inCreateMaliciousSources() {
      StreamObserver<UpdateMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);
      when(rulesValidator.validate(
              eq(UpdateMaliciousSourcesRuleRequest.getDefaultInstance()), any()))
          .thenReturn(Status.OK);
      when(rulesManager.updateMaliciousSourcesRule(
              any(), eq(UpdateMaliciousSourcesRuleRequest.getDefaultInstance())))
          .thenThrow(NoSuchElementException.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.updateMaliciousSourcesRule(
                  UpdateMaliciousSourcesRuleRequest.getDefaultInstance(), responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == NoSuchElementException.class));
    }
  }

  @Nested
  class DeleteMaliciousSourcesRule {
    @Test
    @DisplayName("should delete for a valid request")
    void shouldDeleteMaliciousSourcesRule() {
      DeleteMaliciousSourcesRuleRequest deleteMaliciousSourcesRuleRequest =
          DeleteMaliciousSourcesRuleRequest.newBuilder().setId("id").build();

      when(rulesValidator.validate(deleteMaliciousSourcesRuleRequest)).thenReturn(Status.OK);

      when(rulesManager.deleteMaliciousSourcesRule(any(), eq("id")))
          .thenReturn(Optional.of(MaliciousSourcesRule.newBuilder().setId("id").build()));

      StreamObserver<DeleteMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.deleteMaliciousSourcesRule(
                  deleteMaliciousSourcesRuleRequest, responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onNext(DeleteMaliciousSourcesRuleResponse.getDefaultInstance());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid argument status invalid request")
    void should_fail_deleteMaliciousSourcesRule_invalidRequest() {
      DeleteMaliciousSourcesRuleRequest deleteMaliciousSourcesRuleRequest =
          DeleteMaliciousSourcesRuleRequest.getDefaultInstance();

      when(rulesValidator.validate(deleteMaliciousSourcesRuleRequest))
          .thenReturn(Status.INVALID_ARGUMENT);

      StreamObserver<DeleteMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.deleteMaliciousSourcesRule(
                  deleteMaliciousSourcesRuleRequest, responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should throw a runtime error when it occurs inside manager")
    void propagateRuntimeException_inDeleteMaliciousSources() {
      DeleteMaliciousSourcesRuleRequest deleteMaliciousSourcesRuleRequest =
          DeleteMaliciousSourcesRuleRequest.newBuilder().setId("id").build();

      when(rulesValidator.validate(deleteMaliciousSourcesRuleRequest)).thenReturn(Status.OK);
      doThrow(RuntimeException.class)
          .when(rulesManager)
          .deleteMaliciousSourcesRule(any(), eq("id"));

      StreamObserver<DeleteMaliciousSourcesRuleResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.deleteMaliciousSourcesRule(
                  deleteMaliciousSourcesRuleRequest, responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == RuntimeException.class));
    }
  }

  @Nested
  class BulkDeleteMaliciousSourcesRules {
    @Test
    @DisplayName("should bulk delete for a valid request")
    void shouldBulkDeleteMaliciousSourcesRules() {
      BulkDeleteMaliciousSourcesRulesRequest bulkDeleteRequest =
          BulkDeleteMaliciousSourcesRulesRequest.newBuilder().addIds("id1").addIds("id2").build();

      when(rulesValidator.validate(bulkDeleteRequest)).thenReturn(Status.OK);

      StreamObserver<BulkDeleteMaliciousSourcesRulesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.bulkDeleteMaliciousSourcesRules(
                  bulkDeleteRequest, responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(rulesManager, times(1))
          .bulkDeleteMaliciousSourcesRules(any(), eq(List.of("id1", "id2")));
      verify(responseStreamObserver, times(1))
          .onNext(BulkDeleteMaliciousSourcesRulesResponse.getDefaultInstance());
      verify(responseStreamObserver, times(1)).onCompleted();
    }

    @Test
    @DisplayName("should return invalid argument status for invalid request")
    void should_fail_bulkDeleteMaliciousSourcesRules_invalidRequest() {
      BulkDeleteMaliciousSourcesRulesRequest bulkDeleteRequest =
          BulkDeleteMaliciousSourcesRulesRequest.getDefaultInstance();

      when(rulesValidator.validate(bulkDeleteRequest)).thenReturn(Status.INVALID_ARGUMENT);

      StreamObserver<BulkDeleteMaliciousSourcesRulesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.bulkDeleteMaliciousSourcesRules(
                  bulkDeleteRequest, responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onError(
              argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
    }

    @Test
    @DisplayName("should throw a runtime error when it occurs inside manager")
    void propagateRuntimeException_inBulkDeleteMaliciousSources() {
      BulkDeleteMaliciousSourcesRulesRequest bulkDeleteRequest =
          BulkDeleteMaliciousSourcesRulesRequest.newBuilder().addIds("id1").build();

      when(rulesValidator.validate(bulkDeleteRequest)).thenReturn(Status.OK);
      doThrow(RuntimeException.class)
          .when(rulesManager)
          .bulkDeleteMaliciousSourcesRules(any(), eq(List.of("id1")));

      StreamObserver<BulkDeleteMaliciousSourcesRulesResponse> responseStreamObserver =
          mock(StreamObserver.class);
      Runnable runnable =
          () ->
              maliciousSourcesConfigService.bulkDeleteMaliciousSourcesRules(
                  bulkDeleteRequest, responseStreamObserver);
      RequestContext.forTenantId(TENANT_ID).run(runnable);

      verify(responseStreamObserver, times(1))
          .onError(argThat(err -> err.getClass() == RuntimeException.class));
    }
  }
}
