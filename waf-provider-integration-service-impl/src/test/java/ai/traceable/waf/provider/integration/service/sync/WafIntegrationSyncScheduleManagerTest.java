package ai.traceable.waf.provider.integration.service.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.job.service.query.v1.ConstantValue;
import ai.traceable.job.service.query.v1.ResultSet;
import ai.traceable.job.service.query.v1.Row;
import ai.traceable.job.service.v1.CreateScheduledJobRequest;
import ai.traceable.job.service.v1.CreateScheduledJobResponse;
import ai.traceable.job.service.v1.DeleteScheduledJobRequest;
import ai.traceable.job.service.v1.DeleteScheduledJobResponse;
import ai.traceable.job.service.v1.JobServiceGrpc.JobServiceBlockingStub;
import ai.traceable.job.service.v1.QueryScheduleResponse;
import ai.traceable.job.service.v1.ScheduledJobStatus;
import ai.traceable.job.service.v1.UpdateScheduledJobRequest;
import ai.traceable.job.service.v1.UpdateScheduledJobResponse;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationSyncSchedule;
import ai.traceable.waf.integration.service.api.v1.WafSyncScheduleStatus;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class WafIntegrationSyncScheduleManagerTest {

  private JobServiceBlockingStub jobServiceBlockingStub;
  private WafIntegrationSyncScheduleManager manager;
  private RequestContext requestContext;

  @BeforeEach
  void setup() {
    this.jobServiceBlockingStub = mock(JobServiceBlockingStub.class);
    this.requestContext = mock(RequestContext.class);
    JobServiceClientConfig jobServiceClientConfig =
        new JobServiceClientConfig("localhost", 50051, Duration.ofSeconds(5));

    when(jobServiceBlockingStub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
        .thenReturn(jobServiceBlockingStub);
    when(requestContext.getTenantId()).thenReturn(Optional.of("test-tenant"));

    Mockito.doAnswer(
            invocation -> {
              Callable<?> callable = invocation.getArgument(0, Callable.class);
              return callable.call();
            })
        .when(requestContext)
        .call(any());

    this.manager =
        new WafIntegrationSyncScheduleManager(jobServiceBlockingStub, jobServiceClientConfig);
  }

  @Test
  void enableOrUpdateScheduleCreatesWhenNoExistingSchedule() {
    when(jobServiceBlockingStub.querySchedule(any()))
        .thenReturn(
            QueryScheduleResponse.newBuilder()
                .setResultSet(ResultSet.getDefaultInstance())
                .build());
    when(jobServiceBlockingStub.createScheduledJob(any()))
        .thenReturn(CreateScheduledJobResponse.getDefaultInstance());

    manager.enableOrUpdateSchedule(
        requestContext,
        "integration-1",
        ai.traceable.waf.integration.service.api.v1.WafSyncCronMetadata.newBuilder()
            .setCronExpression("0 0 0 ? * *")
            .setZoneOffset("+00:00")
            .build());

    ArgumentCaptor<CreateScheduledJobRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateScheduledJobRequest.class);
    verify(jobServiceBlockingStub).createScheduledJob(requestCaptor.capture());
    CreateScheduledJobRequest createRequest = requestCaptor.getValue();

    assertEquals("waf-integration-sync", createRequest.getName());
    assertEquals(
        "integration-1", createRequest.getSpec().getWafIntegrationSyncJobSpec().getIntegrationId());
    assertEquals("0 0 0 ? * *", createRequest.getCronMetadata().getCronExpression());
    assertEquals("+00:00", createRequest.getCronMetadata().getZoneOffset());

    verify(jobServiceBlockingStub, never()).updateScheduledJob(any());
    verify(jobServiceBlockingStub, never()).deleteScheduledJob(any());
  }

  @Test
  void enableOrUpdateScheduleUpdatesFirstAndDeletesDuplicates() {
    QueryScheduleResponse queryResponse =
        QueryScheduleResponse.newBuilder()
            .setResultSet(
                ResultSet.newBuilder()
                    .addRows(
                        Row.newBuilder()
                            .putFields(
                                "id", ConstantValue.newBuilder().setStringValue("s1").build())
                            .build())
                    .addRows(
                        Row.newBuilder()
                            .putFields(
                                "id", ConstantValue.newBuilder().setStringValue("s2").build())
                            .build())
                    .addRows(
                        Row.newBuilder()
                            .putFields(
                                "id", ConstantValue.newBuilder().setStringValue("s3").build())
                            .build())
                    .build())
            .build();

    when(jobServiceBlockingStub.querySchedule(any())).thenReturn(queryResponse);
    when(jobServiceBlockingStub.updateScheduledJob(any()))
        .thenReturn(UpdateScheduledJobResponse.getDefaultInstance());
    when(jobServiceBlockingStub.deleteScheduledJob(any()))
        .thenReturn(DeleteScheduledJobResponse.getDefaultInstance());

    manager.enableOrUpdateSchedule(
        requestContext,
        "integration-2",
        ai.traceable.waf.integration.service.api.v1.WafSyncCronMetadata.newBuilder()
            .setCronExpression("0 */15 * ? * *")
            .setZoneOffset("+05:30")
            .build());

    ArgumentCaptor<UpdateScheduledJobRequest> updateCaptor =
        ArgumentCaptor.forClass(UpdateScheduledJobRequest.class);
    verify(jobServiceBlockingStub).updateScheduledJob(updateCaptor.capture());
    assertEquals("s1", updateCaptor.getValue().getId());
    assertEquals(
        "0 */15 * ? * *",
        updateCaptor
            .getValue()
            .getUpdateScheduledJobFields(0)
            .getCronMetadata()
            .getCronExpression());
    assertEquals(
        "+05:30",
        updateCaptor.getValue().getUpdateScheduledJobFields(0).getCronMetadata().getZoneOffset());

    ArgumentCaptor<DeleteScheduledJobRequest> deleteCaptor =
        ArgumentCaptor.forClass(DeleteScheduledJobRequest.class);
    verify(jobServiceBlockingStub, Mockito.times(2)).deleteScheduledJob(deleteCaptor.capture());
    assertEquals("s2", deleteCaptor.getAllValues().get(0).getId());
    assertEquals("s3", deleteCaptor.getAllValues().get(1).getId());

    verify(jobServiceBlockingStub, never()).createScheduledJob(any());
  }

  @Test
  void deleteSchedulesForIntegrationDeletesAllMatchingSchedules() {
    QueryScheduleResponse queryResponse =
        QueryScheduleResponse.newBuilder()
            .setResultSet(
                ResultSet.newBuilder()
                    .addRows(
                        Row.newBuilder()
                            .putFields(
                                "id", ConstantValue.newBuilder().setStringValue("d1").build())
                            .build())
                    .addRows(
                        Row.newBuilder()
                            .putFields(
                                "id", ConstantValue.newBuilder().setStringValue("d2").build())
                            .build())
                    .addRows(
                        Row.newBuilder()
                            .putFields("id", ConstantValue.newBuilder().setStringValue("").build())
                            .build())
                    .build())
            .build();

    when(jobServiceBlockingStub.querySchedule(any())).thenReturn(queryResponse);
    when(jobServiceBlockingStub.deleteScheduledJob(any()))
        .thenReturn(DeleteScheduledJobResponse.getDefaultInstance());

    manager.deleteSchedulesForIntegration(requestContext, "integration-3");

    ArgumentCaptor<DeleteScheduledJobRequest> deleteCaptor =
        ArgumentCaptor.forClass(DeleteScheduledJobRequest.class);
    verify(jobServiceBlockingStub, Mockito.times(2)).deleteScheduledJob(deleteCaptor.capture());
    assertEquals("d1", deleteCaptor.getAllValues().get(0).getId());
    assertEquals("d2", deleteCaptor.getAllValues().get(1).getId());

    verify(jobServiceBlockingStub, never()).createScheduledJob(any());
    verify(jobServiceBlockingStub, never()).updateScheduledJob(any());
  }

  @Test
  void getScheduleForIntegrationReturnsEmptyWhenNoMatchingSchedule() {
    when(jobServiceBlockingStub.querySchedule(any()))
        .thenReturn(
            QueryScheduleResponse.newBuilder()
                .setResultSet(ResultSet.getDefaultInstance())
                .build());

    final Optional<WafIntegrationSyncSchedule> result =
        manager.getScheduleForIntegration(requestContext, "integration-missing");

    assertFalse(result.isPresent());
  }

  @Test
  void getScheduleForIntegrationMapsRowToSchedule() {
    final QueryScheduleResponse queryResponse =
        QueryScheduleResponse.newBuilder()
            .setResultSet(
                ResultSet.newBuilder()
                    .addRows(
                        Row.newBuilder()
                            .putFields(
                                "id",
                                ConstantValue.newBuilder().setStringValue("schedule-1").build())
                            .putFields(
                                "status",
                                ConstantValue.newBuilder()
                                    .setStringValue(
                                        ScheduledJobStatus.SCHEDULED_JOB_STATUS_ENABLED.name())
                                    .build())
                            .putFields(
                                "cron_expression",
                                ConstantValue.newBuilder().setStringValue("0 0 1 ? * *").build())
                            .putFields(
                                "zone_offset",
                                ConstantValue.newBuilder().setStringValue("+05:30").build())
                            .build())
                    .build())
            .build();

    when(jobServiceBlockingStub.querySchedule(any())).thenReturn(queryResponse);

    final Optional<WafIntegrationSyncSchedule> result =
        manager.getScheduleForIntegration(requestContext, "integration-1");

    assertTrue(result.isPresent());
    final WafIntegrationSyncSchedule schedule = result.get();
    assertEquals("schedule-1", schedule.getScheduleId());
    assertEquals(WafSyncScheduleStatus.WAF_SYNC_SCHEDULE_STATUS_ENABLED, schedule.getStatus());
    assertEquals("0 0 1 ? * *", schedule.getCronMetadata().getCronExpression());
    assertEquals("+05:30", schedule.getCronMetadata().getZoneOffset());

    final ArgumentCaptor<ai.traceable.job.service.v1.QueryScheduleRequest> queryCaptor =
        ArgumentCaptor.forClass(ai.traceable.job.service.v1.QueryScheduleRequest.class);
    verify(jobServiceBlockingStub).querySchedule(queryCaptor.capture());
    assertEquals(
        "integration-1",
        queryCaptor
            .getValue()
            .getQuery()
            .getFilter()
            .getRelationalFilter()
            .getRightOperand()
            .getConstantValue()
            .getStringValue());
  }

  @Test
  void queryUsesProvidedIntegrationIdFilter() {
    when(jobServiceBlockingStub.querySchedule(any()))
        .thenReturn(
            QueryScheduleResponse.newBuilder()
                .setResultSet(ResultSet.getDefaultInstance())
                .build());
    when(jobServiceBlockingStub.createScheduledJob(any()))
        .thenReturn(CreateScheduledJobResponse.getDefaultInstance());

    manager.enableOrUpdateSchedule(
        requestContext,
        "integration-filter-check",
        ai.traceable.waf.integration.service.api.v1.WafSyncCronMetadata.newBuilder()
            .setCronExpression("0 0 1 ? * *")
            .setZoneOffset("+00:00")
            .build());

    ArgumentCaptor<ai.traceable.job.service.v1.QueryScheduleRequest> queryCaptor =
        ArgumentCaptor.forClass(ai.traceable.job.service.v1.QueryScheduleRequest.class);
    verify(jobServiceBlockingStub).querySchedule(queryCaptor.capture());
    assertEquals(
        "integration-filter-check",
        queryCaptor
            .getValue()
            .getQuery()
            .getFilter()
            .getRelationalFilter()
            .getRightOperand()
            .getConstantValue()
            .getStringValue());
  }
}
