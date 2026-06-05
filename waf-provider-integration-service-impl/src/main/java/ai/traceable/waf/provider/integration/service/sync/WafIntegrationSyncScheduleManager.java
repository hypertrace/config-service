package ai.traceable.waf.provider.integration.service.sync;

import ai.traceable.job.service.query.v1.ConstantValue;
import ai.traceable.job.service.query.v1.RelationalOperator;
import ai.traceable.job.service.v1.CreateScheduledJobRequest;
import ai.traceable.job.service.v1.CronMetadata;
import ai.traceable.job.service.v1.DeleteScheduledJobRequest;
import ai.traceable.job.service.v1.JobServiceGrpc.JobServiceBlockingStub;
import ai.traceable.job.service.v1.JobSpec;
import ai.traceable.job.service.v1.JobSpecField;
import ai.traceable.job.service.v1.JobType;
import ai.traceable.job.service.v1.QueryScheduleRequest;
import ai.traceable.job.service.v1.QueryScheduleResponse;
import ai.traceable.job.service.v1.ScheduleField;
import ai.traceable.job.service.v1.ScheduleFilter;
import ai.traceable.job.service.v1.ScheduleFilterExpression;
import ai.traceable.job.service.v1.ScheduleQuery;
import ai.traceable.job.service.v1.ScheduleRelationalFilter;
import ai.traceable.job.service.v1.ScheduleSelection;
import ai.traceable.job.service.v1.ScheduledJobField;
import ai.traceable.job.service.v1.ScheduledJobStatus;
import ai.traceable.job.service.v1.Source;
import ai.traceable.job.service.v1.UpdateScheduledJobField;
import ai.traceable.job.service.v1.UpdateScheduledJobRequest;
import ai.traceable.job.service.waf.integration.sync.v1.WafIntegrationSyncJobSpec;
import ai.traceable.job.service.waf.integration.sync.v1.WafIntegrationSyncJobSpecField;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationSyncSchedule;
import ai.traceable.waf.integration.service.api.v1.WafSyncCronMetadata;
import ai.traceable.waf.integration.service.api.v1.WafSyncScheduleStatus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
public class WafIntegrationSyncScheduleManager {
  private static final String WAF_INTEGRATION_SYNC_JOB_NAME = "waf-integration-sync";
  private static final String SCHEDULE_ID_ALIAS = "id";
  private static final String SCHEDULE_STATUS_ALIAS = "status";
  private static final String SCHEDULE_CRON_EXPRESSION_ALIAS = "cron_expression";
  private static final String SCHEDULE_ZONE_OFFSET_ALIAS = "zone_offset";
  private static final String TENANT_ID_REQUIRED_ERROR_MESSAGE = "Tenant ID is required";

  private final JobServiceBlockingStub jobServiceBlockingStub;
  private final JobServiceClientConfig jobServiceClientConfig;

  @Inject
  WafIntegrationSyncScheduleManager(
      final JobServiceBlockingStub jobServiceBlockingStub,
      final JobServiceClientConfig jobServiceClientConfig) {
    this.jobServiceBlockingStub = jobServiceBlockingStub;
    this.jobServiceClientConfig = jobServiceClientConfig;
  }

  public void enableOrUpdateSchedule(
      final RequestContext requestContext,
      final String integrationId,
      final WafSyncCronMetadata cronMetadata) {
    final String tenantId = getTenantIdOrThrow(requestContext);
    log.debug(
        "Starting enableOrUpdateSchedule for integrationId={} tenantId={} cronExpression={} zoneOffset={}",
        integrationId,
        tenantId,
        cronMetadata.getCronExpression(),
        cronMetadata.getZoneOffset());

    final List<String> existingScheduleIds =
        findScheduleIdsForIntegration(requestContext, integrationId);
    final CronMetadata jobCronMetadata =
        CronMetadata.newBuilder()
            .setCronExpression(cronMetadata.getCronExpression())
            .setZoneOffset(cronMetadata.getZoneOffset())
            .build();

    log.debug(
        "Found {} existing WAF integration sync schedules for integrationId={} tenantId={} scheduleIds={}",
        existingScheduleIds.size(),
        integrationId,
        tenantId,
        existingScheduleIds);

    if (existingScheduleIds.isEmpty()) {
      log.info(
          "No existing schedules found. Creating new schedule for integrationId={} tenantId={}",
          integrationId,
          tenantId);
      createSchedule(requestContext, integrationId, jobCronMetadata);
      return;
    }

    // Reconcile duplicate schedules defensively: duplicates can appear due to retries, races,
    // or lack of a strict uniqueness constraint in upstream schedule storage.
    // We update one existing schedule in place and then delete extras (instead of deleting all
    // and recreating) to preserve continuity: if cleanup partially fails, at least one valid
    // schedule still exists for this integration.
    final String scheduleIdToKeep = existingScheduleIds.get(0);
    log.info(
        "Updating existing schedule for integrationId={} tenantId={} scheduleIdToKeep={}",
        integrationId,
        tenantId,
        scheduleIdToKeep);
    updateScheduleCron(requestContext, scheduleIdToKeep, jobCronMetadata);

    final List<String> scheduleIdsToDelete =
        existingScheduleIds.stream().skip(1).collect(Collectors.toUnmodifiableList());
    log.info(
        "Deleting {} duplicate schedules for integrationId={} tenantId={} duplicateScheduleIds={}",
        scheduleIdsToDelete.size(),
        integrationId,
        tenantId,
        scheduleIdsToDelete);

    existingScheduleIds.stream()
        .skip(1)
        .forEach(scheduleId -> deleteSchedule(requestContext, scheduleId));

    log.debug(
        "Completed enableOrUpdateSchedule for integrationId={} tenantId={} scheduleIdKept={} duplicatesDeleted={}",
        integrationId,
        tenantId,
        scheduleIdToKeep,
        scheduleIdsToDelete.size());
  }

  public Optional<WafIntegrationSyncSchedule> getScheduleForIntegration(
      final RequestContext requestContext, final String integrationId) {
    final String tenantId = getTenantIdOrThrow(requestContext);
    log.debug(
        "Querying schedule for integrationId={} tenantId={} jobType={}",
        integrationId,
        tenantId,
        JobType.JOB_TYPE_WAF_INTEGRATION_SYNC);

    final ScheduleFilter integrationIdFilter = buildIntegrationIdFilter(integrationId);

    final ScheduleQuery query =
        ScheduleQuery.newBuilder()
            .addSelections(
                buildScheduledJobSelection(
                    SCHEDULE_ID_ALIAS, ScheduledJobField.SCHEDULED_JOB_FIELD_ID))
            .addSelections(
                buildScheduledJobSelection(
                    SCHEDULE_STATUS_ALIAS, ScheduledJobField.SCHEDULED_JOB_FIELD_STATUS))
            .addSelections(
                buildScheduledJobSelection(
                    SCHEDULE_CRON_EXPRESSION_ALIAS,
                    ScheduledJobField.SCHEDULED_JOB_FIELD_CRON_EXPRESSION))
            .addSelections(
                buildScheduledJobSelection(
                    SCHEDULE_ZONE_OFFSET_ALIAS, ScheduledJobField.SCHEDULED_JOB_FIELD_ZONE_OFFSET))
            .setFilter(integrationIdFilter)
            .build();

    final QueryScheduleResponse response =
        requestContext.call(
            () ->
                jobServiceBlockingStub
                    .withDeadlineAfter(
                        jobServiceClientConfig.getRequestTimeout().toMillis(),
                        TimeUnit.MILLISECONDS)
                    .querySchedule(
                        QueryScheduleRequest.newBuilder()
                            .setJobType(JobType.JOB_TYPE_WAF_INTEGRATION_SYNC)
                            .setQuery(query)
                            .build()));

    final int resultRowCount = response.getResultSet().getRowsCount();
    if (resultRowCount > 1) {
      log.warn(
          "Found {} waf sync schedule for integrationId={} tenantId={}; returning first schedule only",
          resultRowCount,
          integrationId,
          tenantId);
    }

    return response.getResultSet().getRowsList().stream()
        .findFirst()
        .map(row -> toWafIntegrationSyncSchedule(row.getFieldsMap()));
  }

  public void deleteSchedulesForIntegration(
      final RequestContext requestContext, final String integrationId) {
    final String tenantId = getTenantIdOrThrow(requestContext);
    log.debug(
        "Starting deleteSchedulesForIntegration for integrationId={} tenantId={}",
        integrationId,
        tenantId);

    final List<String> scheduleIds = findScheduleIdsForIntegration(requestContext, integrationId);

    log.info(
        "Found {} schedules to delete for integrationId={} tenantId={} scheduleIds={}",
        scheduleIds.size(),
        integrationId,
        tenantId,
        scheduleIds);

    scheduleIds.forEach(scheduleId -> deleteSchedule(requestContext, scheduleId));

    log.debug(
        "Completed deleteSchedulesForIntegration for integrationId={} tenantId={} deletedSchedules={}",
        integrationId,
        tenantId,
        scheduleIds.size());
  }

  private List<String> findScheduleIdsForIntegration(
      final RequestContext requestContext, final String integrationId) {
    final String tenantId = getTenantIdOrThrow(requestContext);
    log.debug(
        "Querying schedule ids for integrationId={} tenantId={} jobType={}",
        integrationId,
        tenantId,
        JobType.JOB_TYPE_WAF_INTEGRATION_SYNC);

    final ScheduleFilter integrationIdFilter = buildIntegrationIdFilter(integrationId);

    final ScheduleQuery query =
        ScheduleQuery.newBuilder()
            .addSelections(
                buildScheduledJobSelection(
                    SCHEDULE_ID_ALIAS, ScheduledJobField.SCHEDULED_JOB_FIELD_ID))
            .setFilter(integrationIdFilter)
            .build();

    final QueryScheduleResponse response =
        requestContext.call(
            () ->
                jobServiceBlockingStub
                    .withDeadlineAfter(
                        jobServiceClientConfig.getRequestTimeout().toMillis(),
                        TimeUnit.MILLISECONDS)
                    .querySchedule(
                        QueryScheduleRequest.newBuilder()
                            .setJobType(JobType.JOB_TYPE_WAF_INTEGRATION_SYNC)
                            .setQuery(query)
                            .build()));

    final List<String> scheduleIds =
        response.getResultSet().getRowsList().stream()
            .map(
                row ->
                    row.getFieldsOrDefault(SCHEDULE_ID_ALIAS, ConstantValue.getDefaultInstance()))
            .map(ConstantValue::getStringValue)
            .filter(scheduleId -> !scheduleId.isEmpty())
            .collect(Collectors.toUnmodifiableList());

    log.debug(
        "QuerySchedule returned {} schedule ids for integrationId={} tenantId={} scheduleIds={}",
        scheduleIds.size(),
        integrationId,
        tenantId,
        scheduleIds);

    return scheduleIds;
  }

  private void createSchedule(
      final RequestContext requestContext,
      final String integrationId,
      final CronMetadata cronMetadata) {
    final String tenantId = getTenantIdOrThrow(requestContext);
    log.debug(
        "Creating scheduled job for integrationId={} tenantId={} jobName={} cronExpression={} zoneOffset={}",
        integrationId,
        tenantId,
        WAF_INTEGRATION_SYNC_JOB_NAME,
        cronMetadata.getCronExpression(),
        cronMetadata.getZoneOffset());

    final JobSpec spec =
        JobSpec.newBuilder()
            .setWafIntegrationSyncJobSpec(
                WafIntegrationSyncJobSpec.newBuilder().setIntegrationId(integrationId))
            .build();

    final CreateScheduledJobRequest request =
        CreateScheduledJobRequest.newBuilder()
            .setName(WAF_INTEGRATION_SYNC_JOB_NAME)
            .setSpec(spec)
            .setCronMetadata(cronMetadata)
            .setSource(Source.SOURCE_USER)
            .setScheduledJobStatus(ScheduledJobStatus.SCHEDULED_JOB_STATUS_ENABLED)
            .build();

    requestContext.call(
        () ->
            jobServiceBlockingStub
                .withDeadlineAfter(
                    jobServiceClientConfig.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .createScheduledJob(request));

    log.info(
        "Created scheduled job successfully for integrationId={} tenantId={} jobName={}",
        integrationId,
        tenantId,
        WAF_INTEGRATION_SYNC_JOB_NAME);
  }

  private void updateScheduleCron(
      final RequestContext requestContext,
      final String scheduleId,
      final CronMetadata cronMetadata) {
    final String tenantId = getTenantIdOrThrow(requestContext);
    log.debug(
        "Updating schedule cron for scheduleId={} tenantId={} cronExpression={} zoneOffset={}",
        scheduleId,
        tenantId,
        cronMetadata.getCronExpression(),
        cronMetadata.getZoneOffset());

    final UpdateScheduledJobRequest request =
        UpdateScheduledJobRequest.newBuilder()
            .setId(scheduleId)
            .addUpdateScheduledJobFields(
                UpdateScheduledJobField.newBuilder().setCronMetadata(cronMetadata))
            .build();

    requestContext.call(
        () ->
            jobServiceBlockingStub
                .withDeadlineAfter(
                    jobServiceClientConfig.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .updateScheduledJob(request));

    log.info(
        "Updated schedule cron successfully for scheduleId={} tenantId={}", scheduleId, tenantId);
  }

  private void deleteSchedule(final RequestContext requestContext, final String scheduleId) {
    final String tenantId = getTenantIdOrThrow(requestContext);
    log.debug("Deleting schedule scheduleId={} tenantId={}", scheduleId, tenantId);

    requestContext.call(
        () ->
            jobServiceBlockingStub
                .withDeadlineAfter(
                    jobServiceClientConfig.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .deleteScheduledJob(
                    DeleteScheduledJobRequest.newBuilder().setId(scheduleId).build()));

    log.info("Deleted schedule successfully scheduleId={} tenantId={}", scheduleId, tenantId);
  }

  private static ScheduleFilter buildIntegrationIdFilter(final String integrationId) {
    return ScheduleFilter.newBuilder()
        .setRelationalFilter(
            ScheduleRelationalFilter.newBuilder()
                .setLeftOperand(
                    ScheduleFilterExpression.newBuilder()
                        .setField(
                            ScheduleField.newBuilder()
                                .setJobSpecField(
                                    JobSpecField.newBuilder()
                                        .setWafIntegrationSyncSpecField(
                                            WafIntegrationSyncJobSpecField
                                                .WAF_INTEGRATION_SYNC_JOB_SPEC_FIELD_INTEGRATION_ID))))
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                .setRightOperand(
                    ScheduleFilterExpression.newBuilder()
                        .setConstantValue(
                            ConstantValue.newBuilder().setStringValue(integrationId))))
        .build();
  }

  private static ScheduleSelection buildScheduledJobSelection(
      final String alias, final ScheduledJobField field) {
    return ScheduleSelection.newBuilder()
        .setAlias(alias)
        .setField(ScheduleField.newBuilder().setScheduledJobField(field))
        .build();
  }

  private static WafIntegrationSyncSchedule toWafIntegrationSyncSchedule(
      final Map<String, ConstantValue> fields) {
    final String scheduleId =
        fields.getOrDefault(SCHEDULE_ID_ALIAS, ConstantValue.getDefaultInstance()).getStringValue();
    final String statusName =
        fields
            .getOrDefault(SCHEDULE_STATUS_ALIAS, ConstantValue.getDefaultInstance())
            .getStringValue();
    final String cronExpression =
        fields
            .getOrDefault(SCHEDULE_CRON_EXPRESSION_ALIAS, ConstantValue.getDefaultInstance())
            .getStringValue();
    final String zoneOffset =
        fields
            .getOrDefault(SCHEDULE_ZONE_OFFSET_ALIAS, ConstantValue.getDefaultInstance())
            .getStringValue();

    return WafIntegrationSyncSchedule.newBuilder()
        .setScheduleId(scheduleId)
        .setStatus(toWafSyncScheduleStatus(statusName))
        .setCronMetadata(
            WafSyncCronMetadata.newBuilder()
                .setCronExpression(cronExpression)
                .setZoneOffset(zoneOffset)
                .build())
        .build();
  }

  private static WafSyncScheduleStatus toWafSyncScheduleStatus(final String statusName) {
    if (statusName.isEmpty()) {
      return WafSyncScheduleStatus.WAF_SYNC_SCHEDULE_STATUS_UNSPECIFIED;
    }
    final ScheduledJobStatus scheduledJobStatus;
    try {
      scheduledJobStatus = ScheduledJobStatus.valueOf(statusName);
    } catch (final IllegalArgumentException e) {
      log.warn(
          "Unknown scheduled job status name received from job-service statusName={}; defaulting to UNSPECIFIED",
          statusName);
      return WafSyncScheduleStatus.WAF_SYNC_SCHEDULE_STATUS_UNSPECIFIED;
    }
    switch (scheduledJobStatus) {
      case SCHEDULED_JOB_STATUS_ENABLED:
        return WafSyncScheduleStatus.WAF_SYNC_SCHEDULE_STATUS_ENABLED;
      case SCHEDULED_JOB_STATUS_DISABLED:
        return WafSyncScheduleStatus.WAF_SYNC_SCHEDULE_STATUS_DISABLED;
      case SCHEDULED_JOB_STATUS_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        return WafSyncScheduleStatus.WAF_SYNC_SCHEDULE_STATUS_UNSPECIFIED;
    }
  }

  private String getTenantIdOrThrow(final RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(
            () ->
                Status.INVALID_ARGUMENT
                    .withDescription(TENANT_ID_REQUIRED_ERROR_MESSAGE)
                    .asRuntimeException());
  }
}
