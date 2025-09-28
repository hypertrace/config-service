package ai.traceable.reporting.config.service.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReportingConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  ReportingConfigServiceGrpc.ReportingConfigServiceBlockingStub reportingConfigServiceStub;
  ReportingConfigRequestValidator reportingConfigRequestValidator;
  ReportingConfigManager reportingConfigManager;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();

    reportingConfigRequestValidator = mock(ReportingConfigRequestValidator.class);
    reportingConfigManager = mock(ReportingConfigManager.class);
    mockGenericConfigService
        .addService(
            new ReportingConfigServiceImpl(reportingConfigRequestValidator, reportingConfigManager))
        .start();

    reportingConfigServiceStub =
        ReportingConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @Test
  void testCreateReportConfiguration() {
    CommonConfigurationDetails commonConfigurationDetails =
        buildCommonConfigurationDetails("report1", "job1");
    ReportConfiguration reportConfiguration =
        buildReportConfiguration(commonConfigurationDetails, "id1");

    when(reportingConfigManager.createReportConfiguration(
            any(RequestContext.class), eq(commonConfigurationDetails)))
        .thenReturn(reportConfiguration);

    ReportConfiguration createdReportConfiguration =
        reportingConfigServiceStub
            .createReportConfiguration(
                CreateReportConfigurationRequest.newBuilder()
                    .setCommonConfigurationDetails(commonConfigurationDetails)
                    .build())
            .getReportConfiguration();
    assertEquals(reportConfiguration, createdReportConfiguration);
  }

  @Test
  void testCreateReportConfigurationWithOneTime() {
    CommonConfigurationDetails commonConfigurationDetails =
        buildOneTimeCommonDetails("report1", "one-time-job");
    ReportConfiguration reportConfiguration =
        buildReportConfiguration(commonConfigurationDetails, "id1");

    when(reportingConfigManager.createReportConfiguration(
            any(RequestContext.class), eq(commonConfigurationDetails)))
        .thenReturn(reportConfiguration);

    ReportConfiguration createdReportConfiguration =
        reportingConfigServiceStub
            .createReportConfiguration(
                CreateReportConfigurationRequest.newBuilder()
                    .setCommonConfigurationDetails(commonConfigurationDetails)
                    .build())
            .getReportConfiguration();
    assertEquals(reportConfiguration, createdReportConfiguration);
    assertFalse(
        createdReportConfiguration
            .getCommonConfigurationDetails()
            .getSchedulingDetails()
            .getOneTimeJobId()
            .isEmpty());
  }

  @Test
  void testCreateReportConfigurationWithScheduled() {
    CommonConfigurationDetails commonConfigurationDetails =
        buildScheduledCommonDetails("report1", "scheduled-job");
    ReportConfiguration reportConfiguration =
        buildReportConfiguration(commonConfigurationDetails, "id1");

    when(reportingConfigManager.createReportConfiguration(
            any(RequestContext.class), eq(commonConfigurationDetails)))
        .thenReturn(reportConfiguration);

    ReportConfiguration createdReportConfiguration =
        reportingConfigServiceStub
            .createReportConfiguration(
                CreateReportConfigurationRequest.newBuilder()
                    .setCommonConfigurationDetails(commonConfigurationDetails)
                    .build())
            .getReportConfiguration();
    assertEquals(reportConfiguration, createdReportConfiguration);
    assertTrue(
        createdReportConfiguration
            .getCommonConfigurationDetails()
            .getSchedulingDetails()
            .getOneTimeJobId()
            .isEmpty());
    assertFalse(
        createdReportConfiguration
            .getCommonConfigurationDetails()
            .getSchedulingDetails()
            .getScheduledJobId()
            .isEmpty());
  }

  @Test
  void testGetReportConfigurations() {
    CommonConfigurationDetails commonConfigurationDetails1 =
        buildCommonConfigurationDetails("report1", "job1");
    ReportConfiguration reportConfiguration1 =
        buildReportConfiguration(commonConfigurationDetails1, "id1");
    CommonConfigurationDetails commonConfigurationDetails2 =
        buildCommonConfigurationDetails("report2", "job2");
    ReportConfiguration reportConfiguration2 =
        buildReportConfiguration(commonConfigurationDetails1, "id2");

    when(reportingConfigManager.getReportConfigurations(
            any(RequestContext.class), any(GetReportsFilter.class)))
        .thenReturn(List.of(reportConfiguration1, reportConfiguration2));

    List<ReportConfiguration> fetchedReportConfigurations =
        reportingConfigServiceStub
            .getReportConfigurations(GetReportConfigurationsRequest.newBuilder().build())
            .getReportConfigurationsList();
    assertEquals(List.of(reportConfiguration1, reportConfiguration2), fetchedReportConfigurations);
  }

  @Test
  void testGetReportConfigurationsWithScheduleTypeOneTime() {
    CommonConfigurationDetails oneTimeConfig = buildOneTimeCommonDetails("oneTimeReport", "job1");
    ReportConfiguration oneTimeReport = buildReportConfiguration(oneTimeConfig, "id1");

    CommonConfigurationDetails scheduledConfig =
        buildScheduledCommonDetails("scheduledReport", "job2");
    ReportConfiguration scheduledReport = buildReportConfiguration(scheduledConfig, "id2");

    // Mock should return only one-time reports since filtering now happens at store level
    when(reportingConfigManager.getReportConfigurations(
            any(RequestContext.class), any(GetReportsFilter.class)))
        .thenReturn(Arrays.asList(oneTimeReport));

    List<ReportConfiguration> fetchedReportConfigurations =
        reportingConfigServiceStub
            .getReportConfigurations(
                GetReportConfigurationsRequest.newBuilder()
                    .setGetReportsFilter(
                        GetReportsFilter.newBuilder()
                            .setReportScheduleType(ReportScheduleType.REPORT_SCHEDULE_TYPE_ONE_TIME)
                            .build())
                    .build())
            .getReportConfigurationsList();

    assertEquals(1, fetchedReportConfigurations.size());
    assertEquals(oneTimeReport, fetchedReportConfigurations.get(0));
    assertFalse(
        fetchedReportConfigurations
            .get(0)
            .getCommonConfigurationDetails()
            .getSchedulingDetails()
            .getOneTimeJobId()
            .isEmpty());
  }

  @Test
  void testGetReportConfigurationsWithScheduleTypeScheduled() {
    CommonConfigurationDetails oneTimeConfig = buildOneTimeCommonDetails("oneTimeReport", "job1");
    ReportConfiguration oneTimeReport = buildReportConfiguration(oneTimeConfig, "id1");

    CommonConfigurationDetails scheduledConfig =
        buildScheduledCommonDetails("scheduledReport", "job2");
    ReportConfiguration scheduledReport = buildReportConfiguration(scheduledConfig, "id2");

    // Mock should return only scheduled reports since filtering now happens at store level
    when(reportingConfigManager.getReportConfigurations(
            any(RequestContext.class), any(GetReportsFilter.class)))
        .thenReturn(Arrays.asList(scheduledReport));

    List<ReportConfiguration> fetchedReportConfigurations =
        reportingConfigServiceStub
            .getReportConfigurations(
                GetReportConfigurationsRequest.newBuilder()
                    .setGetReportsFilter(
                        GetReportsFilter.newBuilder()
                            .setReportScheduleType(
                                ReportScheduleType.REPORT_SCHEDULE_TYPE_SCHEDULED)
                            .build())
                    .build())
            .getReportConfigurationsList();

    assertEquals(1, fetchedReportConfigurations.size());
    assertEquals(scheduledReport, fetchedReportConfigurations.get(0));
    assertTrue(
        fetchedReportConfigurations
            .get(0)
            .getCommonConfigurationDetails()
            .getSchedulingDetails()
            .getOneTimeJobId()
            .isEmpty());
  }

  @Test
  void testGetReportConfigurationsWithScheduleTypeUnspecified() {
    CommonConfigurationDetails oneTimeConfig = buildOneTimeCommonDetails("oneTimeReport", "job1");
    ReportConfiguration oneTimeReport = buildReportConfiguration(oneTimeConfig, "id1");

    CommonConfigurationDetails scheduledConfig =
        buildScheduledCommonDetails("scheduledReport", "job2");
    ReportConfiguration scheduledReport = buildReportConfiguration(scheduledConfig, "id2");

    when(reportingConfigManager.getReportConfigurations(
            any(RequestContext.class), any(GetReportsFilter.class)))
        .thenReturn(Arrays.asList(oneTimeReport, scheduledReport));

    List<ReportConfiguration> fetchedReportConfigurations =
        reportingConfigServiceStub
            .getReportConfigurations(
                GetReportConfigurationsRequest.newBuilder()
                    .setGetReportsFilter(
                        GetReportsFilter.newBuilder()
                            .setReportScheduleType(
                                ReportScheduleType.REPORT_SCHEDULE_TYPE_UNSPECIFIED)
                            .build())
                    .build())
            .getReportConfigurationsList();

    // UNSPECIFIED should return all reports (both one-time and scheduled)
    assertEquals(2, fetchedReportConfigurations.size());
    assertEquals(Arrays.asList(oneTimeReport, scheduledReport), fetchedReportConfigurations);
  }

  @Test
  void testGetReportConfigurationsWithoutScheduleType() {
    CommonConfigurationDetails oneTimeConfig = buildOneTimeCommonDetails("oneTimeReport", "job1");
    ReportConfiguration oneTimeReport = buildReportConfiguration(oneTimeConfig, "id1");

    CommonConfigurationDetails scheduledConfig =
        buildScheduledCommonDetails("scheduledReport", "job2");
    ReportConfiguration scheduledReport = buildReportConfiguration(scheduledConfig, "id2");

    when(reportingConfigManager.getReportConfigurations(
            any(RequestContext.class), any(GetReportsFilter.class)))
        .thenReturn(Arrays.asList(oneTimeReport, scheduledReport));

    List<ReportConfiguration> fetchedReportConfigurations =
        reportingConfigServiceStub
            .getReportConfigurations(
                GetReportConfigurationsRequest.newBuilder()
                    .setGetReportsFilter(GetReportsFilter.newBuilder().build())
                    .build())
            .getReportConfigurationsList();

    assertEquals(2, fetchedReportConfigurations.size());
    assertEquals(Arrays.asList(oneTimeReport, scheduledReport), fetchedReportConfigurations);
  }

  @Test
  void testGetReportConfigurationsWithMissingOneTimeField() {
    CommonConfigurationDetails configWithoutOneTime =
        buildCommonConfigurationDetails("report1", "job1");
    ReportConfiguration reportWithoutOneTime =
        buildReportConfiguration(configWithoutOneTime, "id1");

    // For ONE_TIME request, mock should return empty list since filtering happens at store level
    when(reportingConfigManager.getReportConfigurations(
            any(RequestContext.class), any(GetReportsFilter.class)))
        .thenReturn(Collections.emptyList());

    List<ReportConfiguration> oneTimeFetchedConfigurations =
        reportingConfigServiceStub
            .getReportConfigurations(
                GetReportConfigurationsRequest.newBuilder()
                    .setGetReportsFilter(
                        GetReportsFilter.newBuilder()
                            .setReportScheduleType(ReportScheduleType.REPORT_SCHEDULE_TYPE_ONE_TIME)
                            .build())
                    .build())
            .getReportConfigurationsList();
    assertEquals(0, oneTimeFetchedConfigurations.size());

    // For SCHEDULED request, mock should return the report since filtering happens at store level
    when(reportingConfigManager.getReportConfigurations(
            any(RequestContext.class), any(GetReportsFilter.class)))
        .thenReturn(Arrays.asList(reportWithoutOneTime));

    List<ReportConfiguration> scheduledFetchedConfigurations =
        reportingConfigServiceStub
            .getReportConfigurations(
                GetReportConfigurationsRequest.newBuilder()
                    .setGetReportsFilter(
                        GetReportsFilter.newBuilder()
                            .setReportScheduleType(
                                ReportScheduleType.REPORT_SCHEDULE_TYPE_SCHEDULED)
                            .build())
                    .build())
            .getReportConfigurationsList();
    assertEquals(1, scheduledFetchedConfigurations.size());
    assertEquals(reportWithoutOneTime, scheduledFetchedConfigurations.get(0));
  }

  @Test
  void testUpdateReportConfiguration() {
    CommonConfigurationDetails commonConfigurationDetails =
        buildCommonConfigurationDetails("report1", "job1");
    ReportConfiguration reportConfiguration =
        buildReportConfiguration(commonConfigurationDetails, "id1");
    when(reportingConfigManager.updateReportConfiguration(
            any(RequestContext.class), eq("id1"), eq(commonConfigurationDetails)))
        .thenReturn(reportConfiguration);
    ReportConfiguration updatedReportConfiguration =
        reportingConfigServiceStub
            .updateReportConfiguration(
                UpdateReportConfigurationRequest.newBuilder()
                    .setId("id1")
                    .setCommonConfigurationDetails(commonConfigurationDetails)
                    .build())
            .getReportConfiguration();
    assertEquals(reportConfiguration, updatedReportConfiguration);
  }

  @Test
  void testDeleteReportConfiguration() {
    CommonConfigurationDetails commonConfigurationDetails =
        buildCommonConfigurationDetails("report1", "job1");
    ReportConfiguration reportConfiguration =
        buildReportConfiguration(commonConfigurationDetails, "id1");

    when(reportingConfigManager.createReportConfiguration(
            any(RequestContext.class), eq(commonConfigurationDetails)))
        .thenReturn(reportConfiguration);

    DeleteReportConfigurationResponse response =
        reportingConfigServiceStub.deleteReportConfiguration(
            DeleteReportConfigurationRequest.newBuilder().setId("id1").build());
    assertEquals(DeleteReportConfigurationResponse.getDefaultInstance(), response);
  }

  private CommonConfigurationDetails buildCommonConfigurationDetails(
      String name, String scheduledJobId) {
    return buildScheduledCommonDetails(name, scheduledJobId);
  }

  private CommonConfigurationDetails buildOneTimeCommonDetails(String name, String oneTimeJobId) {
    return CommonConfigurationDetails.newBuilder()
        .setName(name)
        .setEnvironmentId("env1")
        .setSchedulingDetails(SchedulingDetails.newBuilder().setOneTimeJobId(oneTimeJobId).build())
        .setTemplatesConfiguration("templates")
        .setNotificationDetails(
            NotificationDetails.newBuilder()
                .addAllChannelIds(Collections.singletonList("channel1"))
                .build())
        .build();
  }

  private CommonConfigurationDetails buildScheduledCommonDetails(
      String name, String scheduledJobId) {
    return CommonConfigurationDetails.newBuilder()
        .setName(name)
        .setEnvironmentId("env1")
        .setSchedulingDetails(
            SchedulingDetails.newBuilder().setScheduledJobId(scheduledJobId).build())
        .setTemplatesConfiguration("templates")
        .setNotificationDetails(
            NotificationDetails.newBuilder()
                .addAllChannelIds(Collections.singletonList("channel1"))
                .build())
        .build();
  }

  private ReportConfiguration buildReportConfiguration(
      CommonConfigurationDetails commonConfigurationDetails, String id) {
    ReportConfiguration.Builder builder =
        ReportConfiguration.newBuilder()
            .setId(id)
            .setCreator("user1")
            .setCommonConfigurationDetails(commonConfigurationDetails);
    return builder.build();
  }
}
