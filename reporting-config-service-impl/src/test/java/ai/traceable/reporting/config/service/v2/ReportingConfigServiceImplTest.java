package ai.traceable.reporting.config.service.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
