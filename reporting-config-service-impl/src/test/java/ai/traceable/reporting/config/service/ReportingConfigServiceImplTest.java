package ai.traceable.reporting.config.service;

import static ai.traceable.reporting.config.service.v1.ReportConfigurationDetails.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import ai.traceable.reporting.config.service.v1.CreateReportConfigurationRequest;
import ai.traceable.reporting.config.service.v1.DeleteReportConfigurationRequest;
import ai.traceable.reporting.config.service.v1.GetAllReportConfigurationsRequest;
import ai.traceable.reporting.config.service.v1.ReportConfiguration;
import ai.traceable.reporting.config.service.v1.ReportConfigurationDetails;
import ai.traceable.reporting.config.service.v1.ReportConfigurationDetails.ReportFrequency.DayOfWeek;
import ai.traceable.reporting.config.service.v1.ReportConfigurationDetails.ReportFrequency.WeeklyFrequency;
import ai.traceable.reporting.config.service.v1.ReportingConfigServiceGrpc;
import ai.traceable.reporting.config.service.v1.UpdateReportConfigurationRequest;
import java.util.Collections;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReportingConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  ReportingConfigServiceGrpc.ReportingConfigServiceBlockingStub reportingStub;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockGenericConfigService
        .addService(
            new ReportingConfigServiceImpl(
                mockGenericConfigService.channel(), configChangeEventGenerator))
        .start();

    reportingStub = ReportingConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @Test
  void createReadUpdateDeleteReportConfigurations() {
    ReportConfigurationDetails reportConfigurationDetails1 =
        getReportConfigurationDetails(
            ReportFrequency.newBuilder()
                .setDailyFrequency(
                    ReportFrequency.DailyFrequency.newBuilder().setScheduledTime("07:00").build())
                .build());
    ReportConfigurationDetails reportConfigurationDetails2 =
        getReportConfigurationDetails(
            ReportFrequency.newBuilder()
                .setInstantFrequency(ReportFrequency.InstantFrequency.getDefaultInstance())
                .build());
    ReportConfiguration reportConfiguration1 =
        reportingStub
            .createReportConfiguration(
                CreateReportConfigurationRequest.newBuilder()
                    .setReportConfigurationDetails(reportConfigurationDetails1)
                    .build())
            .getReportConfiguration();
    assertEquals(
        getReportConfiguration(reportConfigurationDetails1, reportConfiguration1.getId()),
        reportConfiguration1);
    assertEquals(
        reportConfiguration1,
        reportingStub
            .getAllReportConfigurations(GetAllReportConfigurationsRequest.newBuilder().build())
            .getReportConfigurations(0));
    ReportConfiguration reportConfiguration2 =
        reportingStub
            .createReportConfiguration(
                CreateReportConfigurationRequest.newBuilder()
                    .setReportConfigurationDetails(reportConfigurationDetails2)
                    .build())
            .getReportConfiguration();

    assertEquals(
        reportConfiguration2,
        reportingStub
            .getAllReportConfigurations(GetAllReportConfigurationsRequest.newBuilder().build())
            .getReportConfigurations(0));

    assertEquals(
        List.of(reportConfiguration2, reportConfiguration1),
        reportingStub
            .getAllReportConfigurations(GetAllReportConfigurationsRequest.getDefaultInstance())
            .getReportConfigurationsList());

    ReportConfiguration ruleToUpdate =
        reportConfiguration1.toBuilder()
            .setReportConfigurationDetails(
                reportConfiguration1.getReportConfigurationDetails().toBuilder()
                    .setReportFrequency(
                        ReportFrequency.newBuilder()
                            .setWeeklyFrequency(
                                WeeklyFrequency.newBuilder()
                                    .setScheduledDay(DayOfWeek.DAY_OF_WEEK_MONDAY)
                                    .setScheduledTime("07:00")
                                    .build()))
                    .build())
            .build();
    ReportConfiguration updatedRule =
        reportingStub
            .updateReportConfiguration(
                UpdateReportConfigurationRequest.newBuilder()
                    .setReportConfigurationDetails(ruleToUpdate.getReportConfigurationDetails())
                    .setId(ruleToUpdate.getId())
                    .build())
            .getReportConfiguration();
    assertEquals(ruleToUpdate, updatedRule);

    assertEquals(
        List.of(reportConfiguration2, updatedRule),
        reportingStub
            .getAllReportConfigurations(GetAllReportConfigurationsRequest.getDefaultInstance())
            .getReportConfigurationsList());

    reportingStub.deleteReportConfiguration(
        DeleteReportConfigurationRequest.newBuilder().setId(reportConfiguration2.getId()).build());
    assertEquals(
        List.of(updatedRule),
        reportingStub
            .getAllReportConfigurations(GetAllReportConfigurationsRequest.getDefaultInstance())
            .getReportConfigurationsList());
  }

  private ReportConfigurationDetails getReportConfigurationDetails(
      ReportFrequency reportFrequency) {
    return newBuilder()
        .setIsEnabled(true)
        .setReportFrequency(reportFrequency)
        .addAllChannelIds(Collections.singletonList("channel1"))
        .build();
  }

  private ReportConfiguration getReportConfiguration(
      ReportConfigurationDetails notificationRuleMutableData, String id) {
    ReportConfiguration.Builder builder =
        ReportConfiguration.newBuilder()
            .setId(id)
            .setReportConfigurationDetails(notificationRuleMutableData);
    return builder.build();
  }
}
