package ai.traceable.reporting.config.service.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReportingConfigStoreTest {
  private ReportingConfigStore reportingConfigStore;

  @BeforeEach
  public void setup() {
    ConfigServiceBlockingStub configServiceBlockingStub = mock(ConfigServiceBlockingStub.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    reportingConfigStore =
        new ReportingConfigStore(configServiceBlockingStub, configChangeEventGenerator);
  }

  @Test
  void testFilterReportById() {
    // no id in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter = GetReportsFilter.newBuilder().build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // same id in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter = GetReportsFilter.newBuilder().setId("id1").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // different id in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter = GetReportsFilter.newBuilder().setId("id2").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertFalse(maBeReportConfig.isPresent());
    }
  }

  @Test
  void testFilterReportByEnvironmentId() {
    // no environment in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter = GetReportsFilter.newBuilder().build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // same environment in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter = GetReportsFilter.newBuilder().setEnvironmentId("env1").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // different environment in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter = GetReportsFilter.newBuilder().setEnvironmentId("env2").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertFalse(maBeReportConfig.isPresent());
    }
  }

  @Test
  void testFilterReportWithEnvironmentListById() {
    // no environment in filter
    {
      ReportConfiguration reportConfiguration = createReportConfigurationWithEnvList();
      GetReportsFilter filter = GetReportsFilter.newBuilder().build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // environment in filter is present within report environment list
    {
      ReportConfiguration reportConfiguration = createReportConfigurationWithEnvList();
      GetReportsFilter filter = GetReportsFilter.newBuilder().setEnvironmentId("env2").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // environment in filter is not present within report environment list
    {
      ReportConfiguration reportConfiguration = createReportConfigurationWithEnvList();
      GetReportsFilter filter = GetReportsFilter.newBuilder().setEnvironmentId("env1").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertFalse(maBeReportConfig.isPresent());
    }
  }

  @Test
  void testFilterReportByName() {
    // no names in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter = GetReportsFilter.newBuilder().build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // report name is present in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter =
          GetReportsFilter.newBuilder().addNames("report1").addNames("report2").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // report name is not present in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter =
          GetReportsFilter.newBuilder().addNames("report2").addNames("report3").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertFalse(maBeReportConfig.isPresent());
    }
  }

  @Test
  void testFilterReportByCreator() {
    // no names in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter = GetReportsFilter.newBuilder().build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // report creator is present in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter =
          GetReportsFilter.newBuilder().addCreators("creator1").addCreators("creator2").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertTrue(maBeReportConfig.isPresent());
      assertEquals(reportConfiguration, maBeReportConfig.get());
    }

    // report creator is not present in filter
    {
      ReportConfiguration reportConfiguration = createReportConfiguration();
      GetReportsFilter filter =
          GetReportsFilter.newBuilder().addCreators("creator2").addCreators("creator3").build();
      Optional<ReportConfiguration> maBeReportConfig =
          reportingConfigStore.filterConfigData(reportConfiguration, filter);
      assertFalse(maBeReportConfig.isPresent());
    }
  }

  private ReportConfiguration createReportConfiguration() {
    return ReportConfiguration.newBuilder()
        .setId("id1")
        .setCreator("creator1")
        .setCommonConfigurationDetails(
            CommonConfigurationDetails.newBuilder()
                .setEnvironmentId("env1")
                .setFormat(Format.FORMAT_PDF)
                .setName("report1")
                .setSchedulingDetails(
                    SchedulingDetails.newBuilder().setScheduledJobId("scheduledJob1").build())
                .setTemplatesConfiguration("template configuration")
                .setNotificationDetails(
                    NotificationDetails.newBuilder().addEmailAddresses("name@example.com").build())
                .build())
        .build();
  }

  private ReportConfiguration createReportConfigurationWithEnvList() {
    return ReportConfiguration.newBuilder()
        .setId("id1")
        .setCreator("creator1")
        .setCommonConfigurationDetails(
            CommonConfigurationDetails.newBuilder()
                .addAllEnvironmentIds(List.of("env2", "env3"))
                .setFormat(Format.FORMAT_PDF)
                .setName("report1")
                .setSchedulingDetails(
                    SchedulingDetails.newBuilder().setScheduledJobId("scheduledJob1").build())
                .setTemplatesConfiguration("template configuration")
                .setNotificationDetails(
                    NotificationDetails.newBuilder().addEmailAddresses("name@example.com").build())
                .build())
        .build();
  }
}
