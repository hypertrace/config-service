package ai.traceable.cloud.bot.deployment.config.service.v1.utils;

import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.config.service.commons.metrics.MultiTaggedCounter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.micrometer.core.instrument.MeterRegistry;

@Singleton
public class CloudBotDeploymentMetricsUtil {

  private final MultiTaggedCounter cloudBotDeploymentCounter;

  @Inject
  public CloudBotDeploymentMetricsUtil(final MeterRegistry meterRegistry) {
    if (meterRegistry != null) {
      cloudBotDeploymentCounter =
          MultiTaggedCounter.create(
              "cloud.bot.deployment.config.status.change.count",
              meterRegistry,
              "tenantId",
              "deploymentStatus",
              "configId");
    } else {
      cloudBotDeploymentCounter = MultiTaggedCounter.noop();
    }
  }

  public void incrementCloudBotDeploymentConfigStatusUpdateCount(
      final String tenantId, final DeploymentStatus deploymentStatus, final String configId) {
    cloudBotDeploymentCounter.increment(tenantId, deploymentStatus.name(), configId);
  }
}
