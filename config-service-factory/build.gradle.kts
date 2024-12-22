plugins {
  `java-library`
}

dependencies {
  api(commonLibs.hypertrace.framework.grpc.jakarta)

  implementation(projects.configServiceChangeEventGenerator)
  implementation(commonLibs.hypertrace.documentstore)
  implementation(projects.configServiceImpl)
  implementation(projects.spacesConfigServiceImpl)
  implementation(projects.labelsConfigServiceImpl)
  implementation(projects.labelApplicationRuleConfigServiceImpl)
  implementation(projects.alertingConfigServiceImpl)
  implementation(projects.notificationRuleConfigServiceImpl)
  implementation(projects.notificationChannelConfigServiceImpl)
  implementation(projects.spanProcessingConfigServiceImpl)
  implementation(commonLibs.hypertrace.framework.documentstore.metrics.jakarta)
}
