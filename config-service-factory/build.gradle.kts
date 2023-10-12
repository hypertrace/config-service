plugins {
  `java-library`
}

dependencies {
  api(libs.hypertrace.grpc.framework)

  implementation(projects.configServiceChangeEventGenerator)
  implementation(libs.hypertrace.documentstore)
  implementation(projects.configServiceImpl)
  implementation(projects.spacesConfigServiceImpl)
  implementation(projects.labelsConfigServiceImpl)
  implementation(projects.labelApplicationRuleConfigServiceImpl)
  implementation(projects.alertingConfigServiceImpl)
  implementation(projects.notificationRuleConfigServiceImpl)
  implementation(projects.notificationChannelConfigServiceImpl)
  implementation(projects.spanProcessingConfigServiceImpl)
}
