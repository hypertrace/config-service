plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  implementation(projects.traceableDatamodelConfigServiceApi)
  implementation(projects.traceableEdgeDecisionConfigServiceApi)

  implementation(commonLibs.guice7)

  testImplementation(commonLibs.bundles.junit.mockito)
}

tasks.test {
  useJUnitPlatform()
}
