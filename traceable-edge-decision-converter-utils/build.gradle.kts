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

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
}

tasks.test {
  useJUnitPlatform()
}
