plugins {
  `java-library`
  id("ai.traceable.publish-plugin")
}

dependencies {
  api(projects.anomalyConfigServiceApi)
  api(libs.javax.annotation)
  api(libs.typesafe.config)

  implementation(projects.anomalyConfigServiceUtils)
  implementation(libs.guice)
  implementation(libs.protobuf.javautil)
  implementation(libs.re2j)
  implementation(libs.slf4j.api)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.commons.lang)
  testImplementation(libs.traceable.platform.jnimodsecurity)
}

tasks.test {
  useJUnitPlatform()
}
