import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("com.google.protobuf") version "0.8.17"
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${libs.versions.protoc.get()}"
  }
}

dependencies {
  api(projects.licenseStatusConfigServiceApi)
  api(projects.traceableSpanProcessingConfigServiceApi)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(projects.configUtils)

  implementation(libs.guice)
  implementation(libs.slf4j.api)
  implementation(libs.typesafe.config)
  implementation(libs.protobuf.javautil)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.changeeventgenerator)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java")
    }
  }
}

tasks.test {
  useJUnitPlatform()
}
