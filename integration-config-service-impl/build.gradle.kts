import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.17"
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.integrationConfigServiceApi)

  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.guice)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${libs.versions.protoc.get()}"
  }
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
