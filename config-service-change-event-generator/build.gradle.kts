plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.configServiceChangeEventApi)
  api(libs.typesafe.config)
  api(libs.hypertrace.grpcutils.context)

  implementation(projects.configProtoConverter)
  implementation(libs.hypertrace.eventstore)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  runtimeOnly(libs.kafka.protobuf.serializer)
  constraints {
    runtimeOnly("org.glassfish.jersey.core:jersey-common:2.34") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
    }
    runtimeOnly("org.apache.commons:commons-compress:1.21") {
      because("Multiple vulnerabilities")
    }
  }

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testRuntimeOnly(libs.slf4j.log4jimpl)
}

tasks.test {
  useJUnitPlatform()
}
