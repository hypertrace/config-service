plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)
  implementation(libs.traceable.activityevent.api)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.eventstore)

  constraints {
    implementation("org.apache.commons:commons-compress:1.21") {
      because("Multiple Vulnerabilities [https://nvd.nist.gov/vuln/detail/CVE-2021-35515] [https://nvd.nist.gov/vuln/detail/CVE-2021-35516] [https://nvd.nist.gov/vuln/detail/CVE-2021-35517] [https://nvd.nist.gov/vuln/detail/CVE-2021-36090] in org.apache.commons:commons-compress@1.20")
    }
  }

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
}

tasks.test {
  useJUnitPlatform()
}
