plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  implementation(libs.commons.validator)
  implementation(libs.slf4j.api)
  implementation(libs.seancfoley.ipaddress)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter)

  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
