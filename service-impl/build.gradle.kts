plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":service-api"))
  implementation("com.google.guava:guava:29.0-jre")
  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}

tasks.test {
  useJUnitPlatform()
}