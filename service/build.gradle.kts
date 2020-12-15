plugins {
  java
  application
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.docker-java-application-plugin") version "0.2.3"
  id("org.hypertrace.docker-publish-plugin") version "0.2.3"
  id("ai.traceable.docker-convention-plugin") version "1.2.1"
  id("org.hypertrace.integration-test-plugin") version "0.1.1"
}

dependencies {
  implementation(project(":service-impl"))

  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}

application {
  mainClassName = "ai.traceable.example.app.MyApplication"
}

tasks.integrationTest {
  useJUnitPlatform()
}

// Customize integration test report to include source of service-impl
tasks.jacocoIntegrationTestReport {
  sourceSets(project(":service-impl").sourceSets.getByName("main"))
}
