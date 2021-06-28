plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  implementation("com.typesafe:config:1.4.1")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("ai.traceable.activity.event.service:activity-event-api:0.3.1")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.5.0")
  implementation("org.hypertrace.core.eventstore:event-store:0.1.1")

  annotationProcessor("org.projectlombok:lombok:1.18.20")
  compileOnly("org.projectlombok:lombok:1.18.20")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.9.0")
}

tasks.test {
  useJUnitPlatform()
}
