plugins {
    `java-library`
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
    api(project(":activity-event-producer"))
    api("com.typesafe:config:1.4.1")
    api("io.grpc:grpc-api:1.37.0")
    implementation(project(":iprange-config-service-api"))
    implementation("org.hypertrace.config.service:config-service-api")
    implementation("com.google.inject:guice:5.0.1")
    implementation("com.google.protobuf:protobuf-java-util:3.15.8")
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("com.github.f4b6a3:uuid-creator:2.7.11")
    implementation("commons-net:commons-net:3.8.0")
    implementation("commons-validator:commons-validator:1.7")

    implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.4.0")
    implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.4.0")
    implementation("org.hypertrace.config.service:config-proto-converter")
    implementation("ai.traceable.activity.event.service:activity-event-api:0.3.1")
    annotationProcessor("org.projectlombok:lombok:1.18.20")
    compileOnly("org.projectlombok:lombok:1.18.20")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testImplementation("org.mockito:mockito-core:3.9.0")
    testImplementation(testFixtures("org.hypertrace.config.service:config-service-api"))
}

tasks.test {
    useJUnitPlatform()
}
