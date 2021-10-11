plugins {
    `java-library`
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
    api(projects.labelApplicationRuleConfigServiceApi)
    implementation(projects.configServiceApi)
    implementation(projects.objectStore)
    implementation(projects.validationUtils)
    implementation(projects.configProtoConverter)
    implementation(projects.configServiceChangeEventGenerator)
    implementation(libs.typesafe.config)
    implementation(libs.protobuf.javautil)
    implementation(libs.hypertrace.grpcutils.context)
    implementation(libs.hypertrace.grpcutils.client)

    annotationProcessor(libs.lombok)
    compileOnly(libs.lombok)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(testFixtures(projects.configServiceApi))
}

tasks.test {
    useJUnitPlatform()
}
