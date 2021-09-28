plugins {
    `java-library`
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
    api(projects.alertingConfigServiceApi)
    implementation(libs.hypertrace.configservice.objectstore)
    implementation(libs.hypertrace.configservice.validation)
    implementation(libs.hypertrace.configservice.api)
    implementation(projects.configUtils)
    implementation(libs.slf4j.api)
    implementation(libs.hypertrace.grpcutils.context)
    implementation(libs.hypertrace.grpcutils.client)
    implementation(libs.hypertrace.configservice.protoconverter)

    annotationProcessor(libs.lombok)
    compileOnly(libs.lombok)

    testImplementation(libs.junit.jupiter)
    testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
    useJUnitPlatform()
}
