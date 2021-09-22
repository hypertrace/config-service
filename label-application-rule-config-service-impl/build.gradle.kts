plugins {
    `java-library`
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
    api(projects.labelApplicationRuleConfigServiceApi)
    implementation(projects.configServiceApi)
    implementation(projects.configProtoConverter)
    implementation(libs.typesafe.config)
    implementation(libs.protobuf.javautil)
    implementation(libs.slf4j.api)
    implementation(libs.guava)
    implementation(libs.rxjava3)

    implementation(libs.hypertrace.grpcutils.context)
    implementation(libs.hypertrace.grpcutils.client)
    implementation(libs.hypertrace.grpcutils.rxserver)
    implementation(libs.hypertrace.grpcutils.rxclient)

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
