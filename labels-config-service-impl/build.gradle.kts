plugins {
    `java-library`
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
    api(projects.labelsConfigServiceApi)
    implementation(projects.objectStore)
    implementation(projects.configProtoConverter)
    implementation(projects.configServiceChangeEventGenerator)
    implementation(libs.typesafe.config)
    implementation(libs.protobuf.javautil)
    implementation(libs.slf4j.api)
    implementation(libs.guava)
    implementation(libs.rholder.guava.retrying)
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
