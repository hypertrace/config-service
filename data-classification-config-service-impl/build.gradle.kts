
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    `java-library`
    id("com.google.protobuf") version "0.8.17"
    jacoco
    id("org.hypertrace.jacoco-report-plugin")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protoc.get()}"
    }
}

dependencies {
    api(projects.dataClassificationConfigServiceApi)
    api(projects.featureCachingClient)

    implementation(projects.sensitiveDataConfigServiceApi)
    implementation(libs.hypertrace.configservice.objectstore)
    implementation(libs.hypertrace.configservice.api)
    implementation(libs.hypertrace.configservice.changeeventgenerator)
    implementation(libs.hypertrace.configservice.protoconverter)
    implementation(libs.hypertrace.configservice.validation)
    implementation(libs.hypertrace.grpcutils.client)
    implementation(libs.guice)
    implementation(libs.protobuf.javautil)

    annotationProcessor(libs.lombok)
    compileOnly(libs.lombok)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

sourceSets {
    main {
        java {
            srcDirs("build/generated/source/proto/main/java")
        }
    }
}

tasks.test {
    useJUnitPlatform()
}
