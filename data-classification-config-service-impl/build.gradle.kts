plugins {
  `java-library`
  alias(commonLibs.plugins.google.protobuf)
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${commonLibs.versions.protoc.get()}"
  }
}

dependencies {
  api(projects.dataClassificationConfigServiceApi)
  api(projects.featureCachingClient)

  implementation(projects.configUtils)
  implementation(projects.sensitiveDataConfigServiceApi)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(commonLibs.hypertrace.framework.metrics.jakarta)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.guice7)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
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
