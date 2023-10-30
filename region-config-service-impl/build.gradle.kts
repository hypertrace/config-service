plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.activityEventProducer)
  api(commonLibs.grpc.api)
  api(commonLibs.typesafe.config)
  implementation(projects.regionConfigServiceApi)
  implementation(projects.featureCachingClient)
  implementation(projects.configUtils)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(commonLibs.guice)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.commons.csv)
  implementation(commonLibs.uuidcreator)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.traceable.activityevent.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
  testRuntimeOnly(commonLibs.log4j.slf4j2.impl)
}

tasks.test {
  useJUnitPlatform()
}
