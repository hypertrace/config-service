plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(commonLibs.guice7)
  api(commonLibs.typesafe.config)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.traceable.featureflag.api)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.guava)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
