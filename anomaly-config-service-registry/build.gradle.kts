plugins {
  `java-library`
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  api(projects.anomalyConfigServiceApi)
  api(commonLibs.javax.annotation)
  api(commonLibs.typesafe.config)

  implementation(projects.modsecurityUtils)
  implementation(commonLibs.guice7)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.re2j)
  implementation(commonLibs.jackson.yaml)
  implementation(commonLibs.slf4j2.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(commonLibs.commons.lang)
}

tasks.test {
  useJUnitPlatform()
}
