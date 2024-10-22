plugins {
  `java-library`
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  api(projects.anomalyConfigServiceApi)
  api(commonLibs.javax.annotation)
  api(commonLibs.typesafe.config)

  implementation(projects.anomalyConfigServiceUtils)
  implementation(commonLibs.guice)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.re2j)
  implementation(commonLibs.jackson.yaml)
  implementation(commonLibs.slf4j2.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.commons.lang)
}

tasks.test {
  useJUnitPlatform()
}
