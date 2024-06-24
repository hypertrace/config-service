plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  api(projects.traceableSpanProcessingConfigServiceApi)

  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.re2j)
  implementation(commonLibs.commons.net)
  implementation(commonLibs.commons.validator)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.commons.csv)
  implementation(commonLibs.guava)
  implementation(commonLibs.json.path)
  implementation(localLibs.automaton)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)

  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
