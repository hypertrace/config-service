plugins {
  `java-library`
  id("ai.traceable.publish-plugin")
}

dependencies {
  api(project(":anomaly-config-service-api"))

  api("javax.annotation:javax.annotation-api:1.3.2")
  api("com.typesafe:config:1.4.1")
  implementation("com.google.inject:guice:5.0.1")
  implementation("com.google.protobuf:protobuf-java-util:3.15.7")
  implementation("com.google.re2j:re2j:1.6")
  implementation("org.slf4j:slf4j-api:1.7.30")

  annotationProcessor("org.projectlombok:lombok:1.18.20")
  compileOnly("org.projectlombok:lombok:1.18.20")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.apache.commons:commons-lang3:3.11")
  testImplementation("ai.traceable.platform:jni-modsecurity:0.1.83")
}

tasks.test {
  useJUnitPlatform()
}
