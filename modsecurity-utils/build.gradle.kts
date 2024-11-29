import com.google.protobuf.gradle.id

plugins {
  `java-library`
  alias(commonLibs.plugins.google.protobuf)
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.traceable.publish)
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${commonLibs.versions.protoc.get()}"
  }
  plugins {
    id("grpc") {
      artifact = "io.grpc:protoc-gen-grpc-java:${commonLibs.versions.grpc.get()}"
    }
  }
  generateProtoTasks {
    ofSourceSet("main").forEach { task ->
      task.plugins {
        id("grpc")
      }
    }
  }
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc")
    }
  }
}

dependencies {
  implementation(projects.configUtils)

  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.re2j)
  implementation(commonLibs.commons.net)
  implementation(commonLibs.commons.validator)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.guice)
  implementation(commonLibs.json.path)
  testImplementation(commonLibs.commons.lang)
  implementation(commonLibs.traceable.modsecurity.jni)
  implementation(commonLibs.grpc.netty)
  implementation(commonLibs.traceable.coraza.wafServiceClient)

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
