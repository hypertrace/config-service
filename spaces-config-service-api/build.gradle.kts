import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.ofSourceSet
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.15"
  id("org.hypertrace.publish-plugin")
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.15.7"
  }
  plugins {
    id("grpc") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.36.1"
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

dependencies {
  api("io.grpc:grpc-protobuf:1.36.1")
  api("io.grpc:grpc-stub:1.36.1")
  api("javax.annotation:javax.annotation-api:1.3.2")
  constraints {
    implementation("com.google.guava:guava:30.1-jre") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415")
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
