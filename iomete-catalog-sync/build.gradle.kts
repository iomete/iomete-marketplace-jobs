import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("java")
    id("io.quarkus")

    kotlin("jvm") version "2.2.21"
    kotlin("plugin.allopen") version "2.2.21"
    kotlin("plugin.jpa") version "2.2.21"
}

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
    }

    group = "com.iomete"
    version = "1.0.0"

    apply(plugin = "org.jetbrains.kotlin.jvm")

    tasks.withType<KotlinCompile> {
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_17)
            javaParameters.set(true)
        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    val quarkusPlatformGroupId: String by project
    val quarkusPlatformArtifactId: String by project
    val quarkusPlatformVersion: String by project
    dependencies {
        implementation(kotlin("stdlib-jdk8"))
        implementation(enforcedPlatform("$quarkusPlatformGroupId:$quarkusPlatformArtifactId:$quarkusPlatformVersion"))

        implementation("io.quarkus:quarkus-logging-json")

        implementation("io.quarkus:quarkus-resteasy-jackson")

        implementation("io.quarkus:quarkus-config-yaml")
        implementation("io.quarkus:quarkus-kotlin")
        implementation("io.quarkus:quarkus-rest-client")
        implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
        implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.22.2")

        // Apache Spark
        compileOnly("org.apache.spark:spark-sql_2.12:3.5.9")
        compileOnly("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0")

        // Test dependencies
        testImplementation("io.quarkus:quarkus-junit5")
        testImplementation("io.mockk:mockk:1.14.11")
        testImplementation("org.apache.spark:spark-sql_2.12:3.5.9")
        testImplementation("org.apache.spark:spark-core_2.12:3.5.9")
        testImplementation("org.scala-lang:scala-library:2.12.21")
        testRuntimeOnly("org.scala-lang:scala-library:2.12.21")
        testImplementation("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0")

        // Force specific versions for transitive dependencies
        implementation("org.apache.parquet:parquet-avro:1.18.0")
        implementation("org.apache.avro:avro:1.12.2")
        implementation("com.mysql:mysql-connector-j:8.4.0")
        implementation("com.google.protobuf:protobuf-java:4.35.1")
        implementation("org.apache.thrift:libthrift:0.24.0")
        implementation("io.quarkus:quarkus-core:3.38.2")
    }
}

allOpen {
    annotation("javax.ws.rs.Path")
    annotation("javax.enterprise.context.ApplicationScoped")
    annotation("io.quarkus.test.junit.QuarkusTest")
}

configurations.compileClasspath {
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.21")
        force("org.scala-lang:scala-reflect:2.12.21")
    }
}

configurations.testCompileClasspath {
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.21")
        force("org.scala-lang:scala-reflect:2.12.21")
    }
}

configurations.testRuntimeClasspath {
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.21")
        force("org.scala-lang:scala-reflect:2.12.21")
        force("org.antlr:antlr4-runtime:4.13.2")
    }
}

tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests"
    group = "verification"
    useJUnitPlatform {
        includeTags("integration")
    }
    jvmArgs(
        "-Xmx2g",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    )
}
