import org.gradle.api.tasks.testing.Test
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.4.0"
    application
}

group = "com.iomete"
version = property("projectVersion") as String

val sparkVersion = property("sparkVersion") as String
val hadoopAwsVersion = property("hadoopAwsVersion") as String

// Spark on JDK 17 needs these JVM module openings (driver + local executors in tests).
val sparkJvmArgs =
    listOf(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    )

repositories {
    mavenCentral()
}

dependencies {
    // JSON parsing
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.22.1")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.18")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.26.1")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.26.1")

    // Provided at runtime by the Spark base image
    compileOnly("org.apache.spark:spark-sql_2.12:$sparkVersion")
    compileOnly("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.2")
    testImplementation("io.mockk:mockk:1.14.11")
    testImplementation("org.apache.spark:spark-sql_2.12:$sparkVersion")
    testImplementation("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testImplementation("org.testcontainers:minio:1.21.4")
    testImplementation("software.amazon.awssdk:s3:2.47.5")
    testImplementation(kotlin("test"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.iomete.backup.App")
    applicationDefaultJvmArgs = sparkJvmArgs
}

tasks.withType<KotlinCompile> {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

tasks.test {
    jvmArgs(sparkJvmArgs)
    useJUnitPlatform {
        excludeTags("integration")
    }
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs Docker-backed integration tests."
    group = "verification"
    testClassesDirs =
        sourceSets.test
            .get()
            .output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    jvmArgs(sparkJvmArgs)
    shouldRunAfter(tasks.test)
    useJUnitPlatform {
        includeTags("integration")
    }
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.iomete.backup.App"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}
