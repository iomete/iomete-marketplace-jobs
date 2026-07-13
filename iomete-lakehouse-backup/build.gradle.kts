import org.gradle.api.tasks.testing.Test
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.0.21"
    application
}

group = "com.iomete"
version = "1.0.0"

val sparkVersion = property("sparkVersion") as String
val hadoopAwsVersion = property("hadoopAwsVersion") as String

repositories {
    mavenCentral()
}

dependencies {
    // Kotlin
    implementation(kotlin("stdlib-jdk8"))

    // JSON parsing
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.12")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")

    // Provided at runtime by the Spark base image
    compileOnly("org.apache.spark:spark-sql_2.12:$sparkVersion")
    compileOnly("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("io.mockk:mockk:1.13.12")
    testImplementation("org.apache.spark:spark-sql_2.12:$sparkVersion")
    testImplementation("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testImplementation("org.testcontainers:minio:1.21.4")
    testImplementation("software.amazon.awssdk:s3:2.42.18")
    testImplementation(kotlin("test"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.iomete.backup.App")
    applicationDefaultJvmArgs = listOf(
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
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    )
}

tasks.withType<KotlinCompile> {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.test {
    useJUnitPlatform()
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
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
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