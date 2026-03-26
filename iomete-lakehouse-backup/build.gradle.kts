import org.gradle.api.tasks.testing.Test
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.0.21"
    application
}

group = "com.iomete"
version = "1.0.0"

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

    // Apache Spark (provided at runtime by Spark cluster)
    compileOnly("org.apache.spark:spark-sql_2.12:3.5.3")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("io.mockk:mockk:1.13.12")
    testImplementation("org.apache.spark:spark-sql_2.12:3.5.3")
    testImplementation("org.apache.hadoop:hadoop-aws:3.3.4")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testImplementation("org.testcontainers:minio:1.21.4")
    testImplementation("software.amazon.awssdk:s3:2.42.18")
    testImplementation(kotlin("test"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.iomete.backup.App")
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
