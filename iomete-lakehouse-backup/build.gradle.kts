import org.gradle.api.plugins.jvm.JvmTestSuite
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.4.10"
    application
}

group = "com.iomete"
version = property("projectVersion") as String

val sparkVersion = property("sparkVersion") as String
val hadoopAwsVersion = property("hadoopAwsVersion") as String
val icebergVersion = property("icebergVersion") as String

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

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter("6.1.2")
        }

        // Own source set + classpath: keeps the unshaded Hadoop (see below) off the unit-test classpath.
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter("6.1.2")
            dependencies {
                implementation(project())

                // Spark's shaded hadoop-client (3.3.4) relocates protobuf and breaks MiniDFSCluster;
                // exclude it and pull unshaded Hadoop 3.4.1 (matching the hadoop-aws pin) instead.
                implementation("org.apache.spark:spark-sql_2.12:$sparkVersion") {
                    exclude(group = "org.apache.hadoop", module = "hadoop-client-api")
                    exclude(group = "org.apache.hadoop", module = "hadoop-client-runtime")
                }
                implementation("org.apache.hadoop:hadoop-client:$hadoopAwsVersion")
                implementation("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")
                implementation("org.apache.hadoop:hadoop-minicluster:$hadoopAwsVersion")
                implementation("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$icebergVersion")
                runtimeOnly("org.mockito:mockito-core:5.23.0") // MiniDFSCluster uses Mockito internally
                implementation("org.testcontainers:junit-jupiter:1.21.4")
                implementation("org.testcontainers:minio:1.21.4")
                implementation("software.amazon.awssdk:s3:2.49.6")
                implementation("org.jetbrains.kotlin:kotlin-test")
            }
            targets {
                all {
                    testTask.configure {
                        jvmArgs(sparkJvmArgs)
                        shouldRunAfter(test)
                        testLogging {
                            events("passed", "skipped", "failed")
                        }
                    }
                }
            }
        }
    }
}

dependencies {
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.22.2")

    implementation("org.slf4j:slf4j-api:2.0.18")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.26.1")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.26.1")

    // compileOnly: the Spark base image provides these at runtime.
    compileOnly("org.apache.spark:spark-sql_2.12:$sparkVersion")
    compileOnly("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")

    testImplementation("io.mockk:mockk:1.14.11")
    testImplementation("org.apache.spark:spark-sql_2.12:$sparkVersion")
    // Pin the shaded Hadoop client to the base image's version (overrides Spark's transitive 3.3.4).
    testImplementation("org.apache.hadoop:hadoop-client-api:$hadoopAwsVersion")
    testImplementation("org.apache.hadoop:hadoop-client-runtime:$hadoopAwsVersion")
    testImplementation(kotlin("test"))
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
