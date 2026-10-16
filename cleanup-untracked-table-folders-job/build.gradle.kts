import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("java")
    id("io.quarkus")

    kotlin("jvm") version "2.4.21"
    kotlin("plugin.allopen") version "2.4.21"
    kotlin("plugin.jpa") version "2.4.21"
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

        implementation("io.quarkus:quarkus-arc")
        implementation("io.quarkus:quarkus-kotlin")
        implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

        // Apache Spark
        compileOnly("org.apache.spark:spark-sql_2.12:3.5.9")

        // Test dependencies
        testImplementation("io.quarkus:quarkus-junit5")
        testImplementation("io.mockk:mockk:1.14.11")
        testImplementation("org.apache.spark:spark-sql_2.12:3.5.9")
        testImplementation("org.apache.spark:spark-core_2.12:3.5.9")
        testImplementation("org.scala-lang:scala-library:2.12.21")
        testRuntimeOnly("org.scala-lang:scala-library:2.12.21")
    }

    // Spark supplies the SLF4J provider; a second one in this jar makes the provider depend on classpath order.
    configurations.all {
        exclude(group = "org.jboss.slf4j", module = "slf4j-jboss-logmanager")
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
    }
}

configurations.testCompileClasspath {
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.21")
    }
}

configurations.testRuntimeClasspath {
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.21")
    }
}
