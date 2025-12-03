import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("java")

    kotlin("jvm")  version "2.2.21"
    kotlin("plugin.allopen") version "2.2.10"
    kotlin("plugin.jpa") version "2.2.10"
}

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
    }

    group="com.iomete"
    version="2.1.0"

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
        implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    }
}

