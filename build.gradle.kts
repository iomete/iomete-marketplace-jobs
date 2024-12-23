plugins {
    id("java")

    kotlin("jvm")  version "1.9.22"
    kotlin("plugin.allopen") version "1.9.22"
    kotlin("plugin.jpa") version "1.9.22"
}

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
    }

    group="com.iomete"
    version="2.1.0"

    apply(plugin = "org.jetbrains.kotlin.jvm")

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        kotlinOptions.jvmTarget = "11"
        kotlinOptions.javaParameters = true
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    val quarkusPlatformGroupId: String by project
    val quarkusPlatformArtifactId: String by project
    val quarkusPlatformVersion: String by project
    dependencies {
        implementation(kotlin("stdlib-jdk8"))
        implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    }
}

