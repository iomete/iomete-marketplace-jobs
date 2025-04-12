plugins {
    id("java")

    kotlin("jvm")  version "2.1.20"
    kotlin("plugin.allopen") version "2.1.20"
    kotlin("plugin.jpa") version "2.1.20"
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

