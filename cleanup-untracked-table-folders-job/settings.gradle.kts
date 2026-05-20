pluginManagement {
    repositories {
        mavenLocal()
        mavenCentral()
        gradlePluginPortal()
    }
    val quarkusPluginVersion: String by settings
    plugins {
        id("io.quarkus") version quarkusPluginVersion
    }
}

rootProject.name = "cleanup-untracked-table-folders-job"
