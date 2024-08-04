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

rootProject.name = "iomete-catalog-sync"
include(
    "catalog-sync-job",
    "tpcds-query-executor",
)
