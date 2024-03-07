plugins {
    id("org.jetbrains.kotlin.plugin.allopen")
    id("io.quarkus")
}

dependencies {
    implementation("io.quarkus:quarkus-logging-json")

    implementation("io.quarkus:quarkus-resteasy-jackson")

    implementation("io.quarkus:quarkus-config-yaml")
    implementation("io.quarkus:quarkus-kotlin")
    implementation("io.quarkus:quarkus-rest-client")
    implementation("io.quarkus:quarkus-micrometer-registry-prometheus")

    //Apache Spark
    compileOnly("org.apache.spark:spark-sql_2.12:3.5.1")

    // runtimeOnly("org.apache.logging.log4j:log4j-jul:2.20.0")
    // implementation("org.apache.logging.log4j:log4j-core:2.20.0")
    // implementation("org.apache.logging.log4j:log4j-api:2.20.0")

    // implementation("org.jboss.resteasy:resteasy-client")
    // implementation("org.jboss.resteasy:resteasy-jackson2-provider")
}

quarkus {
    // setOutputDirectory("$projectDir/build/classes/kotlin/main")
}

allOpen {
    annotation("javax.ws.rs.Path")
    annotation("javax.enterprise.context.ApplicationScoped")
    annotation("io.quarkus.test.junit.QuarkusTest")
}

tasks {
    quarkusDev {
        // setSourceDir("$projectDir/src/main/kotlin")
    }

    test {
        systemProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager")
    }
}

configurations.compileClasspath {
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.10")
    }
}
