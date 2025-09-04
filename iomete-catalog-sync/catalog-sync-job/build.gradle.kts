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
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.20.0")

    //Apache Spark
    compileOnly("org.apache.spark:spark-sql_2.12:3.5.3")

    // Test dependencies
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.mockk:mockk:1.13.12")
    testImplementation("org.apache.spark:spark-sql_2.12:3.5.3")
    testImplementation("org.apache.spark:spark-core_2.12:3.5.3")
    testImplementation("org.scala-lang:scala-library:2.12.10")
    testRuntimeOnly("org.scala-lang:scala-library:2.12.10")

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
}

configurations.compileClasspath {
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.10")
    }
}

configurations.testCompileClasspath {
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.10")
    }
}

configurations.testRuntimeClasspath {
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.10")
    }
}
