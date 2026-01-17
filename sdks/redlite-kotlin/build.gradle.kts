plugins {
    kotlin("jvm") version "1.9.20"
    `java-library`
    `maven-publish`
}

group = "com.redlite"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    // Server mode - Redis client
    implementation("redis.clients:jedis:5.0.2")

    // Kotlin stdlib
    implementation(kotlin("stdlib"))

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testImplementation("org.yaml:snakeyaml:2.2")
}

tasks.test {
    useJUnitPlatform()
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    jvmToolchain(17)
}

// Native library loading
tasks.processResources {
    from("native/target/release") {
        include("*.so", "*.dylib", "*.dll")
        into("native")
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Redlite Kotlin SDK")
                description.set("Redis API with SQLite durability - Kotlin SDK")
                url.set("https://github.com/redlite/redlite")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
            }
        }
    }
}
