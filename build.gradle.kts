plugins {
    // kotlin("jvm") version "1.9.20"
    application
}

group = "org.itmo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("Main")
}
