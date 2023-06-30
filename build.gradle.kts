import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.21"
    application
}

group = "dev.piotrp"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.1")

    implementation("org.apache.kafka:kafka-clients:3.5.0")
    implementation("org.slf4j:slf4j-simple:2.0.7")
    implementation("org.jetbrains.kotlinx:kotlinx-cli:0.3.5")
    implementation("com.sksamuel.hoplite:hoplite-yaml:2.7.4")
    implementation("com.sksamuel.hoplite:hoplite-core:2.7.4")
    implementation("org.postgresql:postgresql:42.6.0")
}

tasks.test {
    useJUnitPlatform()
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(11))
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        jvmTarget = "11"
    }
}

application {
    mainClass.set("MainKt")
}

tasks.jar {
    manifest.attributes["Main-Class"] = "MainKt"
    // for building a fat jar - include all dependencies
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    }) {
        // https://stackoverflow.com/questions/999489/invalid-signature-file-when-attempting-to-run-a-jar
        exclude("META-INF/*.SF")
        exclude("META-INF/*.DSA")
        exclude("META-INF/*.RSA")
    }
}