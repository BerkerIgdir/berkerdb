import org.jetbrains.kotlin.cli.jvm.compiler.findMainClass

plugins {
    id("java")
    kotlin("jvm") version "2.0.0-Beta3"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation(kotlin("stdlib-jdk8"))
}
java {
}

tasks.compileJava {
    options.compilerArgs.add(("--enable-preview"))
}

tasks.compileTestJava {
    options.compilerArgs.add(("--enable-preview"))
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-preview")
    forkEvery = 1
    maxHeapSize = "8192m"
}
kotlin {
    jvmToolchain(22)
}