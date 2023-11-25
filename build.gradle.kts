plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.compileJava{
    options.compilerArgs.add(("--enable-preview"))
}
tasks.compileTestJava{
    options.compilerArgs.add(("--enable-preview"))
}
tasks.test {
    useJUnitPlatform()

//    jvmArgs?.forEach(System.out::println)
    jvmArgs?.add("--enable-preview")
}
