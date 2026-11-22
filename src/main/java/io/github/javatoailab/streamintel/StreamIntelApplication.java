package io.github.javatoailab.streamintel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class StreamIntelApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamIntelApplication.class, args);
    }
}
