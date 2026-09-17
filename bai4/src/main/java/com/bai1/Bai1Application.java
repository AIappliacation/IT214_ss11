package com.bai1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@EnableFeignClients
@SpringBootApplication
public class Bai1Application {

    public static void main(String[] args) {
        SpringApplication.run(Bai1Application.class, args);
    }

}
