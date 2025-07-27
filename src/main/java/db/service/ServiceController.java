package db.service;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/server")
public class ServiceController {
    @GetMapping("/ping")
    public Mono<String> ping() {
        return Mono.just("pong");
    }
}
