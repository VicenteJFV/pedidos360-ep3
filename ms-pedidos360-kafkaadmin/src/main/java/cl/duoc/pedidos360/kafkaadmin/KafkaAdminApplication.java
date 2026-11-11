package cl.duoc.pedidos360.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio administrador Kafka de Pedidos360 (seccion 6 de la guia de EP3).
 *
 * Es el equivalente de ms-pedidos360-rabbitadmin para Kafka: administra
 * topicos, configuraciones y consumer groups, separado de la logica de negocio.
 */
@SpringBootApplication
public class KafkaAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }
}
