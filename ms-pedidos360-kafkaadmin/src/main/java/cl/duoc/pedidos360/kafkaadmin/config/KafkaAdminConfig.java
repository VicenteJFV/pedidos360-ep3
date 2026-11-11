package cl.duoc.pedidos360.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * El AdminClient de Kafka, construido con la misma configuracion que el
 * KafkaAdmin que autoconfigura Spring Boot (spring.kafka.bootstrap-servers).
 *
 * Se usa el AdminClient directamente porque KafkaAdmin solo crea topicos al
 * arrancar; consultar particiones, cambiar configuraciones o revisar consumer
 * groups requiere la API completa. Spring lo cierra al detener la aplicacion.
 */
@Configuration
public class KafkaAdminConfig {

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}
