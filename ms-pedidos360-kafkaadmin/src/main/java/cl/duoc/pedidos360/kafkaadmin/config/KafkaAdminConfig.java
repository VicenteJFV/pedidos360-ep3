package cl.duoc.pedidos360.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * El AdminClient de Kafka, como el KafkaAdminConfig de la guia practica 3.4.1.
 *
 * AdminClient consulta metadatos y ejecuta operaciones administrativas: no
 * reemplaza a KafkaTemplate (productor) ni a @KafkaListener (consumidor).
 * Spring lo cierra al detener la aplicacion (destroyMethod).
 */
@Configuration
public class KafkaAdminConfig {

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(@Value("${kafka.admin.bootstrap-servers}") String servers) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "ms-pedidos360-kafkaadmin");
        return AdminClient.create(config);
    }
}
