package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Cuerpo de PATCH /api/kafka/topics/{name}/configs, como en la guia 3.4.1:
 * una propiedad y su valor nuevo, por ejemplo retention.ms = 172800000.
 */
public record TopicConfigRequest(
        @NotBlank(message = "es obligatorio") String configName,
        @NotBlank(message = "es obligatorio") String value
) {
}
