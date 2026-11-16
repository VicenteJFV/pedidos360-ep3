package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.Map;

/**
 * Cuerpo de POST /api/kafka/topics, como el CreateTopicRequest de la guia 3.4.1.
 *
 * Ademas de las validaciones de la guia, limita lo que el cluster del caso
 * puede cumplir: un nombre con los caracteres que Kafka acepta (sin el prefijo
 * __ de los topicos internos), hasta 50 particiones y un factor de replicacion
 * de hasta 3, que son los brokers del cluster.
 */
public record CreateTopicRequest(
        // El vacio lo informa @NotBlank ("es obligatorio"); el patron solo revisa
        // los nombres que si traen algo.
        @NotBlank(message = "es obligatorio")
        @Pattern(regexp = "^$|^(?!__)[a-zA-Z0-9._-]{1,249}$",
                message = "solo admite letras, numeros, punto, guion y guion bajo, y no puede empezar con __")
        String name,
        @NotNull(message = "es obligatorio") @Min(1) @Max(50) Integer partitions,
        @NotNull(message = "es obligatorio") @Min(1) @Max(value = 3, message = "no puede superar los 3 brokers del cluster") Short replicationFactor,
        Map<String, String> configs
) {
}
