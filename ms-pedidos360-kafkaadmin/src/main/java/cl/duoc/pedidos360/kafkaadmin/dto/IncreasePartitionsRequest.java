package cl.duoc.pedidos360.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Cuerpo de PATCH /api/kafka/topics/{name}/partitions (guia 3.4.1).
 * totalPartitions es el total final, no cuantas se agregan.
 */
public record IncreasePartitionsRequest(
        @NotNull @Min(1) @Max(50) Integer totalPartitions
) {
}
