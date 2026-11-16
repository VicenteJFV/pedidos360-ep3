package cl.duoc.pedidos360.kafkaadmin.dto;

/**
 * Cuerpo de un error. Nombra el campo que lo provoco, y no solo el mensaje,
 * para que quien llama sepa que corregir (igual que en rabbitadmin).
 *
 * @param error     descripcion legible del problema
 * @param parametro campo o parametro que lo provoco, o null si no aplica
 */
public record ErrorResponse(String error, String parametro) {
}
