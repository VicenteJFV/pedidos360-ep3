package cl.duoc.pedidos360.kafkaadmin.web.dto;

/**
 * Cuerpo de un error. Nombra el parametro que lo provoco, y no solo el
 * mensaje, para que quien llama sepa que corregir (igual que en rabbitadmin).
 *
 * @param error     descripcion legible del problema
 * @param parametro parametro que lo provoco, o null si no aplica
 */
public record RespuestaError(String error, String parametro) {
}
