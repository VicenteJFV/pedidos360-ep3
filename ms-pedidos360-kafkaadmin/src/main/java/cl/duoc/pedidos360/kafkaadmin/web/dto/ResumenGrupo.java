package cl.duoc.pedidos360.kafkaadmin.web.dto;

/** Una fila del listado de consumer groups. */
public record ResumenGrupo(String grupo, String estado, int miembros) {
}
