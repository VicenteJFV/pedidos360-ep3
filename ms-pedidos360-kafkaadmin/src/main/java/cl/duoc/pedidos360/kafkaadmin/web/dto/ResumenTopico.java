package cl.duoc.pedidos360.kafkaadmin.web.dto;

/** Una fila del listado de topicos. */
public record ResumenTopico(String nombre, int particiones, int factorReplicacion) {
}
