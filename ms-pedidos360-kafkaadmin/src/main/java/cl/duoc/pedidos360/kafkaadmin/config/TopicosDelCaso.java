package cl.duoc.pedidos360.kafkaadmin.config;

import java.util.List;
import java.util.Set;

/**
 * Nombres de Kafka del Caso 0 - Pedidos360.
 *
 * Como NombresDelCaso en rabbitadmin, estan solo como nombres: este
 * microservicio administra los topicos pero no los crea al arrancar (eso lo
 * hace kafka-cluster/crear-topicos.sh). Los necesita para dos cosas:
 *
 *  - Proteger los topicos y grupos funcionales: la guia pide demostrar la
 *    eliminacion con un topico temporal "sin afectar los topicos funcionales".
 *    Eliminar pedidos-eventos por error desde la API borraria la historia de
 *    todos los pedidos.
 *  - Armar las metricas del caso sin tener que listar todo el cluster.
 */
public final class TopicosDelCaso {

    public static final String EVENTOS = "pedidos-eventos";
    public static final String AUDITORIA = "pedidos-auditoria";
    public static final String EVENTOS_DLT = "pedidos-eventos-dlt";
    public static final String AUDITORIA_DLT = "pedidos-auditoria-dlt";

    public static final String GRUPO_REPORTES = "reportes-pedidos";
    public static final String GRUPO_AUDITORIA = "reportes-auditoria";

    /** Los dos topicos de la seccion 4 de la guia, en el orden del flujo. */
    public static final List<String> FUNCIONALES = List.of(EVENTOS, AUDITORIA);

    /** Topicos que la API no permite eliminar. */
    public static final Set<String> PROTEGIDOS = Set.of(EVENTOS, AUDITORIA, EVENTOS_DLT, AUDITORIA_DLT);

    /** Consumer groups de ms-pedidos360-reportes, que la API no permite eliminar. */
    public static final Set<String> GRUPOS_PROTEGIDOS = Set.of(GRUPO_REPORTES, GRUPO_AUDITORIA);

    /** Topicos internos de Kafka, como __consumer_offsets. */
    public static boolean esInterno(String topico) {
        return topico.startsWith("__") || topico.startsWith("_confluent") || topico.startsWith("_schemas");
    }

    private TopicosDelCaso() {
    }
}
