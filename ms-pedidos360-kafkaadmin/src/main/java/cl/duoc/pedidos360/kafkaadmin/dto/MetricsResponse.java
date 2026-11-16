package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;

/**
 * Vista rapida del caso: cuanto hay en cada topico y como va cada grupo.
 *
 * @param brokers brokers que respondieron (3 en el cluster del caso)
 */
public record MetricsResponse(int brokers, List<Topic> topics, List<Group> groups) {

    public record Topic(String name, long messages, int underReplicated) {
    }

    /**
     * @param totalLag mensajes pendientes; si crece y no baja, el consumidor no
     *                 da abasto o esta detenido
     */
    public record Group(String groupId, String state, int members, long totalLag) {
    }
}
