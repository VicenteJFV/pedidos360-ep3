package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;

/**
 * Estado de un consumer group: lo que pide la seccion 6.2 de la guia de EP3
 * (offsets, consumer lag, distribucion del consumo y estado del grupo).
 *
 * @param totalLag mensajes publicados que el grupo todavia no procesa
 * @param members  quien consume y que particiones tiene asignadas
 * @param offsets  avance del grupo en cada particion
 */
public record ConsumerGroupDetailResponse(
        String groupId,
        String state,
        Integer coordinator,
        long totalLag,
        List<Member> members,
        List<PartitionOffset> offsets) {

    public record Member(String memberId, String clientId, String host, List<String> partitions) {
    }

    /**
     * @param committedOffset ultimo offset confirmado por el grupo (el proximo a leer)
     * @param endOffset       offset que recibira el proximo registro publicado
     * @param lag             endOffset - committedOffset
     */
    public record PartitionOffset(String topic, int partition, long committedOffset, long endOffset, long lag) {
    }
}
