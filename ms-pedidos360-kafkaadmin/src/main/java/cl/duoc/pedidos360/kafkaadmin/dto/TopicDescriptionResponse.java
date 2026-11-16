package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;

/**
 * Respuesta de GET /api/kafka/topics/{name}: los campos de la guia 3.4.1
 * (name, internal y por particion partition, leader, replicas, isr), mas lo que
 * pide la seccion 6.2 de la guia de EP3 para supervisar el topico: los offsets
 * de cada particion y cuantas particiones tienen el ISR incompleto.
 *
 * @param messages           registros retenidos (suma de endOffset - beginningOffset)
 * @param underReplicated    particiones con alguna replica atrasada o caida; 0 es lo sano
 */
public record TopicDescriptionResponse(
        String name,
        boolean internal,
        int replicationFactor,
        long messages,
        int underReplicated,
        List<Partition> partitions) {

    /**
     * @param leader          broker que atiende lecturas y escrituras de la particion
     * @param replicas        brokers que tienen una copia
     * @param isr             replicas sincronizadas con el lider (In-Sync Replicas)
     * @param beginningOffset primer offset todavia retenido
     * @param endOffset       offset que recibira el proximo registro
     */
    public record Partition(int partition, Integer leader, List<Integer> replicas, List<Integer> isr,
                            long beginningOffset, long endOffset) {
    }
}
