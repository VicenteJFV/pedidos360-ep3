package cl.duoc.pedidos360.kafkaadmin.web.dto;

import java.util.List;

/**
 * Detalle de un topico: como estan repartidas sus particiones entre los brokers.
 *
 * @param mensajes            registros retenidos en total (suma de offsetFinal - offsetInicial)
 * @param particionesSinIsr   particiones donde alguna replica se atraso o su
 *                            broker esta caido; 0 es lo sano
 */
public record InfoTopico(
        String nombre,
        int particiones,
        int factorReplicacion,
        long mensajes,
        int particionesSinIsr,
        List<InfoParticion> detalle) {

    /**
     * @param lider         broker que atiende lecturas y escrituras de la particion
     * @param replicas      brokers que tienen una copia
     * @param isr           replicas sincronizadas con el lider (In-Sync Replicas)
     * @param offsetInicial primer offset todavia retenido
     * @param offsetFinal   offset que recibira el proximo registro
     */
    public record InfoParticion(
            int particion,
            Integer lider,
            List<Integer> replicas,
            List<Integer> isr,
            long offsetInicial,
            long offsetFinal) {
    }
}
