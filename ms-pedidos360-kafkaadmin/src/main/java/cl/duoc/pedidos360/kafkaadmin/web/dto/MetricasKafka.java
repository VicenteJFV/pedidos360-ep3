package cl.duoc.pedidos360.kafkaadmin.web.dto;

import java.util.List;

/**
 * Vista rapida del caso: cuanto hay en cada topico y como va cada grupo.
 *
 * @param brokersActivos   brokers que respondieron (3 en el cluster del caso)
 */
public record MetricasKafka(int brokersActivos, List<Topico> topicos, List<Grupo> grupos) {

    /**
     * @param mensajes          registros retenidos
     * @param particionesSinIsr particiones con alguna replica atrasada o caida
     */
    public record Topico(String nombre, long mensajes, int particionesSinIsr) {
    }

    /**
     * @param lagTotal mensajes pendientes del grupo; si crece y no baja, el
     *                 consumidor no da abasto o esta detenido
     */
    public record Grupo(String grupo, String estado, int miembros, long lagTotal) {
    }
}
