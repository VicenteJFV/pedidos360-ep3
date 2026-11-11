package cl.duoc.pedidos360.kafkaadmin.web.dto;

import java.util.List;

/**
 * Estado de un consumer group (seccion 6.2 de la guia).
 *
 * @param estado      STABLE (consumiendo), EMPTY (sin miembros: nadie consume),
 *                    PREPARING_REBALANCE / COMPLETING_REBALANCE (reasignando
 *                    particiones) o DEAD
 * @param lagTotal    mensajes publicados que el grupo todavia no procesa
 * @param miembros    quien consume y que particiones tiene asignadas
 * @param offsets     avance del grupo en cada particion
 */
public record InfoGrupo(
        String grupo,
        String estado,
        Integer coordinador,
        long lagTotal,
        List<Miembro> miembros,
        List<OffsetParticion> offsets) {

    public record Miembro(String memberId, String clientId, String host, List<String> particiones) {
    }

    /**
     * @param offsetConsumido ultimo offset confirmado por el grupo (el proximo a leer)
     * @param offsetFinal     offset que recibira el proximo registro publicado
     * @param lag             offsetFinal - offsetConsumido
     */
    public record OffsetParticion(String topico, int particion, long offsetConsumido, long offsetFinal, long lag) {
    }
}
