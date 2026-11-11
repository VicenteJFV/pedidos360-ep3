package cl.duoc.pedidos360.orders.kafka;

/**
 * Nombres de los topicos del caso, en un solo lugar.
 *
 * Son los de la seccion 4 de la guia de EP3 para el Caso 0. Los crea
 * kafka-cluster/crear-topicos.sh con 3 particiones y factor de replicacion 3;
 * el broker tiene auto.create.topics.enable=false, asi que un nombre mal
 * escrito aqui falla en vez de crear un topico nuevo.
 */
public final class TopicosKafka {

    /** Eventos del negocio: hechos funcionales del pedido. Lo consume la reporteria. */
    public static final String EVENTOS = "pedidos-eventos";

    /** Auditoria: quien hizo que, cuando y sobre que pedido. */
    public static final String AUDITORIA = "pedidos-auditoria";

    private TopicosKafka() {
    }
}
