package cl.duoc.pedidos360.reportes;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Consumidor Kafka de Pedidos360 (EP3).
 *
 * Lee los dos topicos que publica ms-pedidos360-orders y los convierte en
 * informacion consultable:
 *
 *   pedidos-eventos    -> reporteria: ultimo estado de cada pedido y resumen
 *   pedidos-auditoria  -> auditoria: quien hizo que y cuando, por pedido
 *
 * No publica eventos del negocio: el unico productor del caso es orders. Lo
 * unico que escribe en Kafka son los mensajes fallidos hacia los DLT.
 */
@SpringBootApplication
public class ReportesApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReportesApplication.class, args);
    }
}
