package cl.duoc.pedidos360.reportes.config;

import cl.duoc.pedidos360.reportes.consumer.EventoInvalidoException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reintentos y Dead Letter Topic, como el KafkaErrorConfig de la guia
 * practica 3.3.2.
 *
 *   intento 1 -> falla -> espera 1 s -> intento 2 -> falla -> espera 1 s
 *   -> intento 3 -> falla -> se publica en <topico>-dlt y se sigue con el
 *   siguiente mensaje
 *
 * Con esto un mensaje que no se puede procesar no detiene la particion para
 * siempre (seccion 7 de la guia): despues de los reintentos sale del camino y
 * queda en el DLT para revisarlo.
 *
 * Dos diferencias con la guia, ambas para no reintentar en vano:
 *  - Un evento que no pasa la validacion (EventoInvalidoException) va directo
 *    al DLT: va a fallar igual en cada intento.
 *  - Un mensaje que ni siquiera es JSON valido falla en el
 *    ErrorHandlingDeserializer, antes de llegar al listener, y Spring Kafka
 *    tampoco lo reintenta. Llega al DLT con sus bytes originales.
 */
@Configuration
public class KafkaErrorConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);

    static final String SUFIJO_DLT = "-dlt";

    /**
     * El DLT de cada topico es el mismo nombre con -dlt (pedidos-eventos-dlt),
     * como eventos-entrada-dlt en la guia. Se respeta la particion de origen:
     * los DLT tambien tienen 3 particiones, y asi un mensaje fallido conserva
     * la particion -y por lo tanto el orden- de su pedido.
     *
     * Hay dos plantillas porque el valor que llega al recuperador no siempre es
     * del mismo tipo: un DTO, si el mensaje se leyo bien y fallo despues, o los
     * bytes originales, si ni siquiera se pudo leer. Con un solo serializador
     * JSON, esos bytes quedarian guardados como texto en Base64.
     */
    @Bean
    public DeadLetterPublishingRecoverer recoverer(ProducerFactory<Object, Object> producerFactory) {
        KafkaTemplate<Object, Object> json = new KafkaTemplate<>(producerFactory.copyWithConfigurationOverride(Map.of(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,
                "spring.json.add.type.headers", false)));
        KafkaTemplate<Object, Object> bytes = new KafkaTemplate<>(producerFactory.copyWithConfigurationOverride(Map.of(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class)));

        Map<Class<?>, KafkaOperations<?, ?>> plantillas = new LinkedHashMap<>();
        plantillas.put(byte[].class, bytes);
        plantillas.put(Object.class, json);

        return new DeadLetterPublishingRecoverer(plantillas,
                (registro, error) -> new TopicPartition(registro.topic() + SUFIJO_DLT, registro.partition()));
    }

    @Bean
    public DefaultErrorHandler errorHandler(
            DeadLetterPublishingRecoverer recoverer,
            @Value("${reportes.reintentos.intervalo-ms:1000}") long intervaloMs,
            @Value("${reportes.reintentos.maximo:2}") long maximo) {

        ConsumerRecordRecoverer conRegistro = (registro, error) -> {
            log.error("ENVIADO A DLT | Topico: {} | Particion: {} | Offset: {} | Key: {} | destino: {} | motivo: {}",
                    registro.topic(), registro.partition(), registro.offset(), registro.key(),
                    registro.topic() + SUFIJO_DLT, motivo(error));
            recoverer.accept(registro, error);
        };

        DefaultErrorHandler manejador = new DefaultErrorHandler(conRegistro, new FixedBackOff(intervaloMs, maximo));
        manejador.addNotRetryableExceptions(EventoInvalidoException.class);

        long intentos = maximo + 1;
        manejador.setRetryListeners((registro, error, intento) -> registrarFallo(registro, error, intento, intentos));
        return manejador;
    }

    private static void registrarFallo(ConsumerRecord<?, ?> registro, Exception error, int intento, long intentos) {
        Throwable causa = NestedExceptionUtils.getMostSpecificCause(error);
        if (causa instanceof EventoInvalidoException) {
            return; // ya lo registro el consumidor como EVENTO INVALIDO
        }
        if (error instanceof DeserializationException || causa instanceof DeserializationException) {
            log.error("MENSAJE ILEGIBLE | Topico: {} | Particion: {} | Offset: {} | Key: {} | no es un JSON valido | no se reintenta",
                    registro.topic(), registro.partition(), registro.offset(), registro.key());
            return;
        }
        log.warn("ERROR DE PROCESAMIENTO | Topico: {} | Particion: {} | Offset: {} | Key: {} | intento {} de {} | motivo: {}",
                registro.topic(), registro.partition(), registro.offset(), registro.key(),
                intento, intentos, causa.getMessage());
    }

    private static String motivo(Exception error) {
        return NestedExceptionUtils.getMostSpecificCause(error).getMessage();
    }
}
