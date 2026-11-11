package cl.duoc.pedidos360.kafkaadmin.web;

import cl.duoc.pedidos360.kafkaadmin.service.KafkaResourceManager;
import cl.duoc.pedidos360.kafkaadmin.web.dto.RespuestaError;
import cl.duoc.pedidos360.kafkaadmin.web.dto.RespuestaOperacion;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * API del administrador Kafka (seccion 6 de la guia de EP3).
 *
 *   GET    /api/kafka/cluster                         brokers y controlador
 *   GET    /api/kafka/topics                          listar topicos
 *   GET    /api/kafka/topics/{name}                   particiones, lider, replicas, ISR y offsets
 *   POST   /api/kafka/topics?name=&partitions=&replicationFactor=[&retentionMs=&cleanupPolicy=]
 *   DELETE /api/kafka/topics/{name}                   solo topicos de prueba
 *   GET    /api/kafka/topics/{name}/config            retention.ms, cleanup.policy...
 *   PUT    /api/kafka/topics/{name}/config            cuerpo JSON {"retention.ms": "86400000"}
 *   GET    /api/kafka/consumer-groups                 listar grupos
 *   GET    /api/kafka/consumer-groups/{groupId}       miembros, offsets y lag
 *   DELETE /api/kafka/consumer-groups/{groupId}       solo grupos de prueba sin miembros
 *   GET    /api/kafka/metricas                        vista rapida del caso
 *
 * Igual que RabbitMQController: el controlador valida y traduce a HTTP, y toda
 * la validacion ocurre antes de llamar al cluster. Un parametro invalido se
 * rechaza con 400 nombrando el parametro, sin abrir una conexion.
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    /**
     * Los caracteres que Kafka acepta en un nombre de topico, hasta 249. No se
     * permite empezar con "__", que es el prefijo de los topicos internos.
     */
    private static final Pattern NOMBRE_TOPICO = Pattern.compile("^(?!__)[a-zA-Z0-9._-]{1,249}$");
    private static final Pattern CLEANUP_POLICY = Pattern.compile("^(delete|compact|delete,compact|compact,delete)$");

    static final int MAX_PARTICIONES = 50;
    static final int MAX_REPLICACION = 3;

    private final KafkaResourceManager administrador;

    public KafkaController(KafkaResourceManager administrador) {
        this.administrador = administrador;
    }

    @GetMapping("/cluster")
    public Object cluster() {
        return administrador.cluster();
    }

    // ---------------------------------------------------------------------
    // Topicos
    // ---------------------------------------------------------------------

    @GetMapping("/topics")
    public Object listarTopicos(@RequestParam(defaultValue = "false") boolean internos) {
        return administrador.listarTopicos(internos);
    }

    @GetMapping("/topics/{name}")
    public ResponseEntity<?> consultarTopico(@PathVariable String name) {
        Optional<ResponseEntity<RespuestaError>> error = validarNombre(name, "name");
        if (error.isPresent()) {
            return error.get();
        }
        return ResponseEntity.ok(administrador.consultarTopico(name.trim()));
    }

    @PostMapping("/topics")
    public ResponseEntity<?> crearTopico(@RequestParam(required = false) String name,
                                         @RequestParam(required = false) String partitions,
                                         @RequestParam(required = false) String replicationFactor,
                                         @RequestParam(required = false) String retentionMs,
                                         @RequestParam(required = false) String cleanupPolicy) {
        Optional<ResponseEntity<RespuestaError>> error = validarNombre(name, "name");
        if (error.isPresent()) {
            return error.get();
        }

        Integer particiones = entero(partitions);
        if (particiones == null || particiones < 1 || particiones > MAX_PARTICIONES) {
            return invalido("partitions debe ser un entero entre 1 y " + MAX_PARTICIONES, "partitions");
        }
        Integer replicacion = entero(replicationFactor);
        if (replicacion == null || replicacion < 1 || replicacion > MAX_REPLICACION) {
            return invalido("replicationFactor debe ser un entero entre 1 y " + MAX_REPLICACION
                    + " (el cluster tiene 3 brokers)", "replicationFactor");
        }

        Map<String, String> configuraciones = new LinkedHashMap<>();
        if (StringUtils.hasText(retentionMs)) {
            configuraciones.put("retention.ms", retentionMs.trim());
        }
        if (StringUtils.hasText(cleanupPolicy)) {
            configuraciones.put("cleanup.policy", cleanupPolicy.trim());
        }
        Optional<ResponseEntity<RespuestaError>> errorConfig = validarConfiguraciones(configuraciones, replicacion,
                Map.of("retention.ms", "retentionMs", "cleanup.policy", "cleanupPolicy"));
        if (errorConfig.isPresent()) {
            return errorConfig.get();
        }

        administrador.crearTopico(name.trim(), particiones, replicacion.shortValue(), configuraciones);
        return ResponseEntity.status(HttpStatus.CREATED).body(new RespuestaOperacion("crear", "topico", name.trim(),
                particiones + " particiones, factor de replicacion " + replicacion));
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<?> eliminarTopico(@PathVariable String name) {
        Optional<ResponseEntity<RespuestaError>> error = validarNombre(name, "name");
        if (error.isPresent()) {
            return error.get();
        }
        administrador.eliminarTopico(name.trim());
        return ResponseEntity.ok(RespuestaOperacion.de("eliminar", "topico", name.trim()));
    }

    // ---------------------------------------------------------------------
    // Configuraciones
    // ---------------------------------------------------------------------

    @GetMapping("/topics/{name}/config")
    public ResponseEntity<?> consultarConfiguracion(@PathVariable String name) {
        Optional<ResponseEntity<RespuestaError>> error = validarNombre(name, "name");
        if (error.isPresent()) {
            return error.get();
        }
        return ResponseEntity.ok(administrador.consultarConfiguracion(name.trim()));
    }

    @PutMapping("/topics/{name}/config")
    public ResponseEntity<?> modificarConfiguracion(@PathVariable String name,
                                                    @RequestBody(required = false) Map<String, Object> cuerpo) {
        Optional<ResponseEntity<RespuestaError>> error = validarNombre(name, "name");
        if (error.isPresent()) {
            return error.get();
        }
        if (cuerpo == null || cuerpo.isEmpty()) {
            return invalido("Indica al menos una configuracion, por ejemplo {\"retention.ms\": \"86400000\"}",
                    "body");
        }

        Map<String, String> cambios = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entrada : cuerpo.entrySet()) {
            if (!KafkaResourceManager.CONFIGURACIONES_ADMINISTRABLES.contains(entrada.getKey())) {
                return invalido(entrada.getKey() + " no es administrable desde esta API. Se permiten: "
                        + KafkaResourceManager.CONFIGURACIONES_ADMINISTRABLES, entrada.getKey());
            }
            if (entrada.getValue() == null || !StringUtils.hasText(entrada.getValue().toString())) {
                return invalido(entrada.getKey() + " no puede ir vacio", entrada.getKey());
            }
            cambios.put(entrada.getKey(), entrada.getValue().toString().trim());
        }
        Optional<ResponseEntity<RespuestaError>> errorConfig = validarConfiguraciones(cambios, MAX_REPLICACION, Map.of());
        if (errorConfig.isPresent()) {
            return errorConfig.get();
        }

        return ResponseEntity.ok(administrador.modificarConfiguracion(name.trim(), cambios));
    }

    // ---------------------------------------------------------------------
    // Consumer groups
    // ---------------------------------------------------------------------

    @GetMapping("/consumer-groups")
    public Object listarGrupos() {
        return administrador.listarGrupos();
    }

    @GetMapping("/consumer-groups/{groupId}")
    public ResponseEntity<?> consultarGrupo(@PathVariable String groupId) {
        if (!StringUtils.hasText(groupId)) {
            return invalido("groupId es obligatorio", "groupId");
        }
        return ResponseEntity.ok(administrador.consultarGrupo(groupId.trim()));
    }

    @DeleteMapping("/consumer-groups/{groupId}")
    public ResponseEntity<?> eliminarGrupo(@PathVariable String groupId) {
        if (!StringUtils.hasText(groupId)) {
            return invalido("groupId es obligatorio", "groupId");
        }
        administrador.eliminarGrupo(groupId.trim());
        return ResponseEntity.ok(RespuestaOperacion.de("eliminar", "consumer group", groupId.trim()));
    }

    @GetMapping("/metricas")
    public Object metricas() {
        return administrador.metricas();
    }

    // ---------------------------------------------------------------------
    // Validacion
    // ---------------------------------------------------------------------

    private static Optional<ResponseEntity<RespuestaError>> validarNombre(String nombre, String parametro) {
        if (!StringUtils.hasText(nombre)) {
            return Optional.of(invalido(parametro + " es obligatorio", parametro));
        }
        if (!NOMBRE_TOPICO.matcher(nombre.trim()).matches()) {
            return Optional.of(invalido(parametro + " solo admite letras, numeros, punto, guion y guion bajo"
                    + " (hasta 249 caracteres) y no puede empezar con __", parametro));
        }
        return Optional.empty();
    }

    /**
     * Rangos de cada configuracion, para no enviarle al cluster un valor que va a rechazar.
     *
     * @param nombres como se llama cada propiedad en la peticion, si no es igual
     *                al nombre de Kafka (al crear: retentionMs en vez de retention.ms)
     */
    private static Optional<ResponseEntity<RespuestaError>> validarConfiguraciones(Map<String, String> configuraciones,
                                                                                  int replicacion,
                                                                                  Map<String, String> nombres) {
        for (Map.Entry<String, String> c : configuraciones.entrySet()) {
            String clave = c.getKey();
            String valor = c.getValue();
            boolean valido = switch (clave) {
                case "retention.ms", "retention.bytes" -> {
                    Long n = largo(valor);
                    yield n != null && (n == -1 || n >= 1);
                }
                case "cleanup.policy" -> CLEANUP_POLICY.matcher(valor).matches();
                case "min.insync.replicas" -> {
                    Integer n = entero(valor);
                    yield n != null && n >= 1 && n <= replicacion;
                }
                case "max.message.bytes" -> {
                    Integer n = entero(valor);
                    yield n != null && n >= 1;
                }
                default -> false;
            };
            if (!valido) {
                String regla = switch (clave) {
                    case "retention.ms" -> "milisegundos: un entero mayor que 0, o -1 para no borrar por tiempo";
                    case "retention.bytes" -> "bytes: un entero mayor que 0, o -1 para no limitar por tamano";
                    case "cleanup.policy" -> "delete, compact o compact,delete";
                    case "min.insync.replicas" -> "un entero entre 1 y el factor de replicacion (" + replicacion + ")";
                    case "max.message.bytes" -> "un entero mayor que 0";
                    default -> "no es una configuracion administrable";
                };
                return Optional.of(invalido(clave + " no es valido: " + regla, nombres.getOrDefault(clave, clave)));
            }
        }
        return Optional.empty();
    }

    private static Integer entero(String valor) {
        try {
            return valor == null ? null : Integer.valueOf(valor.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long largo(String valor) {
        try {
            return valor == null ? null : Long.valueOf(valor.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static ResponseEntity<RespuestaError> invalido(String mensaje, String parametro) {
        return ResponseEntity.badRequest().body(new RespuestaError(mensaje, parametro));
    }
}
