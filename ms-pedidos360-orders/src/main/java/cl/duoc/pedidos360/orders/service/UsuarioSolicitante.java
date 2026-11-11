package cl.duoc.pedidos360.orders.service;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Identifica quien pidio la accion, para el evento de auditoria.
 *
 * Este microservicio no recibe el JWT: lo valida el BFF, que reenvia el usuario
 * del token (preferred_username) en la cabecera X-Usuario. Se toma solo el
 * nombre de usuario; el token nunca llega aqui ni se registra.
 *
 * Sin solicitud HTTP en curso -una prueba, una tarea interna- o sin la cabecera,
 * la accion se atribuye a "sistema".
 */
@Component
public class UsuarioSolicitante {

    public static final String CABECERA = "X-Usuario";
    static final String SIN_USUARIO = "sistema";

    public String actual() {
        if (!(RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes atributos)) {
            return SIN_USUARIO;
        }
        HttpServletRequest solicitud = atributos.getRequest();
        String usuario = solicitud.getHeader(CABECERA);
        return StringUtils.hasText(usuario) ? usuario.trim() : SIN_USUARIO;
    }
}
