import { Component, OnDestroy, OnInit, computed, inject, signal } from '@angular/core';
import { DatePipe, DecimalPipe } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { FormsModule } from '@angular/forms';
import { MsalBroadcastService } from '@azure/msal-angular';
import { InteractionStatus } from '@azure/msal-browser';
import { Observable, Subscription, forkJoin, of, timer } from 'rxjs';
import { catchError, filter, map, switchMap, take } from 'rxjs/operators';
import { environment } from '../../../environments/environment';

/*
 * Panel de Kafka (EP3), solo para Admin.
 *
 * Muestra desde la aplicacion lo que pasa en el cluster Kafka, con los mismos
 * endpoints que usa Postman, a traves del API Gateway y el BFF:
 *
 *   /api/reportes/**  ms-pedidos360-reportes, el consumidor: lo que ya proceso
 *   /api/kafka/**     ms-pedidos360-kafkaadmin (AdminClient): topicos, grupos
 *
 * Se refresca solo cada 3 segundos. Asi se ve el sistema reaccionar a los
 * eventos: se crea o avanza un pedido en otra pestana y aqui aparece la fila
 * de auditoria con su particion y offset, sin recargar.
 */

export interface RegistroAuditoria {
  eventId: string;
  codigoPedido: string;
  accion: string;
  estadoAnterior: string | null;
  estadoNuevo: string;
  usuario: string;
  origen: string;
  detalle: string | null;
  eventoNegocioId: string;
  ocurridoEn: string;
  particion: number;
  offset: number;
}

export interface Resumen {
  totalPedidos: number;
  ventasEntregadas: number;
  porEstado: Record<string, { pedidos: number; monto: number }>;
  ultimoEvento: string | null;
}

export interface PedidoReporte {
  codigo: string;
  pedidoId: number;
  estado: string;
  clienteEmail: string;
  total: number;
  cantidadItems: number;
  ultimoEvento: string;
  eventosRecibidos: number;
}

export interface Cluster {
  clusterId: string;
  controller: number;
  brokers: { id: number; host: string; port: number }[];
}

export interface Metricas {
  brokers: number;
  topics: { name: string; messages: number; underReplicated: number }[];
  groups: { groupId: string; state: string; members: number; totalLag: number }[];
}

export interface Topico {
  name: string;
  replicationFactor: number;
  messages: number;
  underReplicated: number;
  partitions: {
    partition: number;
    leader: number;
    replicas: number[];
    isr: number[];
    beginningOffset: number;
    endOffset: number;
  }[];
}

export interface Grupo {
  groupId: string;
  state: string;
  coordinator: number;
  totalLag: number;
  members: { memberId: string; clientId: string; host: string; partitions: string[] }[];
  offsets: { topic: string; partition: number; committedOffset: number; endOffset: number; lag: number }[];
}

type Pestana = 'vivo' | 'topicos' | 'grupos';

const TOPICOS = ['pedidos-eventos', 'pedidos-auditoria', 'pedidos-eventos-dlt', 'pedidos-auditoria-dlt'];
const GRUPOS = ['reportes-pedidos', 'reportes-auditoria'];
const CONFIGS_A_MOSTRAR = ['retention.ms', 'cleanup.policy', 'min.insync.replicas'];
const INTERVALO_MS = 3000;
/** Cuanto tiempo se resalta una fila nueva de auditoria. */
const RESALTE_MS = 8000;

@Component({
  selector: 'app-kafka-panel',
  standalone: true,
  imports: [DatePipe, DecimalPipe, FormsModule],
  templateUrl: './kafka-panel.html',
  styleUrls: ['./kafka-panel.scss']
})
export class KafkaPanel implements OnInit, OnDestroy {
  private http = inject(HttpClient);
  private broadcast = inject(MsalBroadcastService);
  private api = environment.azure.api.url;
  private sondeo?: Subscription;

  readonly topicos = TOPICOS;
  readonly configsAMostrar = CONFIGS_A_MOSTRAR;

  pestana = signal<Pestana>('vivo');
  enPausa = signal(false);
  actualizado = signal<Date | null>(null);
  errores = signal<string[]>([]);

  // En vivo
  filtroPedido = '';
  pedidoFiltrado = signal('');
  auditoria = signal<RegistroAuditoria[]>([]);
  resumen = signal<Resumen | null>(null);
  pedidos = signal<PedidoReporte[]>([]);

  // Estado del cluster (siempre visible arriba)
  cluster = signal<Cluster | null>(null);
  metricas = signal<Metricas | null>(null);

  // Topicos y grupos
  detalleTopicos = signal<Record<string, Topico | null>>({});
  configs = signal<Record<string, Record<string, string>>>({});
  grupos = signal<Record<string, Grupo | null>>({});

  /** eventId -> momento en que este panel vio la fila por primera vez. */
  private vistos = new Map<string, number>();
  private primeraCarga = true;
  /** Mensajes en cada DLT al abrir el panel, para avisar si llegan nuevos. */
  private dltInicial: Record<string, number> | null = null;

  estados = computed(() => Object.entries(this.resumen()?.porEstado ?? {}));

  dlt = computed(() =>
    (this.metricas()?.topics ?? []).filter((t) => t.name.endsWith('-dlt')));

  /** DLT que recibieron mensajes desde que se abrio el panel. */
  dltNuevos = computed(() => {
    const inicial = this.dltInicial;
    if (!inicial) { return []; }
    return this.dlt()
      .filter((t) => t.messages > (inicial[t.name] ?? 0))
      .map((t) => ({ name: t.name, nuevos: t.messages - (inicial[t.name] ?? 0) }));
  });

  lagTotal = computed(() =>
    (this.metricas()?.groups ?? []).reduce((suma, g) => suma + g.totalLag, 0));

  ngOnInit() {
    // Como en el resto de la aplicacion: no pedir nada hasta que MSAL tenga la
    // cuenta lista, o las peticiones salen sin token.
    this.broadcast.inProgress$
      .pipe(filter((estado) => estado === InteractionStatus.None), take(1))
      .subscribe(() => {
        this.cargarConfigs();
        this.iniciarSondeo();
      });
  }

  ngOnDestroy() {
    this.sondeo?.unsubscribe();
  }

  cambiarPestana(p: Pestana) {
    this.pestana.set(p);
    this.refrescar();
  }

  alternarPausa() {
    this.enPausa.update((v) => !v);
    if (this.enPausa()) {
      this.sondeo?.unsubscribe();
    } else {
      this.iniciarSondeo();
    }
  }

  filtrar() {
    // Las filas que aparecen por cambiar el filtro no son eventos nuevos.
    this.primeraCarga = true;
    this.pedidoFiltrado.set(this.filtroPedido.trim());
    this.refrescar();
  }

  limpiarFiltro() {
    this.filtroPedido = '';
    this.filtrar();
  }

  verAuditoria(codigo: string) {
    this.filtroPedido = codigo;
    this.pestana.set('vivo');
    this.filtrar();
  }

  esNueva(r: RegistroAuditoria): boolean {
    const visto = this.vistos.get(r.eventId);
    return visto !== undefined && visto > 0 && Date.now() - visto < RESALTE_MS;
  }

  /** 604800000 -> "7 días"; 172800000 -> "48 h". */
  duracion(ms: string | undefined): string {
    const n = Number(ms);
    if (!ms || Number.isNaN(n)) { return ms ?? '—'; }
    if (n < 0) { return 'sin límite'; }
    const horas = n / 3_600_000;
    return horas >= 24 && horas % 24 === 0 ? `${horas / 24} días` : `${horas} h`;
  }

  isrIncompleto(p: { replicas: number[]; isr: number[] }): boolean {
    return p.isr.length < p.replicas.length;
  }

  /** Ancho de la barra de lag, relativo al mayor lag del grupo. */
  anchoLag(lag: number, grupo: Grupo): number {
    const max = Math.max(...grupo.offsets.map((o) => o.lag), 1);
    return Math.round((lag / max) * 100);
  }

  refrescar() {
    this.consultar().subscribe((r) => this.aplicar(r));
  }

  private iniciarSondeo() {
    this.sondeo?.unsubscribe();
    this.sondeo = timer(0, INTERVALO_MS)
      .pipe(switchMap(() => this.consultar()))
      .subscribe((r) => this.aplicar(r));
  }

  /**
   * Una ronda de consultas. Cada una tiene su propio catchError: si una falla
   * (por ejemplo, kafkaadmin reiniciandose), el resto del panel se sigue viendo.
   */
  private consultar() {
    const fallas: string[] = [];
    const get = <T>(ruta: string): Observable<T | null> =>
      this.http.get<T>(`${this.api}${ruta}`).pipe(
        catchError((err) => {
          fallas.push(`${ruta} → ${err.status || 'sin respuesta'}`);
          return of(null);
        }));

    const pedido = this.pedidoFiltrado();
    const rutaAuditoria = pedido
      ? `/api/reportes/auditoria?pedido=${encodeURIComponent(pedido)}`
      : '/api/reportes/auditoria';
    const pestana = this.pestana();

    // Lo de cada pestana solo se consulta mientras esa pestana esta abierta.
    const detalle$ = pestana === 'topicos'
      ? forkJoin(Object.fromEntries(TOPICOS.map((t) => [t, get<Topico>(`/api/kafka/topics/${t}`)])))
      : of(null);
    const grupos$ = pestana === 'grupos'
      ? forkJoin(Object.fromEntries(GRUPOS.map((g) => [g, get<Grupo>(`/api/kafka/consumer-groups/${g}`)])))
      : of(null);
    const vivo = pestana === 'vivo';

    return forkJoin({
      metricas: get<Metricas>('/api/kafka/metricas'),
      cluster: get<Cluster>('/api/kafka/cluster'),
      auditoria: vivo ? get<RegistroAuditoria[]>(rutaAuditoria) : of(undefined),
      resumen: vivo ? get<Resumen>('/api/reportes/pedidos/resumen') : of(undefined),
      pedidos: vivo ? get<PedidoReporte[]>('/api/reportes/pedidos') : of(undefined),
      detalle: detalle$,
      grupos: grupos$
    }).pipe(map((r) => ({ ...r, fallas, filtro: pedido })));
  }

  private aplicar(r: {
    metricas: Metricas | null;
    cluster: Cluster | null;
    auditoria: RegistroAuditoria[] | null | undefined;
    resumen: Resumen | null | undefined;
    pedidos: PedidoReporte[] | null | undefined;
    detalle: Record<string, Topico | null> | null;
    grupos: Record<string, Grupo | null> | null;
    fallas: string[];
    filtro: string;
  }) {
    // Una respuesta pedida con el filtro anterior llega tarde: se descarta.
    if (r.filtro !== this.pedidoFiltrado()) { r.auditoria = undefined; }
    if (r.metricas) {
      this.metricas.set(r.metricas);
      if (!this.dltInicial) {
        this.dltInicial = Object.fromEntries(
          r.metricas.topics.filter((t) => t.name.endsWith('-dlt')).map((t) => [t.name, t.messages]));
      }
    }
    if (r.cluster) { this.cluster.set(r.cluster); }
    if (r.auditoria) { this.registrarVistos(r.auditoria); this.auditoria.set(r.auditoria); }
    if (r.resumen) { this.resumen.set(r.resumen); }
    if (r.pedidos) { this.pedidos.set(r.pedidos); }
    if (r.detalle) { this.detalleTopicos.set(r.detalle); }
    if (r.grupos) { this.grupos.set(r.grupos); }
    this.errores.set(r.fallas);
    this.actualizado.set(new Date());
  }

  /**
   * En la primera carga todo se marca como ya visto (0); despues, las filas
   * que no estaban se marcan con la hora actual para resaltarlas.
   */
  private registrarVistos(filas: RegistroAuditoria[]) {
    const ahora = Date.now();
    for (const f of filas) {
      if (!this.vistos.has(f.eventId)) {
        this.vistos.set(f.eventId, this.primeraCarga ? 0 : ahora);
      }
    }
    this.primeraCarga = false;
  }

  private cargarConfigs() {
    forkJoin(Object.fromEntries(TOPICOS.map((t) => [t,
      this.http.get<Record<string, string>>(`${this.api}/api/kafka/topics/${t}/configs`)
        .pipe(catchError(() => of({} as Record<string, string>)))])))
      .subscribe((c) => this.configs.set(c));
  }
}
