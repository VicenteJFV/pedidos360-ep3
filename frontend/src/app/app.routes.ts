import { Routes } from '@angular/router';
import { Catalogo } from './components/catalogo/catalogo';
import { AdminDashboard } from './components/admin-dashboard/admin-dashboard';
import { Carrito } from './components/carrito/carrito';
import { KafkaPanel } from './components/kafka-panel/kafka-panel';

import { adminGuard } from './guards/admin.guard';
import { MsalGuard } from '@azure/msal-angular';

export const routes: Routes = [
  { path: 'catalogo', component: Catalogo },
  { path: 'admin', component: AdminDashboard, canActivate: [MsalGuard, adminGuard] },
  // EP3: reporteria, auditoria y estado del cluster Kafka. Solo Admin, igual
  // que /api/kafka y /api/reportes en el BFF.
  { path: 'kafka', component: KafkaPanel, canActivate: [MsalGuard, adminGuard] },
  // Cualquier usuario con sesion; el rol solo cambia a nombre de quien queda el pedido.
  { path: 'carrito', component: Carrito, canActivate: [MsalGuard] },
  { path: '', redirectTo: '/catalogo', pathMatch: 'full' } 
];