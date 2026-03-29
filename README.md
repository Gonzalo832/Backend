# Backend - App Queseria

API REST para sincronizacion de entregas de leche desde app movil offline-first.

## Requisitos
- Node.js 18+
- MySQL 8+

## 1) Instalar dependencias
```bash
npm install
```

## 2) Configurar variables de entorno
```bash
cp .env.example .env
```
Edita `.env` con tus datos de MySQL.

Para pruebas desde celular o cualquier equipo en tu red local, deja:
```env
HOST=0.0.0.0
PORT=4000
```

Si macOS Firewall esta activo, permite conexiones entrantes para Node.js.

## 3) Crear esquema y datos semilla
Ejecuta en MySQL:
```sql
SOURCE /ruta/completa/Backend/sql/schema.sql;
SOURCE /ruta/completa/Backend/sql/seed.sql;
```

## 4) Levantar servidor
```bash
npm run dev
```

Con la configuracion por defecto, la API queda accesible desde tu Mac y desde otros dispositivos en la misma LAN usando la IP local de tu Mac.

## Endpoints principales
- `GET /api/health`
- `GET /api/rutas`
- `GET /api/lecheros?rutaId=1`
- `GET /api/maestros`
- `POST /api/sync/entregas`
- `GET /api/pagos/semanal?fecha=2026-03-18`

## Ejemplo de sincronizacion
`POST /api/sync/entregas`
```json
{
  "entregas": [
    {
      "localId": 101,
      "lecheroId": 1,
      "trabajadorId": 1,
      "fecha": "2026-03-21",
      "litrosEntregados": 120.5,
      "dedupeKey": "deviceA-101"
    }
  ]
}
```
