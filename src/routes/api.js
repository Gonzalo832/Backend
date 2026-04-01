const express = require('express');
const pool = require('../db');
const { syncEntregas } = require('../services/syncService');

const router = express.Router();
const asyncHandler = (handler) => (req, res, next) => Promise.resolve(handler(req, res, next)).catch(next);
const REGISTROS_RETENCION_DIAS = 15;

async function tableExists(tableName, connectionOrPool = pool) {
  const [rows] = await connectionOrPool.query(`SHOW TABLES LIKE '${tableName}'`);
  return rows.length > 0;
}

async function getEntregaLecheroColumn(connectionOrPool = pool) {
  const [rows] = await connectionOrPool.query("SHOW COLUMNS FROM registros_entrega LIKE 'lechero_id'");
  if (rows.length === 0) {
    throw new Error('La tabla registros_entrega debe tener la columna lechero_id');
  }

  return 'lechero_id';
}

async function getCatalogTableName(connectionOrPool = pool) {
  if (await tableExists('lecheros', connectionOrPool)) {
    return 'lecheros';
  }

  throw new Error('No existe tabla de catalogo para lecheros');
}

async function ensurePedidosTable(connectionOrPool = pool) {
  await connectionOrPool.execute(
    `CREATE TABLE IF NOT EXISTS pedidos (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      ruta_id BIGINT NULL,
      lechero_id BIGINT NULL,
      nombre_cliente VARCHAR(180) NOT NULL,
      nota TEXT NULL,
      fecha DATE NOT NULL,
      fecha_entrega DATE NULL,
      kg_solicitado_fresco DECIMAL(10,3) NOT NULL DEFAULT 0,
      kg_solicitado_hebra DECIMAL(10,3) NOT NULL DEFAULT 0,
      kg_entregado_fresco DECIMAL(10,3) NULL,
      kg_entregado_hebra DECIMAL(10,3) NULL,
      independiente TINYINT(1) NOT NULL DEFAULT 0,
      dedupe_key VARCHAR(255) NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uk_pedidos_dedupe_key (dedupe_key)
    )`
  );
}

async function purgeOldRegistros(connectionOrPool = pool) {
  const diasPrevios = REGISTROS_RETENCION_DIAS - 1;

  await connectionOrPool.execute(
    `DELETE FROM registros_entrega
     WHERE fecha < DATE_SUB(CURDATE(), INTERVAL ${diasPrevios} DAY)`
  );
}

function parseIsoDate(value) {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(trimmed) ? trimmed : null;
}

router.get('/health', asyncHandler(async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ ok: true, message: 'API y DB disponibles' });
  } catch (error) {
    res.status(500).json({ ok: false, message: 'Error de conexion a DB' });
  }
}));

router.get('/rutas', asyncHandler(async (_req, res) => {
  const [rows] = await pool.query('SELECT id, nombre FROM rutas ORDER BY nombre ASC');
  res.json(rows);
}));

router.get('/productores', asyncHandler(async (req, res) => {
  const rutaId = Number(req.query.rutaId);

  if (!rutaId) {
    return res.status(400).json({ message: 'rutaId es obligatorio' });
  }

  const tableName = await getCatalogTableName();

  const [rows] = await pool.execute(
    `SELECT id, nombre, ruta_id AS rutaId
     FROM ${tableName}
     WHERE ruta_id = ?
     ORDER BY nombre ASC`,
    [rutaId]
  );

  return res.json(rows);
}));

router.get('/lecheros', asyncHandler(async (req, res) => {
  const rutaId = Number(req.query.rutaId);

  if (!rutaId) {
    return res.status(400).json({ message: 'rutaId es obligatorio' });
  }

  const tableName = await getCatalogTableName();

  const [rows] = await pool.execute(
    `SELECT id, nombre, ruta_id AS rutaId
     FROM ${tableName}
     WHERE ruta_id = ?
     ORDER BY nombre ASC`,
    [rutaId]
  );

  return res.json(rows);
}));

router.post('/rutas', asyncHandler(async (req, res) => {
  const nombre = req.body?.nombre?.trim();

  if (!nombre) {
    return res.status(400).json({ message: 'El nombre de la ruta es obligatorio' });
  }

  const [result] = await pool.execute(
    'INSERT INTO rutas (nombre) VALUES (?)',
    [nombre]
  );

  return res.status(201).json({ id: result.insertId, nombre });
}));

router.post('/lecheros', asyncHandler(async (req, res) => {
  const nombre = req.body?.nombre?.trim();
  const rutaId = Number(req.body?.rutaId);

  if (!nombre) {
    return res.status(400).json({ message: 'El nombre del lechero es obligatorio' });
  }
  if (!rutaId) {
    return res.status(400).json({ message: 'rutaId es obligatorio' });
  }

  try {
    const [rutaRows] = await pool.execute('SELECT id FROM rutas WHERE id = ? LIMIT 1', [rutaId]);
    if (rutaRows.length === 0) {
      return res.status(400).json({
        message: `La ruta ${rutaId} no existe en servidor. Actualiza/descarga catalogo y vuelve a intentar.`
      });
    }

    const tableName = await getCatalogTableName();

    const [existsRows] = await pool.execute(
      `SELECT id, nombre, ruta_id AS rutaId
       FROM ${tableName}
       WHERE ruta_id = ? AND LOWER(nombre) = LOWER(?)
       LIMIT 1`,
      [rutaId, nombre]
    );

    if (existsRows.length > 0) {
      return res.json(existsRows[0]);
    }

    const [result] = await pool.execute(
      `INSERT INTO ${tableName} (nombre, ruta_id) VALUES (?, ?)`,
      [nombre, rutaId]
    );

    return res.status(201).json({ id: result.insertId, nombre, rutaId });
  } catch (error) {
    return res.status(500).json({
      message: 'No se pudo crear el lechero',
      detail: error.message
    });
  }
}));

router.delete('/lecheros/:id', asyncHandler(async (req, res) => {
  const id = Number(req.params.id);

  if (!id) {
    return res.status(400).json({ message: 'id invalido' });
  }

  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();
    const entregaColumn = await getEntregaLecheroColumn(connection);

    const [rows] = await connection.execute(
      'SELECT id, nombre FROM lecheros WHERE id = ? LIMIT 1',
      [id]
    );

    if (rows.length === 0) {
      await connection.rollback();
      return res.status(404).json({ message: 'El lechero no existe' });
    }

    await connection.execute(
      `DELETE FROM registros_entrega WHERE ${entregaColumn} = ?`,
      [id]
    );
    await connection.execute('DELETE FROM lecheros WHERE id = ?', [id]);

    await connection.commit();
    return res.json({ ok: true, id, nombre: rows[0].nombre });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      message: 'No se pudo eliminar el lechero',
      detail: error.message
    });
  } finally {
    connection.release();
  }
}));

router.delete('/rutas/:id', asyncHandler(async (req, res) => {
  const id = Number(req.params.id);

  if (!id) {
    return res.status(400).json({ message: 'id invalido' });
  }

  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();
    const tableName = await getCatalogTableName(connection);
    const entregaColumn = await getEntregaLecheroColumn(connection);

    const [rutas] = await connection.execute('SELECT id, nombre FROM rutas WHERE id = ?', [id]);

    if (rutas.length === 0) {
      await connection.rollback();
      return res.status(404).json({ message: 'La ruta no existe' });
    }

    await connection.execute(
      `DELETE FROM registros_entrega
       WHERE ${entregaColumn} IN (
         SELECT id FROM ${tableName} WHERE ruta_id = ?
       )`,
      [id]
    );
    await connection.execute(`DELETE FROM ${tableName} WHERE ruta_id = ?`, [id]);
    await connection.execute('DELETE FROM rutas WHERE id = ?', [id]);

    await connection.commit();
    return res.json({ ok: true });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      message: 'No se pudo eliminar la ruta',
      detail: error.message
    });
  } finally {
    connection.release();
  }
}));

router.get('/maestros', asyncHandler(async (_req, res) => {
  const [rutas] = await pool.query('SELECT id, nombre FROM rutas ORDER BY nombre ASC');
  const tableName = await getCatalogTableName();
  const [lecheros] = await pool.query(
    `SELECT id, nombre, ruta_id AS rutaId FROM ${tableName} ORDER BY nombre ASC`
  );

  res.json({ rutas, productores: lecheros, lecheros });
}));

router.post('/sync/entregas', asyncHandler(async (req, res) => {
  const entregas = req.body?.entregas;

  if (!Array.isArray(entregas) || entregas.length === 0) {
    return res.status(400).json({ message: 'entregas debe ser un arreglo con al menos un registro' });
  }

  for (const entrega of entregas) {
    const lecheroId = entrega.lecheroId ?? entrega.productorId;
    const dedupeKey = String(entrega.dedupeKey || '').trim();
    if (
      typeof entrega.localId !== 'number' ||
      typeof lecheroId !== 'number' ||
      typeof entrega.fecha !== 'string' ||
      typeof entrega.litrosEntregados !== 'number' ||
      !dedupeKey
    ) {
      return res.status(400).json({ message: 'Estructura de entrega invalida' });
    }

    entrega.dedupeKey = dedupeKey;
  }

  try {
    await purgeOldRegistros();
    const syncedLocalIds = await syncEntregas(entregas);
    return res.json({ syncedLocalIds });
  } catch (error) {
    return res.status(500).json({ message: 'No se pudo sincronizar', detail: error.message });
  }
}));

router.post('/sync/pedidos', asyncHandler(async (req, res) => {
  const pedidos = req.body?.pedidos;

  if (!Array.isArray(pedidos) || pedidos.length === 0) {
    return res.status(400).json({ message: 'pedidos debe ser un arreglo con al menos un registro' });
  }

  const connection = await pool.getConnection();
  const syncedLocalIds = [];

  try {
    await connection.beginTransaction();
    await ensurePedidosTable(connection);

    for (const pedido of pedidos) {
      const localId = Number(pedido.localId);
      const dedupeKey = String(pedido.dedupeKey || '').trim();
      const rutaId = Number(pedido.rutaId);
      const lecheroId = pedido.productorId === null || pedido.productorId === undefined
        ? null
        : Number(pedido.productorId);
      const nombreCliente = String(pedido.nombreCliente || '').trim();
      const nota = String(pedido.nota || '').trim();
      const fecha = parseIsoDate(pedido.fecha);
      const fechaEntrega = parseIsoDate(pedido.fechaEntrega);
      const kgSolicitadoFresco = Number(pedido.kgSolicitadoFresco || 0);
      const kgSolicitadoHebra = Number(pedido.kgSolicitadoHebra || 0);
      const kgEntregadoFresco = pedido.kgEntregadoFresco === null || pedido.kgEntregadoFresco === undefined || pedido.kgEntregadoFresco === ''
        ? null
        : Number(pedido.kgEntregadoFresco);
      const kgEntregadoHebra = pedido.kgEntregadoHebra === null || pedido.kgEntregadoHebra === undefined || pedido.kgEntregadoHebra === ''
        ? null
        : Number(pedido.kgEntregadoHebra);
      const independiente = Number(pedido.independiente) ? 1 : 0;

      if (!Number.isFinite(localId) || localId <= 0) {
        throw new Error('localId invalido en pedidos');
      }
      if (!Number.isFinite(rutaId) || rutaId <= 0) {
        throw new Error('rutaId invalido en pedidos');
      }
      if (!dedupeKey) {
        throw new Error('dedupeKey invalido en pedidos');
      }
      if (!nombreCliente) {
        throw new Error('nombreCliente invalido en pedidos');
      }
      if (!fecha) {
        throw new Error('fecha invalida en pedidos');
      }
      if (!fechaEntrega) {
        throw new Error('fechaEntrega invalida en pedidos');
      }
      if (!Number.isFinite(kgSolicitadoFresco) || kgSolicitadoFresco < 0) {
        throw new Error('kgSolicitadoFresco invalido en pedidos');
      }
      if (!Number.isFinite(kgSolicitadoHebra) || kgSolicitadoHebra < 0) {
        throw new Error('kgSolicitadoHebra invalido en pedidos');
      }
      if (kgEntregadoFresco !== null && (!Number.isFinite(kgEntregadoFresco) || kgEntregadoFresco < 0)) {
        throw new Error('kgEntregadoFresco invalido en pedidos');
      }
      if (kgEntregadoHebra !== null && (!Number.isFinite(kgEntregadoHebra) || kgEntregadoHebra < 0)) {
        throw new Error('kgEntregadoHebra invalido en pedidos');
      }

      await connection.execute(
        `INSERT INTO pedidos (
          ruta_id,
          lechero_id,
          nombre_cliente,
          nota,
          fecha,
          fecha_entrega,
          kg_solicitado_fresco,
          kg_solicitado_hebra,
          kg_entregado_fresco,
          kg_entregado_hebra,
          independiente,
          dedupe_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          ruta_id = VALUES(ruta_id),
          lechero_id = VALUES(lechero_id),
          nombre_cliente = VALUES(nombre_cliente),
          nota = VALUES(nota),
          fecha = VALUES(fecha),
          fecha_entrega = VALUES(fecha_entrega),
          kg_solicitado_fresco = VALUES(kg_solicitado_fresco),
          kg_solicitado_hebra = VALUES(kg_solicitado_hebra),
          kg_entregado_fresco = VALUES(kg_entregado_fresco),
          kg_entregado_hebra = VALUES(kg_entregado_hebra),
          independiente = VALUES(independiente)`,
        [
          rutaId,
          lecheroId,
          nombreCliente,
          nota,
          fecha,
          fechaEntrega,
          kgSolicitadoFresco,
          kgSolicitadoHebra,
          kgEntregadoFresco,
          kgEntregadoHebra,
          independiente,
          dedupeKey,
        ]
      );

      syncedLocalIds.push(localId);
    }

    await connection.commit();
    return res.json({ syncedLocalIds });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({ message: 'No se pudo sincronizar pedidos', detail: error.message });
  } finally {
    connection.release();
  }
}));

router.get('/registros/quincena', asyncHandler(async (req, res) => {
  const rutaId = Number(req.query.rutaId);
  const lecheroId = Number(req.query.lecheroId);
  const fechaHasta = parseIsoDate(req.query.fechaHasta) || new Date().toISOString().slice(0, 10);

  if (!rutaId) {
    return res.status(400).json({ message: 'rutaId es obligatorio' });
  }

  await purgeOldRegistros();

  const tableName = await getCatalogTableName();
  const fkLechero = await getEntregaLecheroColumn();
  const filtroLechero = Number.isFinite(lecheroId) && lecheroId > 0 ? 'AND p.id = ?' : '';

  const query = `
    SELECT
      p.id AS lecheroId,
      p.nombre AS lechero,
      DATE_FORMAT(re.fecha, '%Y-%m-%d') AS fecha,
      SUM(re.litros_entregados) AS litros
    FROM registros_entrega re
    INNER JOIN ${tableName} p ON p.id = re.${fkLechero}
    WHERE p.ruta_id = ?
      AND re.fecha >= DATE_SUB(?, INTERVAL ${REGISTROS_RETENCION_DIAS - 1} DAY)
      AND re.fecha <= ?
      ${filtroLechero}
    GROUP BY p.id, p.nombre, re.fecha
    ORDER BY re.fecha DESC, p.nombre ASC
  `;

  const params = [rutaId, fechaHasta, fechaHasta];
  if (filtroLechero) {
    params.push(lecheroId);
  }

  const [rows] = await pool.execute(query, params);

  return res.json({
    diasMaximos: REGISTROS_RETENCION_DIAS,
    fechaHasta,
    fechaDesde: new Date(new Date(`${fechaHasta}T00:00:00`).getTime() - ((REGISTROS_RETENCION_DIAS - 1) * 24 * 60 * 60 * 1000))
      .toISOString()
      .slice(0, 10),
    registros: rows.map((row) => ({
      lecheroId: Number(row.lecheroId),
      lechero: row.lechero,
      fecha: row.fecha,
      litros: Number(row.litros || 0),
    })),
  });
}));

router.get('/registros/hoy', asyncHandler(async (req, res) => {
  const rutaId = Number(req.query.rutaId);
  const fecha = req.query.fecha;

  if (!rutaId) {
    return res.status(400).json({ message: 'rutaId es obligatorio' });
  }
  if (!fecha) {
    return res.status(400).json({ message: 'fecha es obligatoria, ejemplo: 2026-03-21' });
  }

  const tableName = await getCatalogTableName();
  const fkLechero = await getEntregaLecheroColumn();

  const query = `
    SELECT
      re.id AS serverId,
      re.${fkLechero} AS lecheroId,
      re.trabajador_id AS trabajadorId,
      DATE_FORMAT(re.fecha, '%Y-%m-%d') AS fecha,
      re.litros_entregados AS litrosEntregados,
      re.dedupe_key AS dedupeKey
    FROM registros_entrega re
    INNER JOIN (
      SELECT MAX(id) AS id
      FROM registros_entrega
      WHERE fecha = ?
      GROUP BY ${fkLechero}, fecha
    ) latest ON latest.id = re.id
    INNER JOIN ${tableName} p ON p.id = re.${fkLechero}
    WHERE p.ruta_id = ?
      AND re.fecha = ?
    ORDER BY re.id ASC
  `;

  const [rows] = await pool.execute(query, [fecha, rutaId, fecha]);
  return res.json(rows);
}));

router.get('/pagos/semanal', asyncHandler(async (req, res) => {
  const fechaReferencia = req.query.fecha;
  const rutaId = Number(req.query.rutaId);

  if (!fechaReferencia) {
    return res.status(400).json({ message: 'fecha es obligatoria, ejemplo: 2026-03-18' });
  }

  const tableName = await getCatalogTableName();
  const fkLechero = await getEntregaLecheroColumn();

  const routeFilter = rutaId ? 'AND p.ruta_id = ?' : '';
  const query = `
    SELECT
      p.id AS lecheroId,
      p.nombre AS lechero,
      SUM(re.litros_entregados) AS totalLitros,
      COALESCE((SELECT precio_actual_litro FROM configuracion ORDER BY id ASC LIMIT 1), 0) AS precioLitro,
      SUM(re.litros_entregados) * COALESCE((SELECT precio_actual_litro FROM configuracion ORDER BY id ASC LIMIT 1), 0) AS totalPago
    FROM registros_entrega re
    INNER JOIN ${tableName} p ON p.id = re.${fkLechero}
    WHERE re.fecha >= ?
      AND re.fecha <= DATE_ADD(?, INTERVAL 6 DAY)
    ${routeFilter}
    GROUP BY p.id, p.nombre
    ORDER BY p.nombre ASC
  `;

  const params = rutaId
    ? [fechaReferencia, fechaReferencia, rutaId]
    : [fechaReferencia, fechaReferencia];
  const [rows] = await pool.execute(query, params);
  return res.json(rows);
}));

router.get('/pagos/semanal/detalle', asyncHandler(async (req, res) => {
  const fechaReferencia = req.query.fecha;
  const rutaId = Number(req.query.rutaId);

  if (!fechaReferencia) {
    return res.status(400).json({ message: 'fecha es obligatoria, ejemplo: 2026-03-18' });
  }
  if (!rutaId) {
    return res.status(400).json({ message: 'rutaId es obligatoria' });
  }

  const tableName = await getCatalogTableName();
  const fkLechero = await getEntregaLecheroColumn();

  const query = `
    SELECT
      p.id AS lecheroId,
      DATE_FORMAT(re.fecha, '%Y-%m-%d') AS fecha,
      SUM(re.litros_entregados) AS litros
    FROM registros_entrega re
    INNER JOIN ${tableName} p ON p.id = re.${fkLechero}
    WHERE re.fecha >= ?
      AND re.fecha <= DATE_ADD(?, INTERVAL 6 DAY)
      AND p.ruta_id = ?
    GROUP BY p.id, re.fecha
    ORDER BY p.id ASC, re.fecha ASC
  `;

  const [rows] = await pool.execute(query, [fechaReferencia, fechaReferencia, rutaId]);
  return res.json(rows);
}));

router.get('/configuracion/precio', asyncHandler(async (_req, res) => {
  const [rows] = await pool.query(
    'SELECT COALESCE((SELECT precio_actual_litro FROM configuracion ORDER BY id ASC LIMIT 1), 0) AS precioPorLitro'
  );

  return res.json({ precioPorLitro: Number(rows[0]?.precioPorLitro || 0) });
}));

router.put('/configuracion/precio', asyncHandler(async (req, res) => {
  const precioPorLitro = Number(req.body?.precioPorLitro);

  if (!Number.isFinite(precioPorLitro) || precioPorLitro <= 0) {
    return res.status(400).json({ message: 'precioPorLitro debe ser un numero mayor a 0' });
  }

  await pool.execute(
    `INSERT INTO configuracion (id, precio_actual_litro)
     VALUES (1, ?)
     ON DUPLICATE KEY UPDATE precio_actual_litro = VALUES(precio_actual_litro)`,
    [precioPorLitro]
  );

  return res.json({ ok: true, precioPorLitro });
}));

module.exports = router;
