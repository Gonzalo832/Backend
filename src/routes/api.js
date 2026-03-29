const express = require('express');
const pool = require('../db');
const { syncEntregas } = require('../services/syncService');

const router = express.Router();
const asyncHandler = (handler) => (req, res, next) => Promise.resolve(handler(req, res, next)).catch(next);

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
    if (
      typeof entrega.localId !== 'number' ||
      typeof lecheroId !== 'number' ||
      typeof entrega.fecha !== 'string' ||
      typeof entrega.litrosEntregados !== 'number' ||
      typeof entrega.dedupeKey !== 'string'
    ) {
      return res.status(400).json({ message: 'Estructura de entrega invalida' });
    }
  }

  try {
    const syncedLocalIds = await syncEntregas(entregas);
    return res.json({ syncedLocalIds });
  } catch (error) {
    return res.status(500).json({ message: 'No se pudo sincronizar', detail: error.message });
  }
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
    INNER JOIN ${tableName} p ON p.id = re.${fkLechero}
    WHERE p.ruta_id = ?
      AND re.fecha = ?
    ORDER BY re.id ASC
  `;

  const [rows] = await pool.execute(query, [rutaId, fecha]);
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
    WHERE YEARWEEK(re.fecha, 3) = YEARWEEK(?, 3)
    ${routeFilter}
    GROUP BY p.id, p.nombre
    ORDER BY p.nombre ASC
  `;

  const params = rutaId ? [fechaReferencia, rutaId] : [fechaReferencia];
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
    WHERE YEARWEEK(re.fecha, 3) = YEARWEEK(?, 3)
      AND p.ruta_id = ?
    GROUP BY p.id, re.fecha
    ORDER BY p.id ASC, re.fecha ASC
  `;

  const [rows] = await pool.execute(query, [fechaReferencia, rutaId]);
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
