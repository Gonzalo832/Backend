const pool = require('../db');

async function resolveLecheroColumn(conn) {
  const [lecheroColumn] = await conn.query("SHOW COLUMNS FROM registros_entrega LIKE 'lechero_id'");
  if (lecheroColumn.length === 0) {
    throw new Error('La tabla registros_entrega debe tener la columna lechero_id');
  }

  return 'lechero_id';
}

async function hasTrabajadorColumn(conn) {
  const [rows] = await conn.query("SHOW COLUMNS FROM registros_entrega LIKE 'trabajador_id'");
  return rows.length > 0;
}

async function trabajadorExists(conn, trabajadorId) {
  if (!Number.isFinite(trabajadorId) || trabajadorId <= 0) {
    return false;
  }

  const [rows] = await conn.execute(
    'SELECT id FROM trabajadores WHERE id = ? LIMIT 1',
    [trabajadorId]
  );

  return rows.length > 0;
}

async function getLecheroRutaId(conn, lecheroId) {
  const [rows] = await conn.execute(
    'SELECT ruta_id AS rutaId FROM lecheros WHERE id = ? LIMIT 1',
    [lecheroId]
  );

  if (rows.length === 0) {
    throw new Error(`El lechero ${lecheroId} no existe en servidor`);
  }

  return Number(rows[0].rutaId);
}

async function findOrCreateTrabajador(conn, rutaId) {
  const [existingRows] = await conn.execute(
    'SELECT id FROM trabajadores WHERE ruta_id = ? ORDER BY id ASC LIMIT 1',
    [rutaId]
  );

  if (existingRows.length > 0) {
    return Number(existingRows[0].id);
  }

  const [result] = await conn.execute(
    'INSERT INTO trabajadores (nombre, ruta_id) VALUES (?, ?)',
    ['General', rutaId]
  );

  return Number(result.insertId);
}

async function resolveTrabajadorId(conn, entrega, lecheroId) {
  const requestedTrabajadorId = Number(entrega.trabajadorId);

  if (await trabajadorExists(conn, requestedTrabajadorId)) {
    return requestedTrabajadorId;
  }

  const rutaId = await getLecheroRutaId(conn, lecheroId);
  return findOrCreateTrabajador(conn, rutaId);
}

async function syncEntregas(entregas) {
  const conn = await pool.getConnection();
  const syncedLocalIds = [];

  try {
    await conn.beginTransaction();
    const lecheroColumn = await resolveLecheroColumn(conn);
    const includeTrabajador = await hasTrabajadorColumn(conn);

    for (const entrega of entregas) {
      const lecheroId = entrega.lecheroId ?? entrega.productorId;
      let existingId = null;
      const dedupeKey = String(entrega.dedupeKey || '').trim();

      if (dedupeKey) {
        const [existingRows] = await conn.execute(
          'SELECT id FROM registros_entrega WHERE dedupe_key = ? LIMIT 1',
          [dedupeKey]
        );

        if (existingRows.length > 0) {
          existingId = Number(existingRows[0].id);
        }
      }

      if (!existingId) {
        const [byDayRows] = await conn.execute(
          `SELECT id
           FROM registros_entrega
           WHERE ${lecheroColumn} = ? AND fecha = ?
           ORDER BY id DESC
           LIMIT 1`,
          [lecheroId, entrega.fecha]
        );

        if (byDayRows.length > 0) {
          existingId = Number(byDayRows[0].id);
        }
      }

      if (!existingId) {
        if (includeTrabajador) {
          const trabajadorId = await resolveTrabajadorId(conn, entrega, lecheroId);
          await conn.execute(
            `INSERT INTO registros_entrega (
              ${lecheroColumn},
              trabajador_id,
              fecha,
              litros_entregados,
              dedupe_key
            ) VALUES (?, ?, ?, ?, ?)`,
            [
              lecheroId,
              trabajadorId,
              entrega.fecha,
              entrega.litrosEntregados,
              dedupeKey || null
            ]
          );
        } else {
          await conn.execute(
            `INSERT INTO registros_entrega (
              ${lecheroColumn},
              fecha,
              litros_entregados,
              dedupe_key
            ) VALUES (?, ?, ?, ?)`,
            [
              lecheroId,
              entrega.fecha,
              entrega.litrosEntregados,
              dedupeKey || null
            ]
          );
        }
      } else if (includeTrabajador) {
        const trabajadorId = await resolveTrabajadorId(conn, entrega, lecheroId);
        await conn.execute(
          `UPDATE registros_entrega
           SET ${lecheroColumn} = ?,
               trabajador_id = ?,
               fecha = ?,
               litros_entregados = ?,
               dedupe_key = ?
           WHERE id = ?`,
          [
            lecheroId,
            trabajadorId,
            entrega.fecha,
            entrega.litrosEntregados,
            dedupeKey || null,
            existingId
          ]
        );
      } else {
        await conn.execute(
          `UPDATE registros_entrega
           SET ${lecheroColumn} = ?,
               fecha = ?,
               litros_entregados = ?,
               dedupe_key = ?
           WHERE id = ?`,
          [
            lecheroId,
            entrega.fecha,
            entrega.litrosEntregados,
            dedupeKey || null,
            existingId
          ]
        );
      }

      await conn.execute(
        `DELETE FROM registros_entrega
         WHERE ${lecheroColumn} = ? AND fecha = ? AND id <> (
           SELECT id_keep FROM (
             SELECT MAX(id) AS id_keep
             FROM registros_entrega
             WHERE ${lecheroColumn} = ? AND fecha = ?
           ) t
         )`,
        [lecheroId, entrega.fecha, lecheroId, entrega.fecha]
      );

      syncedLocalIds.push(entrega.localId);
    }

    await conn.commit();
    return syncedLocalIds;
  } catch (error) {
    await conn.rollback();
    throw error;
  } finally {
    conn.release();
  }
}

module.exports = {
  syncEntregas
};
