USE queseria;

INSERT INTO rutas (nombre) VALUES
  ('Ruta Norte'),
  ('Ruta Sur')
ON DUPLICATE KEY UPDATE nombre = VALUES(nombre);

INSERT INTO lecheros (id, nombre, ruta_id) VALUES
  (1, 'Don Ernesto', 1),
  (2, 'Lacteos El Prado', 1),
  (3, 'Granja La Cumbre', 2),
  (4, 'Rancho Santa Fe', 2)
ON DUPLICATE KEY UPDATE nombre = VALUES(nombre), ruta_id = VALUES(ruta_id);

INSERT INTO trabajadores (id, nombre, ruta_id) VALUES
  (1, 'General Norte', 1),
  (2, 'General Sur', 2)
ON DUPLICATE KEY UPDATE nombre = VALUES(nombre), ruta_id = VALUES(ruta_id);

INSERT INTO configuracion (id, precio_actual_litro) VALUES
  (1, 11.50)
ON DUPLICATE KEY UPDATE precio_actual_litro = VALUES(precio_actual_litro);
