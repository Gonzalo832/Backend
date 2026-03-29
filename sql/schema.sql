-- (Opcional) Crear y usar la base de datos si lo vas a correr desde cero
CREATE DATABASE IF NOT EXISTS queseria;
USE queseria;

-- 1. Tablas independientes (no dependen de otras llaves)
CREATE TABLE rutas (
    id INT NOT NULL AUTO_INCREMENT,
    nombre VARCHAR(120) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY (nombre)
);

CREATE TABLE configuracion (
    id INT NOT NULL AUTO_INCREMENT,
    precio_actual_litro DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE usuarios (
    id INT NOT NULL AUTO_INCREMENT,
    nombre VARCHAR(120) NOT NULL,
    email VARCHAR(180) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    rol ENUM('admin','trabajador') NOT NULL DEFAULT 'trabajador',
    activo TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY (email)
);

-- 2. Tablas que dependen de la tabla 'rutas'
CREATE TABLE lecheros (
    id INT NOT NULL AUTO_INCREMENT,
    nombre VARCHAR(120) NOT NULL,
    ruta_id INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (ruta_id) REFERENCES rutas(id)
);

CREATE TABLE trabajadores (
    id INT NOT NULL AUTO_INCREMENT,
    nombre VARCHAR(120) NOT NULL,
    ruta_id INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (ruta_id) REFERENCES rutas(id)
);

-- 3. Tabla de transacciones (depende de trabajadores y lecheros)
CREATE TABLE registros_entrega (
    id BIGINT NOT NULL AUTO_INCREMENT,
    lechero_id INT NOT NULL,
    trabajador_id INT NOT NULL,
    fecha DATE NOT NULL,
    litros_entregados DECIMAL(10,2) NOT NULL,
    dedupe_key VARCHAR(80) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY (dedupe_key),
    FOREIGN KEY (trabajador_id) REFERENCES trabajadores(id),
    FOREIGN KEY (lechero_id) REFERENCES lecheros(id)
);