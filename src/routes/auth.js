const express = require('express');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const pool = require('../db');
const { jwtSecret } = require('../config');

const router = express.Router();
const asyncHandler = (handler) => (req, res, next) => Promise.resolve(handler(req, res, next)).catch(next);

router.post('/login', asyncHandler(async (req, res) => {
  const { email, password } = req.body ?? {};

  if (typeof email !== 'string' || typeof password !== 'string' || !email || !password) {
    return res.status(400).json({ message: 'email y password son obligatorios' });
  }

  const [rows] = await pool.execute(
    'SELECT id, nombre, email, password_hash, rol FROM usuarios WHERE email = ? AND activo = 1 LIMIT 1',
    [email.trim().toLowerCase()]
  );

  const usuario = rows[0];

  if (!usuario) {
    return res.status(401).json({ message: 'Credenciales incorrectas' });
  }

  const passwordValida = await bcrypt.compare(password, usuario.password_hash);

  if (!passwordValida) {
    return res.status(401).json({ message: 'Credenciales incorrectas' });
  }

  const token = jwt.sign(
    { id: usuario.id, email: usuario.email, rol: usuario.rol },
    jwtSecret,
    { expiresIn: '30d' }
  );

  return res.json({
    token,
    usuario: {
      id: usuario.id,
      nombre: usuario.nombre,
      email: usuario.email,
      rol: usuario.rol
    }
  });
}));

module.exports = router;
