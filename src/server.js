const express = require('express');
const cors = require('cors');
const apiRoutes = require('./routes/api');
const authRoutes = require('./routes/auth');
const { host, port } = require('./config');

const app = express();

app.use(cors());
app.use(express.json());
app.use('/api/auth', authRoutes);
app.use('/api', apiRoutes);

app.use((error, _req, res, _next) => {
  const status = Number(error?.status) || 500;
  res.status(status).json({ message: error?.message || 'Error interno', detail: error?.detail || error?.message });
});

app.listen(port, host, () => {
  console.log(`Backend escuchando en http://${host}:${port} (localhost: http://localhost:${port})`);
});
