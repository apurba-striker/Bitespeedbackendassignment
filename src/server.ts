// src/server.ts
import express from 'express';
import { IdentityService, IdentifyRequest } from './identityServcise';
import { pool } from './database';

const app = express();
const port = process.env.PORT || 3000;
const identityService = new IdentityService();

app.use(express.json());

app.post('/identify', async (req, res) => {
  try {
    const request: IdentifyRequest = req.body;
    
    // Validate request
    if (!request.email && !request.phoneNumber) {
      return res.status(400).json({ 
        error: 'Either email or phoneNumber must be provided' 
      });
    }

    const response = await identityService.identify(request);
    res.json(response);
  } catch (error) {
    console.error('Error in /identify:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check endpoint
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'healthy' });
  } catch (error) {
    res.status(500).json({ status: 'unhealthy' });
  }
});

app.listen(port, () => {
  console.log(`Bitespeed Identity Service running on port ${port}`);
});
