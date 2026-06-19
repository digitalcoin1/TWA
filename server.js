const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const Redis = require('ioredis');

const app = express();
const server = http.createServer(app);

// Production-ready Socket.io config
const io = new Server(server, {
    cors: { origin: "*" } 
});

const PORT = process.env.PORT || 3000;
const COOLDOWN_SECONDS = 1;

// Use the environment variable provided by Render/Upstash
const redisClient = new Redis(process.env.REDIS_URL); 

redisClient.on('connect', () => console.log('📦 Connected to Cloud Redis successfully.'));
app.use(express.static('public'));

// --- ENGINE: Monikers ---
const ADJECTIVES = ['Crypto', 'Cyber', 'Neon', 'Quantum', 'Pixel', 'Sonic', 'Matrix', 'Aero', 'Turbo', 'Shadow', 'Solar', 'Lunar', 'Alpha', 'Beta'];
const NOUNS = ['Hiker', 'Rider', 'Runner', 'Gamer', 'Phantom', 'Wizard', 'Glitch', 'Node', 'Vortex', 'Echo', 'Surfer', 'Knight', 'Ranger', 'Rover'];

function generateUsername(ip) {
    if (ip === '::1' || ip === '127.0.0.1' || ip.includes('::ffff:127.0.0.1')) return 'TWA Developer/Akhtar';
    let hash = 0;
    for (let i = 0; i < ip.length; i++) hash = ip.charCodeAt(i) + ((hash << 5) - hash);
    hash = Math.abs(hash);
    return `${ADJECTIVES[hash % ADJECTIVES.length]}${NOUNS[hash % NOUNS.length]}${(hash % 899) + 100}`;
}

// --- SOCKET LOGIC ---
io.on('connection', async (socket) => {
    const userIp = socket.handshake.address;
    socket.username = generateUsername(userIp);
    
    const defaultRoom = "Channel 1";
    socket.join(defaultRoom);
    await redisClient.sadd(`active_users:${defaultRoom}`, socket.username);

    const history = await redisClient.lrange(`room:history:${defaultRoom}`, 0, -1);
    socket.emit('room history', history.map(msg => JSON.parse(msg)).reverse());
    io.to(defaultRoom).emit('update user count', { room: defaultRoom, count: await redisClient.scard(`active_users:${defaultRoom}`) });

    socket.on('join room', async (newRoom) => {
        socket.rooms.forEach(async (room) => {
            if (room !== socket.id) {
                socket.leave(room);
                await redisClient.srem(`active_users:${room}`, socket.username);
                io.to(room).emit('update user count', { room, count: await redisClient.scard(`active_users:${room}`) });
            }
        });
        socket.join(newRoom);
        await redisClient.sadd(`active_users:${newRoom}`, socket.username);
        const newHistory = await redisClient.lrange(`room:history:${newRoom}`, 0, -1);
        socket.emit('room history', newHistory.map(msg => JSON.parse(msg)).reverse());
        io.to(newRoom).emit('update user count', { room: newRoom, count: await redisClient.scard(`active_users:${newRoom}`) });
    });

    socket.on('chat message', async (data) => {
        const { room, text } = data;
        if (await redisClient.get(`cooldown:${userIp}`)) return socket.emit('system error', 'Spam block active.');

        const payload = { room, text, user: socket.username };
        await redisClient.lpush(`room:history:${room}`, JSON.stringify(payload));
        await redisClient.ltrim(`room:history:${room}`, 0, 49);
        io.to(room).emit('chat message', payload);
        await redisClient.set(`cooldown:${userIp}`, 'true', 'EX', COOLDOWN_SECONDS);
    });

    socket.on('disconnect', async () => {
        const rooms = Array.from(socket.rooms);
        for (const room of rooms) {
            if (room !== socket.id) {
                await redisClient.srem(`active_users:${room}`, socket.username);
                const count = await redisClient.scard(`active_users:${room}`);
                io.to(room).emit('update user count', { room: room, count: count });
            }
        }
    });
});

server.listen(PORT, () => console.log(`TWA Server running on port ${PORT}`));