const express = require('express');
const { Kafka } = require('kafkajs');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'chat-app',
    password: '12345678',
    port: 5432,
});

const kafka = new Kafka({
    clientId: 'chat-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'chat-group' });
 producer.connect();

const runKafka = async () => {
   
    await consumer.connect();
    console.log("consumer is connected")

    await consumer.subscribe({ topic: 'chat-messages', fromBeginning: true });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const msg = JSON.parse(message.value.toString());
            const { senderId, receiverId, text } = msg;
            console.log(msg);
            await pool.query(
                'INSERT INTO messages (sender_id, receiver_id, message) VALUES ($1, $2, $3)',
                [senderId, receiverId, text]
            );
        },
    });
};


app.post('/send', async (req, res) => {
    const { senderId, receiverId, text } = req.body;
    console.log(senderId);
    console.log(receiverId);
    console.log(text);
    await producer.send({
        topic: 'chat-messages',
        messages: [{ value: JSON.stringify({ senderId, receiverId, text }) }],
    });
    await pool.query(
        'INSERT INTO users (id,username) VALUES ($1,$2)',[senderId,text]
    )
  
    res.sendStatus(200);
});

app.get('/messages', async (req, res) => {
    const { userId } = req.body;
    console.log(userId);
    runKafka();
    const result = await pool.query(
        'SELECT * FROM messages WHERE sender_id = $1 OR receiver_id = $1 ORDER BY timestamp ASC',
        [userId]
    );
    res.json(result.rows);
});

app.listen(3000, () => {
    console.log('Server is running on port 3000');
});
