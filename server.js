const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const { Kafka, Partitioners, logLevel } = require('kafkajs');

const PORT = process.env.PORT || 3000;
const BROKER = process.env.KAFKA_BROKER || 'localhost:9094';
const TOPIC = 'chat';
const NUM_PARTITIONS = 2;
const USERS = ['user1', 'user2'];

// Hash userID kiểu Java String.hashCode() rồi lấy modulo số partition.
// user1 -> partition 0, user2 -> partition 1.
function hashUserId(userId) {
  let h = 0;
  for (let i = 0; i < userId.length; i++) {
    h = (Math.imul(31, h) + userId.charCodeAt(i)) | 0;
  }
  return h;
}

function partitionFor(userId) {
  return Math.abs(hashUserId(userId)) % NUM_PARTITIONS;
}

const kafka = new Kafka({ clientId: 'web-chat', brokers: [BROKER], logLevel: logLevel.WARN });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
// Group id mới mỗi lần khởi động + fromBeginning để nạp lại lịch sử chat
const consumer = kafka.consumer({ groupId: `web-chat-${Date.now()}` });

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const history = [];

app.use(express.static(path.join(__dirname, 'public')));
app.get('/api/users', (req, res) => {
  res.json(USERS.map((id) => ({ id, hash: hashUserId(id), partition: partitionFor(id) })));
});

io.on('connection', (socket) => {
  socket.emit('history', history);

  socket.on('send', async ({ userId, text }, ack) => {
    if (!USERS.includes(userId) || typeof text !== 'string' || !text.trim()) {
      return ack?.({ ok: false, error: 'invalid message' });
    }
    const partition = partitionFor(userId);
    try {
      const [meta] = await producer.send({
        topic: TOPIC,
        messages: [{
          key: userId,
          partition,
          value: JSON.stringify({ userId, text: text.trim(), sentAt: Date.now() }),
        }],
      });
      console.log(`[produce] ${userId} -> partition ${meta.partition}, offset ${meta.baseOffset}`);
      ack?.({ ok: true, partition: meta.partition, offset: meta.baseOffset });
    } catch (err) {
      console.error('[produce] error', err);
      ack?.({ ok: false, error: err.message });
    }
  });
});

async function main() {
  const admin = kafka.admin();
  await admin.connect();
  const [topicMeta] = (await admin.fetchTopicMetadata({ topics: [TOPIC] })).topics;
  console.log(`Topic "${TOPIC}" có ${topicMeta.partitions.length} partition`);
  await admin.disconnect();

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const msg = { ...JSON.parse(message.value.toString()), partition, offset: message.offset };
      console.log(`[consume] partition ${partition}, offset ${message.offset}: ${msg.userId}: ${msg.text}`);
      history.push(msg);
      io.emit('message', msg);
    },
  });

  server.listen(PORT, () => {
    console.log(`Web chat: http://localhost:${PORT}`);
    USERS.forEach((u) => console.log(`  ${u}: hash=${hashUserId(u)} -> partition ${partitionFor(u)}`));
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    await Promise.allSettled([consumer.disconnect(), producer.disconnect()]);
    process.exit(0);
  });
}
