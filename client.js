const net = require('net');
const fs = require('fs');

const HOST = 'localhost';
const PORT = 3000;

const PACKET_SIZE = 17; 
let receivedPackets = new Map();
let missingSequences = [];

const VALID_SYMBOLS = ['AAPL', 'MSFT', 'AMZN', 'META'];
const VALID_INDICATORS = ['B', 'S'];

function createRequestPayload(callType, resendSeq = 0) {
  const payload = Buffer.alloc(2);
  payload.writeUInt8(callType, 0);
  payload.writeUInt8(resendSeq, 1);
  return payload;
}

function parsePacket(data) {
  if (data.length !== PACKET_SIZE) {
    throw new Error(`Invalid packet size: ${data.length}`);
  }

  const packet = {
    symbol: data.slice(0, 4).toString('ascii'),
    buysellindicator: data.slice(4, 5).toString('ascii'),
    quantity: data.readInt32BE(5),
    price: data.readInt32BE(9),
    packetSequence: data.readInt32BE(13)
  };

  validatePacket(packet);
  return packet;
}

function validatePacket(packet) {
  if (!VALID_SYMBOLS.includes(packet.symbol)) {
    throw new Error(`Invalid symbol: ${packet.symbol}`);
  }
  if (!VALID_INDICATORS.includes(packet.buysellindicator)) {
    throw new Error(`Invalid buy/sell indicator: ${packet.buysellindicator}`);
  }
  if (packet.quantity <= 0) {
    throw new Error(`Invalid quantity: ${packet.quantity}`);
  }
  if (packet.price <= 0) {
    throw new Error(`Invalid price: ${packet.price}`);
  }
  if (packet.packetSequence <= 0) {
    throw new Error(`Invalid packet sequence: ${packet.packetSequence}`);
  }
}

function findMissingSequences() {
  const sequences = Array.from(receivedPackets.keys()).sort((a, b) => a - b);
  const missing = [];
  for (let i = 1; i <= sequences[sequences.length - 1]; i++) {
    if (!sequences.includes(i)) {
      missing.push(i);
    }
  }
  return missing;
}

function saveToJson() {
  const sortedPackets = Array.from(receivedPackets.values()).sort((a, b) => a.packetSequence - b.packetSequence);
  fs.writeFileSync('output.json', JSON.stringify(sortedPackets, null, 2));
  console.log('Data saved to output.json');
}

function verifyDataIntegrity() {
  const packets = Array.from(receivedPackets.values());
  const sequences = packets.map(p => p.packetSequence);
  const uniqueSequences = new Set(sequences);

  if (sequences.length !== uniqueSequences.size) {
    throw new Error('Duplicate sequence numbers detected');
  }

  const maxSequence = Math.max(...sequences);
  if (sequences.length !== maxSequence) {
    throw new Error(`Missing sequences. Expected ${maxSequence}, got ${sequences.length}`);
  }

  console.log('Data integrity check passed');
}

function startClient() {
  const client = new net.Socket();

  client.connect(PORT, HOST, () => {
    console.log('Connected to server');
    client.write(createRequestPayload(1));
  });

  let buffer = Buffer.alloc(0);

  client.on('data', (data) => {
    buffer = Buffer.concat([buffer, data]);

    while (buffer.length >= PACKET_SIZE) {
      const packetData = buffer.slice(0, PACKET_SIZE);
      buffer = buffer.slice(PACKET_SIZE);

      try {
        const packet = parsePacket(packetData);
        if (!receivedPackets.has(packet.packetSequence)) {
          receivedPackets.set(packet.packetSequence, packet);
          console.log(`Received packet with sequence: ${packet.packetSequence}`);
        }
      } catch (error) {
        console.error('Error parsing packet:', error.message);
      }
    }
  });

  client.on('close', () => {
    console.log('Connection closed');
    missingSequences = findMissingSequences();
    
    if (missingSequences.length > 0) {
      console.log(`Missing sequences: ${missingSequences.join(', ')}`);
      startClient();
    } else {
      try {
        verifyDataIntegrity();
        saveToJson();
        console.log('All packets received and saved. Process complete.');
      } catch (error) {
        console.error('Data integrity check failed:', error.message);
      }
    }
  });

  client.on('error', (err) => {
    console.error('Error:', err);
  });
}

startClient();