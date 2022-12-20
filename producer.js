import { randomUUID } from 'node:crypto';
import { Kafka } from 'kafkajs';

async function bootstrap() {
 
  const kafka = new Kafka({
    clientId: 'kafka-producer',
    brokers: ['proven-aardvark-11874-us1-kafka.upstash.io:9092'],
    sasl: {
      mechanism: 'scram-sha-256',
      username: 'cHJvdmVuLWFhcmR2YXJrLTExODc0JEPj9yzMl1sBFkEG4E1wtLCHXlTmdrMKOlE',
      password: 'I9aZ2wkg8FMxMnGjouE-iAeNBjU2QdQsPaErdIED702IuvMAJexIMNw8tA5LeCvfZempJg==',
    },
    ssl: true,
  })

  const producer = kafka.producer()

  await producer.connect()

  await producer.send({
    topic: 'notifications.send-notification',
    messages: [{
      value: JSON.stringify({
        content: 'Nova Solicitação de amizade!',
        category: 'social',
        recipientId: randomUUID()
      })
    }]
  })
  await producer.disconnect()

}

await bootstrap()