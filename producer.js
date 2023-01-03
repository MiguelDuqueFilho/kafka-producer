import { randomUUID } from 'node:crypto';
import { Kafka , Partitioners} from 'kafkajs';

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

  const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    allowAutoTopicCreation: false,
    transactionTimeout: 30000
  })

  await producer.connect()

  const xmlMessage = 
  `<?xml version="1.0" encoding="UTF-8"?>
<DOC xmlns="http://www.bcb.gov.br/SPB/BMC0004.xsd" >
  <BCMSG>
    <IdentdEmissor>00000001</IdentdEmissor>
   <IdentdDestinatario>00000002</IdentdDestinatario>
    <IdentdContg>00000003</IdentdContg>
    <IdentdOperad>00000004</IdentdOperad>
    <IdentdOperadConfc>1</IdentdOperadConfc>
    <Grupo_Seq>
      <NumSeq>176</NumSeq>
      <IndrCont>1</IndrCont>
    </Grupo_Seq>
    <DomSist>55555</DomSist>
    <NUOp>12345678901234567890123</NUOp>
  </BCMSG>
  <SISMSG>
    <BMC0004>
      <CodMsg>BMC0004</CodMsg>
      <NumCtrlBMC>88888888</NumCtrlBMC>
      <ISPBIF>99999999</ISPBIF>
      <Grupo_BMC0004_OpContrd>
        <CodMoeda>176</CodMoeda>
        <DtLiquid>2022-08-13</DtLiquid>
        <Grupo_BMC0004_OpInterbanc>
       <NumCtrlBMCContrd>12345678</NumCtrlBMCContrd>
          <VlrMN>10</VlrMN>
          <VlrME>10</VlrME>
          <TaxCam>10</TaxCam>
          <ISPBIFCtrapart>12345678</ISPBIFCtrapart>
          <TpOpCAM>1</TpOpCAM>
        </Grupo_BMC0004_OpInterbanc>
        <VlrTotCompraMN>10</VlrTotCompraMN>
        <VlrTotCompraME>10</VlrTotCompraME>
        <VlrTotVendaMN>10</VlrTotVendaMN>
        <VlrTotVendaME>10</VlrTotVendaME>
      </Grupo_BMC0004_OpContrd>
      <DtMovto>2022-08-13</DtMovto>
    </BMC0004>
  </SISMSG>
  <USERMSG>String</USERMSG>
</DOC>
`;

  await producer.send({
    topic: 'message_send_to_service',
    messages: [
      {
        key: randomUUID(),
        value: JSON.stringify({
          messageId: randomUUID(),
          codMsg: 'BMC0004',
          xmlMessage,
        }),
      },
    ],
  })
  await producer.disconnect()

}

await bootstrap()