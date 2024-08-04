const  {Kafka} =require('kafkajs');
const kafka=new Kafka({
    clientId: 'chat-app',
    brokers:["192.168.4.153:9092"]
});
async function init() {
    const admin = kafka.admin();
    console.log("Admin connecting...");
    admin.connect();
    console.log("Aedming Connction Success...");
  
    console.log("Creating Topic [rider-updates]");
    await admin.createTopics({
      topics: [
        {
          topic: "chat-app",
          numPartitions: 2,
        },
      ],
    });
    console.log("Topic Created Success [rider-updates]");
  
    console.log("Disconnecting Admin..");
    await admin.disconnect();
  }
init();
