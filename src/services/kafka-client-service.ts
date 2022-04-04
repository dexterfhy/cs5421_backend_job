import { Admin, Consumer, ConsumerConfig, ConsumerRunConfig, ConsumerSubscribeTopic, GroupOverview, ITopicConfig, Kafka, KafkaConfig, Producer, ProducerConfig, ProducerRecord, RecordMetadata } from "kafkajs";

export class KafkaClientService {
    private static kafkaClientService: KafkaClientService | undefined;
    private kafka: Kafka;
    private admin: Admin;
    private producers: Producer[];
    private consumers: Consumer[];

    public static async initialize(
        kafkaConfig: KafkaConfig,
        topicConfigs: ITopicConfig[],
        producerConfigs: ProducerConfig[],
        consumerConfigs: ConsumerConfig[],
        consumerSubscribeTopics: ConsumerSubscribeTopic[]
    ): Promise<KafkaClientService> {
        if (!KafkaClientService.kafkaClientService) {
            KafkaClientService.kafkaClientService = new KafkaClientService(kafkaConfig, producerConfigs, consumerConfigs);
            await KafkaClientService.kafkaClientService.admin.connect();
            await Promise.all(KafkaClientService.kafkaClientService.producers.map(producer => producer.connect()));
            await Promise.all(KafkaClientService.kafkaClientService.consumers.map(consumer => consumer.connect()));
            const existingTopics: string[] = await KafkaClientService.kafkaClientService.listTopics();
            await KafkaClientService.kafkaClientService.createTopics(
                topicConfigs.filter(topicConfig => !existingTopics.includes(topicConfig.topic!))
            );
            await Promise.all(consumerSubscribeTopics.map((topic, index) => 
                KafkaClientService.kafkaClientService!.consumers[index].subscribe(topic)
            ));
        }
        console.log(`[Kafka Client Service - Initialize] KafkaClientService singleton instance has been initialized successfully`);
        return KafkaClientService.kafkaClientService;
    }

    public static async terminate(): Promise<void> {
        if (KafkaClientService.kafkaClientService) {
            await KafkaClientService.kafkaClientService.admin.disconnect();
            await Promise.all(KafkaClientService.kafkaClientService.producers.map(producer => producer.disconnect()));
            await Promise.all(KafkaClientService.kafkaClientService.consumers.map(consumer => consumer.disconnect()));
            KafkaClientService.kafkaClientService = undefined;
            console.log(`[Kafka Client Service - Terminate] KafkaClientService singleton instance has been terminated gracefully`);
        }
    }

    private constructor(kafkaConfig: KafkaConfig, producerConfigs: ProducerConfig[], consumerConfigs: ConsumerConfig[]) {
        this.kafka = new Kafka(kafkaConfig);
        this.admin = this.kafka.admin();
        this.producers = producerConfigs.map(this.kafka.producer.bind(this.kafka));
        this.consumers = consumerConfigs.map(this.kafka.consumer.bind(this.kafka));
    }

    public createTopics(topics: ITopicConfig[]): Promise<boolean> {
        console.log(`[Kafka Client Service - Create Topics] topics: ${JSON.stringify(topics)}`);
        return this.admin.createTopics({topics});
    }

    public deleteTopics(topics: string[]): Promise<void> {
        console.log(`[Kafka Client Service - Delete Topics] topics: ${JSON.stringify(topics)}`);
        return this.admin.deleteTopics({topics});
    }

    public listTopics(): Promise<string[]> {
        return this.admin.listTopics();
    }

    public producerSend(producerRecord: ProducerRecord, index: number): Promise<RecordMetadata[]> {
        console.log(`[Kafka Client Service - Producer Send] producerRecord: ${JSON.stringify(producerRecord)}, index: ${index}`);
        return this.producers[index].send(producerRecord);
    }

    public consumerRun(consumerRunConfig: ConsumerRunConfig, index: number): Promise<void> {
        console.log(`[Kafka Client Service - Consumer Run] consumerRunConfig: ${JSON.stringify(consumerRunConfig)}, index: ${index}`);
        return this.consumers[index].run(consumerRunConfig);
    }
}
