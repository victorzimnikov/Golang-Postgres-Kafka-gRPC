package config

type ConsumerConfig struct {
	DatabaseURL            string
	KafkaBrokers           string
	KafkaOrderCreatedTopic string
	KafkaConsumerGroup     string
}

func LoadConsumerConfig() (ConsumerConfig, error) {
	databaseURL, err := requiredEnv("DATABASE_URL")
	if err != nil {
		return ConsumerConfig{}, err
	}

	kafkaBrokers, err := requiredEnv("KAFKA_BROKERS")
	if err != nil {
		return ConsumerConfig{}, err
	}

	kafkaOrderCreatedTopic, err := requiredEnv("KAFKA_ORDER_CREATED_TOPIC")
	if err != nil {
		return ConsumerConfig{}, err
	}

	kafkaConsumerGroup, err := requiredEnv("KAFKA_CONSUMER_GROUP")
	if err != nil {
		return ConsumerConfig{}, err
	}

	return ConsumerConfig{
		DatabaseURL:            databaseURL,
		KafkaBrokers:           kafkaBrokers,
		KafkaOrderCreatedTopic: kafkaOrderCreatedTopic,
		KafkaConsumerGroup:     kafkaConsumerGroup,
	}, nil
}
