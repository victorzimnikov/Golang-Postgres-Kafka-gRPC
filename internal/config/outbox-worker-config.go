package config

type OutboxWorkerConfig struct {
	DatabaseURL            string
	KafkaBrokers           string
	KafkaOrderCreatedTopic string
}

func LoadOutboxWorkerConfig() (OutboxWorkerConfig, error) {
	databaseURL, err := requiredEnv("DATABASE_URL")
	if err != nil {
		return OutboxWorkerConfig{}, err
	}

	kafkaBrokers, err := requiredEnv("KAFKA_BROKERS")
	if err != nil {
		return OutboxWorkerConfig{}, err
	}

	kafkaOrderCreatedTopic, err := requiredEnv("KAFKA_ORDER_CREATED_TOPIC")
	if err != nil {
		return OutboxWorkerConfig{}, err
	}

	return OutboxWorkerConfig{
		DatabaseURL:            databaseURL,
		KafkaBrokers:           kafkaBrokers,
		KafkaOrderCreatedTopic: kafkaOrderCreatedTopic,
	}, nil
}
