package config

type OrderServiceConfig struct {
	DatabaseURL string
	GRPCPort    string
}

func LoadOrderServiceConfig() (OrderServiceConfig, error) {
	databaseURL, err := requiredEnv("DATABASE_URL")
	if err != nil {
		return OrderServiceConfig{}, err
	}

	gRPCPort, err := requiredEnv("GRPC_PORT")
	if err != nil {
		return OrderServiceConfig{}, err
	}

	return OrderServiceConfig{
		DatabaseURL: databaseURL,
		GRPCPort:    gRPCPort,
	}, nil
}
