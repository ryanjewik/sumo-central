package config

import "os"

type Config struct {
	Port          string
	MongoURI      string
	MongoDB       string
	SumoBaseURL   string
	WebhookName   string
	WebhookSecret string
	WebhookDest   string
}

func Load() Config {
	return Config{
		Port:          os.Getenv("PORT"),
		MongoURI:      os.Getenv("MONGO_URI"),           // e.g. mongodb://localhost:27017
		MongoDB:       os.Getenv("MONGO_DB"),            // e.g. sumo
		SumoBaseURL:   os.Getenv("SUMO_BASE_URL"),       // e.g. https://sumo-api.com
		WebhookName:   os.Getenv("SUMO_WEBHOOK_NAME"),   // e.g. "sumo-central-dev"
		WebhookSecret: os.Getenv("SUMO_WEBHOOK_SECRET"), // used to verify signatures
		WebhookDest:   os.Getenv("SUMO_WEBHOOK_DEST"),   // e.g. https://your-backend.com/webhook
	}
}
