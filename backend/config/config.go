package config

import (
	"fmt"
	"net/url"
	"os"
)

type Config struct {
	GinPort       string
	MongoURI      string
	MongoDBName   string
	DBUser        string
	DBPass        string
	DBName        string
	DBHost        string
	DBPort        string
	SumoBase      string
	WebhookName   string
	WebhookSecret string
	WebhookDest   string
}

func Load() Config {
	return Config{
		GinPort:       os.Getenv("GIN_PORT"),
		MongoURI:      os.Getenv("MONGO_URI"),
		MongoDBName:   os.Getenv("MONGO_DB_NAME"),
		DBUser:        os.Getenv("DB_USERNAME"),
		DBPass:        os.Getenv("DB_PASSWORD"),
		DBName:        os.Getenv("DB_NAME"),
		DBHost:        os.Getenv("DB_HOST"),
		DBPort:        os.Getenv("DB_PORT"),
		SumoBase:      os.Getenv("SUMO_BASE"),
		WebhookName:   os.Getenv("SUMO_WEBHOOK_NAME"),
		WebhookSecret: os.Getenv("SUMO_WEBHOOK_SECRET"),
		WebhookDest:   os.Getenv("SUMO_WEBHOOK_DEST"),
	}
}

// Build DSN for pgx
func (c Config) PostgresDSN() string {
	host := c.DBHost
	if host == "" {
		host = "localhost"
	}
	port := c.DBPort
	if port == "" {
		port = "5432"
	}
	// escape password in case it has special chars
	pwd := url.QueryEscape(c.DBPass)

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.DBUser,
		pwd,
		host,
		port,
		c.DBName,
	)
}
