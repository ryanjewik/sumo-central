// backend/handlers/app.go
package handlers

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/ryanjewik/sumopedia/backend/config"
	"github.com/ryanjewik/sumopedia/backend/services"
	"go.mongodb.org/mongo-driver/mongo"
)

type App struct {
	Cfg   config.Config
	Mongo *mongo.Database
	PG    *pgxpool.Pool
	Sumo  services.SumoService
	Redis *redis.Client
}

func NewApp(cfg config.Config, mongoDB *mongo.Database, pg *pgxpool.Pool, sumo services.SumoService) *App {
	return &App{
		Cfg:   cfg,
		Mongo: mongoDB,
		PG:    pg,
		Sumo:  sumo,
	}
}
