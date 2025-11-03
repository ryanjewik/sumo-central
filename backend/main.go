package main

import (
	"context"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ryanjewik/sumopedia/backend/config"
	"github.com/ryanjewik/sumopedia/backend/handlers"
	"github.com/ryanjewik/sumopedia/backend/router"
	"github.com/ryanjewik/sumopedia/backend/services"
)

func main() {
	cfg := config.Load()

	// ----- Mongo -----
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		log.Fatal("mongo connect:", err)
	} else {
		log.Println("✅ connected to mongo")
	}
	mongoDB := mongoClient.Database(cfg.MongoDBName)

	// ----- Postgres (pgxpool) -----
	pgdsn := cfg.PostgresDSN()
	pgpool, err := pgxpool.New(context.Background(), pgdsn)
	if err != nil {
		log.Fatal("postgres connect:", err)
	}
	// optional: ping
	if err := pgpool.Ping(context.Background()); err != nil {
		log.Fatal("postgres ping:", err)
	} else {
		log.Println("✅ connected to postgres")
	}

	// ----- Sumo service -----
	sumoSvc := services.NewSumoService(cfg.SumoBase, cfg.WebhookSecret)

	// subscribe once at startup
	if err := sumoSvc.SubscribeWebhook(ctx, services.SubscribeRequest{
		Name:        cfg.WebhookName,
		Destination: cfg.WebhookDest,
		Secret:      cfg.WebhookSecret,
		Subscriptions: map[string]bool{
			"newBasho":     true,
			"matchResults": true,
			"endBasho":     true,
			"newMatches":   true,
		},
	}); err != nil {
		log.Println("⚠️ could not subscribe to sumo webhook:", err)
	} else {
		log.Println("✅ subscribed to sumo webhook")
	}

	// ----- Gin -----
	app := handlers.NewApp(cfg, mongoDB, pgpool, sumoSvc)

	r := gin.Default()
	router.Register(r, app)

	port := cfg.GinPort
	if port == "" {
		port = "8080"
	}
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}
