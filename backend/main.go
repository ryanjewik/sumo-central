package main

import (
	"context"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ryanjewik/sumo-central/backend/config"
	"github.com/ryanjewik/sumo-central/backend/router"
	"github.com/ryanjewik/sumo-central/backend/services"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type AppDeps struct {
	Cfg   config.Config
	Mongo *mongo.Database
	Sumo  services.SumoService
}

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1) connect to mongo
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		log.Fatal("mongo connect:", err)
	}
	db := client.Database(cfg.MongoDB)

	// 2) create sumo service (for subscribe + maybe later for other calls)
	sumoSvc := services.NewSumoService(cfg.SumoBaseURL, cfg.WebhookSecret)

	deps := AppDeps{
		Cfg:   cfg,
		Mongo: db,
		Sumo:  sumoSvc,
	}

	// 3) subscribe to webhook at startup (once)
	if err := deps.Sumo.SubscribeWebhook(ctx, services.SubscribeRequest{
		Name:        cfg.WebhookName,
		Destination: cfg.WebhookDest,
		Secret:      cfg.WebhookSecret,
		Subscriptions: map[string]bool{
			"basho.created":   true,
			"match.completed": true,
			// put the hook types you want true
		},
	}); err != nil {
		log.Println("⚠️ failed to subscribe to sumo webhook:", err)
	} else {
		log.Println("✅ subscribed to sumo webhook")
	}

	// 4) gin
	r := gin.Default()
	router.Register(r, deps)

	port := cfg.Port
	if port == "" {
		port = "8080"
	}
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}
