package main

import (
	"context"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
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

	// ----- Redis -----
	var redisClient *redis.Client
	if cfg.RedisURL != "" {
		opt, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			log.Println("⚠️ invalid REDIS_URL, falling back to localhost:6379", err)
			redisClient = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		} else {
			redisClient = redis.NewClient(opt)
		}
	} else {
		redisClient = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	}
	// quick ping
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Println("⚠️ could not connect to redis:", err)
	} else {
		log.Println("✅ connected to redis")
		// attach to app for handlers to use
		app.Redis = redisClient
	}

	// Start background maintenance tasks (cleanup old refresh tokens every 24h, keep 30 day window)
	services.StartCleanupTicker(pgpool, 24*time.Hour, 30)

	// Start votes reconciler (persist Redis SOT -> Postgres periodically)
	// Interval configurable: default 30s
	reconcileInterval := 30 * time.Second
	reconcileCtx, reconcileCancel := context.WithCancel(context.Background())
	defer reconcileCancel()
	handlers.StartReconciler(app, reconcileInterval, reconcileCtx)

	r := gin.Default()
	// Trust the reverse proxy (nginx) which will be terminating TLS and
	// forwarding requests via the Docker network. This allows Gin to use
	// X-Forwarded-Proto and X-Forwarded-For headers correctly when behind
	// a trusted proxy. In production you may want to restrict this to the
	// proxy's IP range instead of 0.0.0.0/0.
	if err := r.SetTrustedProxies([]string{"0.0.0.0/0"}); err != nil {
		log.Println("⚠️ could not set trusted proxies:", err)
	}
	router.Register(r, app)

	port := cfg.GinPort
	if port == "" {
		port = "8080"
	}
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}
