package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ryanjewik/sumo-central/backend/config"
	"github.com/ryanjewik/sumo-central/backend/services"
	"go.mongodb.org/mongo-driver/mongo"
)

type App struct {
	Cfg   config.Config
	Mongo *mongo.Database
	Sumo  services.SumoService
}

func NewApp(cfg config.Config, db *mongo.Database, sumo services.SumoService) *App {
	return &App{Cfg: cfg, Mongo: db, Sumo: sumo}
}

func (a *App) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
