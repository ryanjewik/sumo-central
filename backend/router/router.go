package router

import (
	"github.com/gin-gonic/gin"
	"github.com/ryanjewik/sumo-central/backend/handlers"
)

type Deps interface {
	// minimal interface so we don’t tie router to concrete struct
}

func Register(r *gin.Engine, deps interface{}) {
	// cast back to your struct
	d := deps.(struct {
		Cfg   interface{}
		Mongo interface{}
		Sumo  interface{}
	})
	// ^ if that feels too hacky, just pass *AppDeps directly.

	app := deps.(*handlers.App) // better: pass a concrete

	r.GET("/health", app.Health)

	api := r.Group("/api")
	{
		api.GET("/matches", app.GetMatches) // read from Mongo
	}

	r.POST("/webhook", app.HandleWebhook)
}
