// backend/router/router.go
package router

import (
	"github.com/gin-gonic/gin"
	"github.com/ryanjewik/sumopedia/backend/handlers"
)

func Register(r *gin.Engine, app *handlers.App) {
	r.GET("/health", app.Health)
	r.POST("/webhook", app.HandleWebhook)

	r.POST("/auth/register", app.Register)

	api := r.Group("/api")
	{
		api.GET("/matches", app.GetMatches) // reads from Mongo
	}
}
