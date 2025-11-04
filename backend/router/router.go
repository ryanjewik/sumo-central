// backend/router/router.go
package router

import (
	"github.com/gin-gonic/gin"
	"github.com/ryanjewik/sumopedia/backend/handlers"
)

func Register(r *gin.Engine, app *handlers.App) {
	r.GET("/health", app.Health)
	r.POST("/webhook", app.HandleWebhook)
	// Homepage route - returns the homepage document stored in Mongo
	r.GET("/", app.Home)
	// also expose under /homepage for explicit API consumer
	r.GET("/homepage", app.Home)

	// Rikishi and Basho individual pages (fetch by `id` field in Mongo)
	r.GET("/rikishi/:id", app.GetRikishi)
	r.GET("/basho/:id", app.GetBasho)

	// Authentication
	r.POST("/auth/register", app.Register)
	r.POST("/auth/login", app.Login)
	r.GET("/auth", app.Auth)
	r.POST("/auth/refresh", app.Refresh)
	r.POST("/auth/logout", app.Logout)

	// Protected actions (require JWT)
	r.POST("/matches/:id/vote", app.JWTMiddleware(), app.Vote)

	//r.POST("/auth/register", app.Register)

	// api := r.Group("/api")
	// {
	// 	api.GET("/matches", app.GetMatches) // reads from Mongo
	// }
}
