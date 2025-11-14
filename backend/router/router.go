// backend/router/router.go
package router

import (
	"github.com/gin-gonic/gin"
	"github.com/ryanjewik/sumopedia/backend/handlers"
)

func Register(r *gin.Engine, app *handlers.App) {
	r.GET("/health", app.Health)
	r.POST("/webhook", app.HandleWebhook)
	// in your router setup
	r.GET("/webhook", func(c *gin.Context) { c.String(200, "ok") })
	r.HEAD("/webhook", func(c *gin.Context) { c.Status(200) })

	// Homepage route - returns the homepage document stored in Mongo
	r.GET("/", app.Home)
	// also expose under /homepage for explicit API consumer
	r.GET("/homepage", app.Home)
	// expose a dedicated upcoming document (written by background jobs)
	// Accept both /upcoming and /api/upcoming so frontend rewrites work
	r.GET("/upcoming", app.Upcoming)
	r.GET("/api/upcoming", app.Upcoming)
	r.GET("/leaderboard", app.Leaderboard)

	// Rikishi and Basho listing endpoints (Postgres)
	r.GET("/rikishi", app.ListRikishi)
	r.GET("/basho", app.ListBasho)

	// Rikishi and Basho individual pages (fetch by `id` field in Mongo)
	r.GET("/rikishi/:id", app.GetRikishi)
	r.GET("/basho/:id", app.GetBasho)

	// User profile endpoint
	r.GET("/users/:id", app.GetUser)
	r.GET("/api/users/:id", app.GetUser)

	// Authenticated current user routes
	r.GET("/users/me", app.JWTMiddleware(), app.GetUser)
	r.GET("/api/users/me", app.JWTMiddleware(), app.GetUser)
	r.PATCH("/api/users/me", app.JWTMiddleware(), app.UpdateMe)
	// Also accept PATCH on /users/me since the Next.js dev proxy may strip the
	// `/api` prefix when forwarding requests. Registering both avoids 404s
	// when the frontend is configured to proxy `/api/*` -> backend without
	// preserving the `/api` path.
	r.PATCH("/users/me", app.JWTMiddleware(), app.UpdateMe)

	// Authentication
	r.POST("/auth/register", app.Register)
	r.POST("/auth/login", app.Login)
	r.GET("/auth", app.Auth)
	r.POST("/auth/refresh", app.Refresh)
	r.POST("/auth/logout", app.Logout)

	// Protected actions (require JWT)
	r.POST("/matches/:id/vote", app.JWTMiddleware(), app.Vote)
}
