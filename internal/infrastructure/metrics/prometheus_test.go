package metrics

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestMetricsBearerAuth(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	exporter := InitMetrics(9154, "/metrics", "secret-token", logger)

	reqUnauthorized := httptest.NewRequest("GET", "/metrics", nil)
	w1 := httptest.NewRecorder()
	
	// Test unauthorized request (no token)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		expected := "Bearer " + exporter.authToken
		if authHeader != expected {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	handler.ServeHTTP(w1, reqUnauthorized)
	if w1.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 Unauthorized for missing token, got %d", w1.Code)
	}

	// Test authorized request (valid Bearer token)
	reqAuthorized := httptest.NewRequest("GET", "/metrics", nil)
	reqAuthorized.Header.Set("Authorization", "Bearer secret-token")
	w2 := httptest.NewRecorder()

	handler.ServeHTTP(w2, reqAuthorized)
	if w2.Code != http.StatusOK {
		t.Errorf("expected status 200 OK for valid Bearer token, got %d", w2.Code)
	}
}
