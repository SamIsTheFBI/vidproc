package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"video-processing/internal/api"
	"video-processing/internal/queue"
	"video-processing/internal/transcriber"
)

func main() {
	outDir := "./data"
	if err := os.MkdirAll(outDir+"/uploads", 0755); err != nil {
		log.Fatalf("create dirs: %v", err)
	}

	store, err := queue.NewStore("./videopipeline.db")
	if err != nil {
		log.Fatalf("open store: %v", err)
	}

	tc := &transcriber.Config{
		WhisperBin: "/Users/samisthefbi/code/whisper.cpp/build/bin/whisper-cli",
		ModelPath:  "./Users/samisthefbi/code/whisper.cpp/models/ggml-base.en.bin",
	}

	q := queue.New(store, tc)

	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		log.Fatalf("start queue: %v", err)
	}

	h := &api.Handler{Queue: q, Store: store, OutDir: outDir}

	srv := &http.Server{
		Addr:    ":8080",
		Handler: h.Routes(),
	}

	go func() {
		log.Println("listening on :8080")
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)

	q.Shutdown()
	log.Println("done")
}
