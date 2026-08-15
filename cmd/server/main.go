package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"video-processing/internal/queue"
)

func main() {
	store, err := queue.NewStore("./videopipeline.db")
	if err != nil {
		log.Fatalf("open store: %v", err)
	}

	q := queue.New(store)

	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		log.Fatalf("start queue: %v", err)
	}

	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: server <input.mp4> <output.mp4>")
		os.Exit(1)
	}

	input := os.Args[1]
	output := os.Args[2]

	jobID, err := q.Submit(input, output)
	if err != nil {
		log.Fatalf("submit: %v", err)
	}
	log.Printf("submitted job %s", jobID)

	// Block until shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down gracefully...")
	q.Shutdown()
	log.Println("done")

	// // 30min timeout
	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	// defer cancel()

	// log.Printf("transcoding %s → %s (3 renditions concurrently)", input, output)

	// progress, errs := transcoder.TranscodeAll(ctx, input, output, transcoder.DefaultRenditions)

	// for p := range progress {
	// 	if p.Done {
	// 		log.Printf("[%s] done", p.JobID)
	// 	} else {
	// 		log.Printf("[%s] frame=%d fps=%.1f speed=%.2fx bitrate=%s",
	// 			p.JobID, p.Frame, p.FPS, p.Speed, p.Bitrate)
	// 	}
	// }

	// for err := range errs {
	// 	log.Printf("error: %v", err)
	// }

	// log.Println("all renditions complete")
}
