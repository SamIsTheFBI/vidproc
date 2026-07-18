package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
	"video-processing/internal/transcoder"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: server <input.mp4> <output.mp4>")
		os.Exit(1)
	}

	input := os.Args[1]
	output := os.Args[2]

	// 30min timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	log.Printf("transcoding %s → %s (3 renditions concurrently)", input, output)

	progress, errs := transcoder.TranscodeAll(ctx, input, output, transcoder.DefaultRenditions)

	for p := range progress {
		if p.Done {
			log.Printf("[%s] done", p.JobID)
		} else {
			log.Printf("[%s] frame=%d fps=%.1f speed=%.2fx bitrate=%s",
				p.JobID, p.Frame, p.FPS, p.Speed, p.Bitrate)
		}
	}

	for err := range errs {
		log.Printf("error: %v", err)
	}

	log.Println("all renditions complete")
}
