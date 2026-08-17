package transcoder

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

type Rendition struct {
	Name    string // "720p"
	Width   int
	Height  int
	Bitrate string // "2500k"
}

var DefaultRenditions = []Rendition{
	{Name: "1080p", Width: 1920, Height: 1080, Bitrate: "5000k"},
	{Name: "720p", Width: 1280, Height: 720, Bitrate: "2500k"},
	{Name: "480p", Width: 854, Height: 480, Bitrate: "1000k"},
}

func TranscodeAll(ctx context.Context, inputPath string, outputDir string, renditions []Rendition) (<-chan Progress, <-chan error) {
	progress := make(chan Progress, 10)
	errs := make(chan error, len(renditions))

	var wg sync.WaitGroup

	for _, r := range renditions {
		wg.Add(1)
		go func(r Rendition) {
			defer wg.Done()

			outputPath := filepath.Join(outputDir, filepath.Base(inputPath), fmt.Sprintf("output_%s.mp4", r.Name))

			if err := transcodeOne(ctx, inputPath, outputPath, r, progress); err != nil {
				errs <- fmt.Errorf("rendition %s: %w", r.Name, err)
			}
		}(r)
	}

	go func() {
		wg.Wait()
		close(progress)
		close(errs)
	}()

	return progress, errs
}

func transcodeOne(ctx context.Context, inputPath string, outputPath string, r Rendition, out chan<- Progress) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y", // overwrite output without asking
		"-i", inputPath,
		"-vf", fmt.Sprintf("scale=%d:%d", r.Width, r.Height),
		"-c:v", "libx264",
		"-preset", "fast",
		"-b:v", r.Bitrate,
		"-c:a", "aac",
		"-b:a", "128k",
		"-progress", "pipe:2", // write progress key=values to stderr
		"-nostats", // suppress the default stats output
		outputPath,
	)

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	log.Printf("[ffmpeg/%s] running: ffmpeg %v", r.Name, cmd.Args)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start ffmpeg: %w", err)
	}

	parseDone := make(chan struct{})
	go func() {
		defer close(parseDone)
		parseProgress(r.Name, stderrPipe, out)
	}()

	waitErr := cmd.Wait()
	<-parseDone

	return waitErr
}
