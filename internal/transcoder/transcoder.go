package transcoder

import (
	"context"
	"fmt"
	"io"
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

			if err := transcodeHLS(ctx, inputPath, outputDir, r, progress); err != nil {
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

func transcodeHLS(ctx context.Context, inputPath, outputDir string, r Rendition, out chan<- Progress) error {
	renditionDir := filepath.Join(outputDir, r.Name)
	if err := os.MkdirAll(renditionDir, 0755); err != nil {
		return fmt.Errorf("create rendition dir: %w", err)
	}

	playlistPath := filepath.Join(renditionDir, "index.m3u8")
	segmentPattern := filepath.Join(renditionDir, "segment_%03d.ts")

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", inputPath,
		"-vf", fmt.Sprintf("scale=%d:%d", r.Width, r.Height),
		"-c:v", "libx264",
		"-preset", "fast",
		"-b:v", r.Bitrate,
		"-c:a", "aac",
		"-b:a", "128k",
		// HLS specific flags
		"-f", "hls",
		"-hls_time", "6", // 6-second segments
		"-hls_playlist_type", "vod", // VOD = all segments known upfront
		"-hls_segment_filename", segmentPattern,
		"-progress", "pipe:2",
		"-nostats",
		playlistPath,
	)

	log.Printf("[ffmpeg/%s] running HLS transcode", r.Name)

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipes: %w", err)
	}

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

func WriteMasterPlaylist(outputDir string, renditions []Rendition, srtPath string) error {
	f, err := os.Create(filepath.Join(outputDir, "master.m3u8"))
	if err != nil {
		return fmt.Errorf("create master playlist: %w", err)
	}
	defer f.Close()

	fmt.Fprintln(f, "#EXTM3U")
	fmt.Fprintln(f, "#EXT-X-VERSION:3")

	if srtPath != "" {
		fmt.Fprintf(f, "#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"English\",DEFAULT=YES,AUTOSELECT=YES,URI=\"subtitles.m3u8\"\n")
	}

	// declare each rendition stream
	bandwidths := map[string]int{
		"1080p": 5500000,
		"720p":  2750000,
		"480p":  1100000,
	}
	resolutions := map[string]string{
		"1080p": "1920x1080",
		"720p":  "1280x720",
		"480p":  "854x480",
	}

	for _, r := range renditions {
		bw := bandwidths[r.Name]
		res := resolutions[r.Name]
		if srtPath != "" {
			fmt.Fprintf(f, "#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%s,SUBTITLES=\"subs\"\n", bw, res)
		} else {
			fmt.Fprintf(f, "#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%s\n", bw, res)
		}
		fmt.Fprintf(f, "%s/index.m3u8\n", r.Name)
	}

	return nil
}

func WriteSubtitlePlaylist(outputDir, vttPath string) error {
	if err := copyFile(vttPath, filepath.Join(outputDir, "subtitles.vtt")); err != nil {
		return fmt.Errorf("copy vtt: %w", err)
	}

	f, err := os.Create(filepath.Join(outputDir, "subtitles.m3u8"))
	if err != nil {
		return fmt.Errorf("create subtitle playlist: %w", err)
	}
	defer f.Close()

	fmt.Fprintln(f, "#EXTM3U")
	fmt.Fprintln(f, "#EXT-X-TARGETDURATION:99999")
	fmt.Fprintln(f, "#EXT-X-VERSION:3")
	fmt.Fprintln(f, "#EXTINF:99999,")
	fmt.Fprintln(f, "subtitles.vtt")
	fmt.Fprintln(f, "#EXT-X-ENDLIST")

	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}
