package transcriber

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Config struct {
	WhisperBin string
	ModelPath  string
}

type Segment struct {
	Start string
	End   string
	Text  string
}

type Result struct {
	Segments     []Segment
	SubtitlePath string
}

func extractAudio(ctx context.Context, videoPath, outputDir string) (string, error) {
	wavPath := filepath.Join(outputDir, "audio.wav")

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", videoPath,
		"-ar", "16000", // 16kHz sample rate — whisper requirement
		"-ac", "1", // mono
		"-c:a", "pcm_s16le", // raw 16-bit PCM
		wavPath,
	)

	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("extract audio: %w\n%s", err, out)
	}

	return wavPath, nil
}

func Transcribe(ctx context.Context, cfg Config, videoPath, outputDir string) (*Result, error) {
	wavPath, err := extractAudio(ctx, videoPath, outputDir)
	if err != nil {
		return nil, err
	}
	defer os.Remove(wavPath)

	cmd := exec.CommandContext(ctx, cfg.WhisperBin,
		"-m", cfg.ModelPath,
		"-f", wavPath,
		"-ovtt", // output VTT format
	)

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start whisper: %w", err)
	}

	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			fmt.Println("[whisper]", scanner.Text())
		}
	}()

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("whisper failed: %w", err)
	}

	vttPath := wavPath + ".vtt"
	segments, err := parseVTT(vttPath)
	if err != nil {
		return nil, fmt.Errorf("parse vtt: %w", err)
	}

	return &Result{Segments: segments, SubtitlePath: vttPath}, nil
}

func parseVTT(path string) ([]Segment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open srt: %w", err)
	}
	defer f.Close()

	var segments []Segment
	var current Segment
	var textLines []string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		switch {
		case line == "":
			if current.Start != "" {
				current.Text = strings.Join(textLines, " ")
				segments = append(segments, current)
				current = Segment{}
				textLines = nil
			}

		case strings.Contains(line, "-->"):
			// timestamp line: "00:00:01,234 --> 00:00:04,567"
			parts := strings.SplitN(line, " --> ", 2)
			if len(parts) == 2 {
				current.Start = parts[0]
				current.End = parts[1]
			}

		case isNumeric(line):
			// sequence number — skip it

		default:
			// subtitle text — may be multiple lines
			textLines = append(textLines, line)
		}
	}

	// flush last segment if file doesn't end with blank line
	if current.Start != "" {
		current.Text = strings.Join(textLines, " ")
		segments = append(segments, current)
	}

	return segments, scanner.Err()
}

func isNumeric(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

func EmbedSubtitles(ctx context.Context, videoPath, srtPath, outputPath string) error {
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", videoPath,
		"-i", srtPath,
		"-c", "copy",
		"-c:s", "mov_text",
		"-metadata:s:s:0", "language=eng",
		outputPath,
	)

	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("embed subtitles: %w\n%s", err, out)
	}

	return nil
}
