package transcoder

import (
	"bufio"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

type Progress struct {
	JobID   string
	Frame   int
	FPS     float64
	Speed   float64
	Bitrate string
	OutTime time.Duration
	Done    bool
}

func parseProgress(jobID string, stderr io.Reader, out chan<- Progress) {
	scanner := bufio.NewScanner(stderr)
	var cur Progress
	cur.JobID = jobID

	for scanner.Scan() {
		key, val, ok := strings.Cut(scanner.Text(), "=")
		if !ok {
			continue
		}

		switch strings.TrimSpace(key) {
		case "frame":
			cur.Frame, _ = strconv.Atoi(strings.TrimSpace(val))
		case "fps":
			cur.FPS, _ = strconv.ParseFloat(strings.TrimSpace(val), 64)
		case "speed":
			s := strings.TrimSuffix(strings.TrimSpace(val), "x")
			cur.Speed, _ = strconv.ParseFloat(s, 64)
		case "bitrate":
			cur.Bitrate = strings.TrimSpace(val)
		case "progress":
			cur.Done = strings.TrimSpace(val) == "end"
			out <- cur
			cur = Progress{JobID: jobID}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("error reading input: %v", err)
	}

}
