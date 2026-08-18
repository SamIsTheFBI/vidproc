package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"video-processing/internal/queue"
)

type Handler struct {
	Queue  *queue.Queue
	Store  *queue.Store
	OutDir string
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /upload", h.handleUpload)
	mux.HandleFunc("GET /jobs/{id}", h.handleJobStatus)
	mux.HandleFunc("GET /jobs/{id}/progress", h.handleProgress)
	mux.HandleFunc("GET /videos/{id}/{rendition}", h.handleVideoServe)
	mux.HandleFunc("GET /jobs/{id}/transcript", h.handleTranscript)
	return mux
}

func (h *Handler) handleUpload(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 10<<30) // 10G limit

	file, header, err := r.FormFile("video")
	if err != nil {
		http.Error(w, "missing video field", http.StatusBadRequest)
		return
	}
	defer file.Close()

	uploadPath := filepath.Join(h.OutDir, "uploads", header.Filename)
	if err := os.MkdirAll(filepath.Dir(uploadPath), 0755); err != nil {
		http.Error(w, "could not create upload dir", http.StatusInternalServerError)
		return
	}

	dst, err := os.Create(uploadPath)
	if err != nil {
		http.Error(w, "could not create file", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		http.Error(w, "upload failed", http.StatusInternalServerError)
		return
	}

	jobID, err := h.Queue.Submit(uploadPath, filepath.Join(h.OutDir, "processed"))
	if err != nil {
		http.Error(w, "could not queue job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID})
}

func (h *Handler) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	job, err := h.Store.GetJob(id)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (h *Handler) handleVideoServe(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rendition := r.PathValue("rendition") // 1080p, 720p, 480p

	videoPath := filepath.Join(h.OutDir, "processed", id, fmt.Sprintf("output_%s.mp4", rendition))

	f, err := os.Open(videoPath)
	log.Printf("video path: %s", videoPath)
	if err != nil {
		http.Error(w, "video not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		http.Error(w, "could not stat file", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "video/mp4")
	http.ServeContent(w, r, stat.Name(), stat.ModTime(), f)
}

func (h *Handler) handleProgress(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// sse headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	// subscribe to progress updates for this job
	updates, unsubscribe := h.Queue.Subscribe(id)
	defer unsubscribe()

	for {
		select {
		case p, ok := <-updates:
			if !ok {
				// channel closed (job done or queue shut donw)
				fmt.Fprintf(w, "data: {\"done\":true}\n\n")
				flusher.Flush()
				return
			}

			data, _ := json.Marshal(p)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			// client disconnected
			return
		}
	}
}

func (h *Handler) handleTranscript(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	job, err := h.Store.GetJob(id)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	srtPath := filepath.Join(job.OutputDir, "audio.wav.srt")
	http.ServeFile(w, r, srtPath)
}
