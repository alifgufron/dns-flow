package file

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alifgufron/dns-flow/internal/domain"
)

type Config struct {
	Path       string
	MaxSizeMB  int
	MaxAgeDays int
	MaxBackups int
	Compress   bool
}

type Writer struct {
	cfg    Config
	logger *slog.Logger
	mu     sync.Mutex
	file   *os.File
	size   int64
	done   chan struct{}
}

func NewWriter(cfg Config, logger *slog.Logger) *Writer {
	return &Writer{
		cfg:    cfg,
		logger: logger,
		done:   make(chan struct{}),
	}
}

func (w *Writer) Name() string {
	return "file"
}

func (w *Writer) Migrate() error {
	dir := filepath.Dir(w.cfg.Path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		w.logger.Info("file: creating directory", "path", dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(w.cfg.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.file = f

	stat, err := f.Stat()
	if err == nil {
		w.size = stat.Size()
	}

	w.logger.Info("file: ready",
		"path", w.cfg.Path,
		"max_size_mb", w.cfg.MaxSizeMB,
		"compress", w.cfg.Compress,
	)
	return nil
}

func (w *Writer) Write(event domain.DNSRawEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	maxBytes := int64(w.cfg.MaxSizeMB) * 1024 * 1024
	if maxBytes > 0 && w.size >= maxBytes {
		if err := w.rotate(); err != nil {
			w.logger.Error("file: rotation failed", "error", err)
		}
	}

	data = append(data, '\n')
	n, err := w.file.Write(data)
	if err != nil {
		return err
	}
	w.size += int64(n)

	return nil
}

func (w *Writer) rotate() error {
	if w.file == nil {
		return nil
	}

	oldPath := w.cfg.Path
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil

	ts := time.Now().UTC().Format("20060102T150405Z")
	ext := filepath.Ext(oldPath)
	base := strings.TrimSuffix(oldPath, ext)
	backupPath := fmt.Sprintf("%s.%s%s", base, ts, ext)

	if err := os.Rename(oldPath, backupPath); err != nil {
		return err
	}

	if w.cfg.Compress {
		if err := gzipFile(backupPath); err != nil {
			w.logger.Warn("file: compression failed", "path", backupPath, "error", err)
		}
	}

	f, err := os.OpenFile(oldPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.size = 0

	go w.cleanup()

	return nil
}

func gzipFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	gzPath := path + ".gz"
	gzFile, err := os.Create(gzPath)
	if err != nil {
		return err
	}
	defer gzFile.Close()

	w := gzip.NewWriter(gzFile)
	if _, err := w.Write(data); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	os.Remove(path)
	return nil
}

func (w *Writer) cleanup() {
	w.mu.Lock()
	maxAge := time.Duration(w.cfg.MaxAgeDays) * 24 * time.Hour
	maxBackups := w.cfg.MaxBackups
	w.mu.Unlock()

	if maxAge <= 0 && maxBackups <= 0 {
		return
	}

	dir := filepath.Dir(w.cfg.Path)
	base := strings.TrimSuffix(filepath.Base(w.cfg.Path), filepath.Ext(w.cfg.Path))

	entries, _ := os.ReadDir(dir)
	var backups []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, base+".") {
			continue
		}
		if w.cfg.Compress && !strings.HasSuffix(name, ".gz") {
			continue
		}
		ext := filepath.Ext(name)
		if ext == ".gz" {
			name = strings.TrimSuffix(name, ".gz")
		}
		parts := strings.SplitN(name, ".", 2)
		if len(parts) != 2 {
			continue
		}
		backups = append(backups, e.Name())
	}

	sort.Sort(sort.Reverse(sort.StringSlice(backups)))

	now := time.Now()
	for i, name := range backups {
		if maxBackups > 0 && i >= maxBackups {
			os.Remove(filepath.Join(dir, name))
			continue
		}
		if maxAge <= 0 {
			continue
		}
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		if now.Sub(info.ModTime()) > maxAge {
			os.Remove(filepath.Join(dir, name))
		}
	}
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		w.file.Close()
		w.file = nil
	}

	w.logger.Info("file: closed")
	close(w.done)
	return nil
}
