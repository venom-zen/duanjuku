package app

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestLiveHongguoPlaybackResolution drives the production Hongguo resolution
// path end to end: app detail, then media resolution for episodes past the
// trial boundary. It is the regression check for the playback signature, since
// an upstream rejection shows up as an empty body rather than an HTTP error.
// Media addresses stay in the process; only quality and key presence are logged.
func TestLiveHongguoPlaybackResolution(t *testing.T) {
	if os.Getenv("JUKU_LIVE_HONGGUO_PROBE") != "1" {
		t.Skip("set JUKU_LIVE_HONGGUO_PROBE=1 for the Hongguo live probes")
	}
	cfg := defaultConfig()
	cfg.dataDir, cfg.OutputDir, cfg.ProxyURL = t.TempDir(), t.TempDir(), "direct"
	// Retries stays at the production default: upstream answers some single
	// requests with an empty body, and the resolution path has to absorb that.
	d := NewDownloader(cfg)
	seriesID := firstNonEmpty(strings.TrimSpace(os.Getenv("JUKU_LIVE_HONGGUO_SERIES")), "7654784942311738393")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	entry, err := d.hongguoAppDetail(ctx, seriesID)
	if err != nil {
		t.Fatalf("app detail: %v", err)
	}
	t.Logf("app detail title=%q chapters=%d", truncate(entry.Drama.DisplayTitle(), 40), len(entry.Chapters))

	resolved := 0
	for _, index := range []int{1, 4, 10, 20} {
		if index > len(entry.Chapters) {
			continue
		}
		chapter := entry.Chapters[index-1]
		videoID := strings.TrimPrefix(chapter.VideoURL, "hongguo-cenc://")
		media, resolveErr := d.resolveHongguoAppMedia(ctx, videoID)
		if resolveErr != nil {
			// Upstream empties out single videos now and then, so one miss is
			// reported rather than failed; the gate below needs a majority.
			t.Logf("episode %-3d unresolved: %v", index, resolveErr)
			continue
		}
		resolved++
		t.Logf("episode %-3d quality=%d variants=%d encrypted=%t duration=%s",
			index, media.Quality, len(media.Variants), len(media.CENCKey) > 0, media.Duration.Round(time.Millisecond))
		if media.URL == "" {
			t.Errorf("episode %d resolved without a media address", index)
		}
	}
	if resolved < 2 {
		t.Fatalf("playback signature regressed: only %d of the sampled episodes resolved", resolved)
	}

	// The web and fallback paths stay wired, so record what they still offer.
	if web, webErr := d.resolveHongguoWebMedia(ctx, seriesID, strings.TrimPrefix(entry.Chapters[0].VideoURL, "hongguo-cenc://")); webErr != nil {
		t.Logf("web media episode 1: %v", webErr)
	} else {
		t.Logf("web media episode 1 quality=%d duration=%s", web.Quality, web.Duration.Round(time.Millisecond))
	}
}
