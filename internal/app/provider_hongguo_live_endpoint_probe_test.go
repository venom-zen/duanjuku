package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestLiveHongguoAppEndpointProbe compares the raw upstream shape of every
// Hongguo app endpoint the product depends on, using the production signing and
// transport path. It reports key names and byte counts only, never media
// addresses, so the failure layer can be told apart from an endpoint change.
func TestLiveHongguoAppEndpointProbe(t *testing.T) {
	if os.Getenv("JUKU_LIVE_HONGGUO_PROBE") != "1" {
		t.Skip("set JUKU_LIVE_HONGGUO_PROBE=1 for one signed, text-only Hongguo endpoint probe")
	}
	cfg := defaultConfig()
	cfg.dataDir, cfg.OutputDir, cfg.ProxyURL, cfg.Retries = t.TempDir(), t.TempDir(), "direct", 1
	d := NewDownloader(cfg)
	inner := d.client.Transport

	seriesID := firstNonEmpty(strings.TrimSpace(os.Getenv("JUKU_LIVE_HONGGUO_SERIES")), "7654784942311738393")
	if !hongguoNumericID.MatchString(seriesID) {
		t.Fatal("JUKU_LIVE_HONGGUO_SERIES must be a numeric Hongguo series id")
	}

	allowed := []string{
		"/reading/distribution/category/landpage/v/",
		"/novel/player/video_detail/v1/",
		"/novel/player/video_model/v1/",
		"/novel/player/multi_video_model/preload/v1",
		"/detail",
		"/player/",
	}
	type probeRecord struct {
		path      string
		status    int
		length    int
		keys      []string
		code      string
		message   string
		truncated bool
		signed    bool
		device    string
		headers   map[string][]string
	}
	var mu sync.Mutex
	var records []probeRecord

	d.client.Transport = rankingTransport(func(request *http.Request) (*http.Response, error) {
		path := request.URL.Path
		permitted := false
		for _, prefix := range allowed {
			if strings.HasPrefix(path, prefix) {
				permitted = true
				break
			}
		}
		if !permitted {
			t.Errorf("probe refused an unexpected path: host=%s path=%s", request.URL.Host, path)
			return rankingHTTPResponse(request, http.StatusForbidden, "blocked by probe"), nil
		}
		response, err := inner.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		content, readErr := io.ReadAll(io.LimitReader(response.Body, providerMaxBodyBytes))
		truncated := false
		if len(content) == providerMaxBodyBytes {
			truncated = true
		}
		response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(content))
		response.ContentLength = -1
		record := probeRecord{path: path, status: response.StatusCode, length: len(content), truncated: truncated}
		record.signed = request.Header.Get("X-Gorgon") != "" && request.Header.Get("X-Khronos") != "" && request.Header.Get("X-SS-STUB") != ""
		record.device = request.URL.Query().Get("device_id") + "/" + request.URL.Query().Get("iid")
		record.headers = liveProbeDiagnosticHeaders(response.Header)
		if readErr != nil {
			record.message = readErr.Error()
		} else if len(content) > 0 && strings.Contains(response.Header.Get("Content-Type"), "json") {
			var decoded map[string]any
			decoder := json.NewDecoder(bytes.NewReader(content))
			decoder.UseNumber()
			if err := decoder.Decode(&decoded); err == nil && decoded != nil {
				keys := make([]string, 0, len(decoded))
				for key := range decoded {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				record.keys = keys
				record.code = firstNonEmpty(mapString(decoded, "code", "status_code"), mapString(nestedMap(decoded, "BaseResp"), "StatusCode"))
				record.message = truncate(firstNonEmpty(mapString(decoded, "message", "msg", "StatusMessage"), mapString(nestedMap(decoded, "BaseResp"), "StatusMessage")), 80)
			}
		}
		mu.Lock()
		records = append(records, record)
		mu.Unlock()
		return response, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	client := d.hongguoClient()
	client.mu.Lock()
	deviceID, installID := client.state.DeviceID, client.state.InstallID
	client.mu.Unlock()
	t.Logf("probe identity device_id=%s iid=%s (random, never registered upstream)", deviceID, installID)

	report := func(label string, err error) {
		mu.Lock()
		var record *probeRecord
		if len(records) > 0 {
			record = &records[len(records)-1]
		}
		mu.Unlock()
		if record == nil {
			t.Logf("%-34s no upstream request reached the transport: %v", label, err)
			return
		}
		t.Logf("%-34s path=%s status=%d bytes=%d signed=%t device=%s keys=%v code=%q msg=%q err=%v",
			label, record.path, record.status, record.length, record.signed, record.device, record.keys, record.code, record.message, err)
		if record.length == 0 || len(record.keys) == 0 {
			for _, name := range slices.Sorted(maps.Keys(record.headers)) {
				t.Logf("%-34s   header %s: %s", label, name, strings.Join(record.headers[name], " | "))
			}
		}
	}

	call := func(label, path string, extra url.Values, payload any) (map[string]any, error) {
		result, err := d.hongguoAppRequest(ctx, http.MethodPost, path, extra, payload)
		report(label, err)
		return result, err
	}

	// Control: the catalog feed that keeps refreshing the library.
	catalogPayload := map[string]any{
		"req_scene": "default", "offset": 0, "limit": 3,
		"req_type": "only_content", "need_selector_panel": false, "client_req_type": 3,
		"session_id": "", "filter_ids": "",
		"select_items": map[string]any{
			"genre": []string{"short_play"}, "sort": []string{"online_time"}, "gender": []string{},
			"category_dim_theme": []string{}, "category_dim_role": []string{}, "category_dim_epoch": []string{},
			"online_time": []string{}, "creation_status": []string{},
		},
	}
	call("catalog-landpage(control)", "/reading/distribution/category/landpage/v/", nil, catalogPayload)

	detailResult, detailErr := call("video_detail", "/novel/player/video_detail/v1/", nil, map[string]any{"series_id": seriesID})
	var appVid string
	if detailErr == nil {
		detail := nestedMap(detailResult, "data", "video_data")
		for _, row := range anyList(detail["video_list"]) {
			video, _ := row.(map[string]any)
			if vid := mapString(video, "vid"); hongguoNumericID.MatchString(vid) {
				appVid = vid
				break
			}
		}
		t.Logf("video_detail yielded %d data keys, first vid taken=%t", len(nestedMap(detailResult, "data")), appVid != "")
	}

	// Video ids without the app layer, so app probes still run when detail is blocked.
	var webVids []string
	webBody, webErr := d.fetchProviderText(ctx, hongguoBaseURL+"/detail?series_id="+url.QueryEscape(seriesID), hongguoBaseURL+"/")
	if webErr == nil {
		page := routerLoaderMap(parseRouterData(webBody), "detail_page", "detail_")
		detail, _ := page["seriesDetail"].(map[string]any)
		for _, value := range anyList(detail["vid_list"]) {
			vid := strings.TrimSpace(fmt.Sprint(value))
			if hongguoNumericID.MatchString(vid) {
				webVids = append(webVids, vid)
			}
		}
		t.Logf("web detail seriesDetail keys=%v vid_count=%d", sortedKeys(detail), len(webVids))
	} else {
		t.Logf("web detail unavailable: %v", webErr)
	}
	if appVid == "" && len(webVids) > 0 {
		appVid = webVids[0]
	}

	if appVid != "" {
		call("video_model(current)", "/novel/player/video_model/v1/", nil, map[string]any{
			"video_id": appVid, "content_type": 1,
			"biz_param": map[string]any{"need_all_video_definition": true, "video_platform": 3},
		})
		call("multi_video_model/preload", "/novel/player/multi_video_model/preload/v1", nil, map[string]any{
			"video_ids": []string{appVid}, "content_type": 1, "dr_scene": "preload",
			"biz_param": map[string]any{"need_all_video_definition": true, "video_platform": 1024, "dr_scene": "preload"},
		})
		call("video_model(aid=1967)", "/novel/player/video_model/v1/", url.Values{"aid": {"1967"}}, map[string]any{
			"video_id": appVid, "content_type": 1,
			"biz_param": map[string]any{"need_all_video_definition": true, "video_platform": 3},
		})

		playerBody, playerErr := d.fetchProviderText(ctx, hongguoBaseURL+"/player/"+url.PathEscape(seriesID)+"/"+url.PathEscape(appVid), hongguoBaseURL+"/")
		if playerErr != nil {
			t.Logf("web player page unavailable: %v", playerErr)
		} else {
			page := routerLoaderMap(parseRouterData(playerBody), "player_", "player_page")
			info, _ := page["video_player_info"].(map[string]any)
			protected, _ := page["protectedPlayback"].(map[string]any)
			t.Logf("web player page loader keys=%v video_player_info=%d protectedPlayback=%d ld_json=%t tv_episode=%t content_url=%t public_mp4=%t",
				sortedKeys(page), len(info), len(protected),
				strings.Contains(playerBody, "application/ld+json"),
				strings.Contains(playerBody, "TVEpisode"),
				strings.Contains(playerBody, "contentUrl"),
				strings.Contains(playerBody, "/video/public/"))
		}
	} else {
		t.Log("no video id available from either detail path; app playback probes skipped")
	}

	mu.Lock()
	defer mu.Unlock()
	t.Logf("probe summary (%d upstream calls):", len(records))
	for _, record := range records {
		t.Logf("  %-46s status=%d bytes=%d empty=%t keys=%d", record.path, record.status, record.length, record.length == 0, len(record.keys))
	}
}

// liveProbeDiagnosticHeaders keeps the upstream fields that explain an empty
// body: risk control, signing, auth and trace markers.
func liveProbeDiagnosticHeaders(headers http.Header) map[string][]string {
	markers := []string{"auth", "verify", "risk", "block", "sec", "sdk", "gorgon", "khronos", "error", "trace", "server", "x-tt", "x-ss", "logid", "sign", "stub", "delivered"}
	kept := map[string][]string{}
	for name, values := range headers {
		lower := strings.ToLower(name)
		if strings.Contains(lower, "cookie") {
			continue
		}
		for _, marker := range markers {
			if strings.Contains(lower, marker) {
				snapshot := make([]string, 0, len(values))
				for _, value := range values {
					snapshot = append(snapshot, truncate(value, 120))
				}
				kept[name] = snapshot
				break
			}
		}
	}
	return kept
}

func sortedKeys(value map[string]any) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestLiveHongguoWebPlayerStructure maps the field shapes the web player page
// now exposes, because the signed app layer returns empty bodies. It reports
// types, key names and address patterns only; media addresses and their query
// values stay in the test process.
func TestLiveHongguoWebPlayerStructure(t *testing.T) {
	if os.Getenv("JUKU_LIVE_HONGGUO_PROBE") != "1" {
		t.Skip("set JUKU_LIVE_HONGGUO_PROBE=1 for the Hongguo live probes")
	}
	cfg := defaultConfig()
	cfg.dataDir, cfg.OutputDir, cfg.ProxyURL, cfg.Retries = t.TempDir(), t.TempDir(), "direct", 1
	d := NewDownloader(cfg)
	seriesID := firstNonEmpty(strings.TrimSpace(os.Getenv("JUKU_LIVE_HONGGUO_SERIES")), "7654784942311738393")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	body, err := d.fetchProviderText(ctx, hongguoBaseURL+"/detail?series_id="+url.QueryEscape(seriesID), hongguoBaseURL+"/")
	if err != nil {
		t.Fatalf("web detail: %v", err)
	}
	detail, _ := routerLoaderMap(parseRouterData(body), "detail_page", "detail_")["seriesDetail"].(map[string]any)
	var vids []string
	for _, value := range anyList(detail["vid_list"]) {
		if vid := strings.TrimSpace(fmt.Sprint(value)); hongguoNumericID.MatchString(vid) {
			vids = append(vids, vid)
		}
	}
	if len(vids) == 0 {
		t.Fatal("web detail yielded no video ids")
	}
	t.Logf("series %s vid_count=%d episode_cnt=%q accessible_episode_cnt=%q pay_type=%q",
		seriesID, len(vids), mapString(detail, "episode_cnt"), mapString(detail, "accessible_episode_cnt"), mapString(detail, "pay_type"))

	// Boundary scan: where does unauthenticated web access stop?
	for _, index := range []int{0, 1, 2, 3, 5, 10, 20} {
		if index >= len(vids) {
			continue
		}
		vid := vids[index]
		pageBody, pageErr := d.fetchProviderText(ctx, hongguoBaseURL+"/player/"+url.PathEscape(seriesID)+"/"+url.PathEscape(vid), hongguoBaseURL+"/")
		if pageErr != nil {
			t.Logf("episode %-2d status=blocked %v", index+1, pageErr)
			continue
		}
		page := routerLoaderMap(parseRouterData(pageBody), "player_", "player_page")
		seo := nestedMap(page, "publicVideoSeo")
		req := nestedMap(page, "req")
		videoList, _ := page["videoList"].(map[string]any)
		t.Logf("episode %-2d bytes=%d vid_match=%t protectedPlayback=%v videoList_keys=%d publicVideoSeo=%v pcLoginRequired=%v pcAuthenticatedPlayback=%v encryptedBrowserAllowed=%v ld_blocks=%d",
			index+1, len(pageBody), mapString(page, "vid") == vid, page["protectedPlayback"], len(videoList),
			seo, req["pcLoginRequired"], req["pcAuthenticatedPlayback"], req["encryptedBrowserAllowed"],
			len(liveProbeJSONLDBlocks(pageBody)))
		if index == 0 {
			liveProbeDescribe(t, "player", page, 0)
			liveProbeDescribeJSONLD(t, pageBody)
			liveProbeDescribe(t, "player.videoList", videoList, 1)
		}
	}

	// Third path: the external fallback parse endpoint used in production.
	for _, index := range []int{0, 3} {
		if index >= len(vids) {
			continue
		}
		media, err := d.resolveHongguoPlaybackAPI(ctx, seriesID, vids[index])
		t.Logf("fallback-api episode %-2d video_platform=3 variants=%d quality=%d encrypted=%t err=%v",
			index+1, len(media.Variants), media.Quality, len(media.CENCKey) > 0, err)
		liveProbeDescribeFallbackPayload(ctx, t, d, seriesID, vids[index], 3)
		liveProbeDescribeFallbackPayload(ctx, t, d, seriesID, vids[index], 1024)
	}
}

// liveProbeDescribeFallbackPayload reports the shape of the external fallback
// response without carrying media addresses into the log.
func liveProbeDescribeFallbackPayload(ctx context.Context, t *testing.T, d *Downloader, seriesID, videoID string, platform int) {
	t.Helper()
	reference, err := json.Marshal(hongguoPlaybackReference{ContentType: 1004, SeriesID: seriesID, VideoID: videoID, VideoPlatform: platform})
	if err != nil {
		t.Logf("  fallback(video_platform=%d) marshal: %v", platform, err)
		return
	}
	query := url.Values{"id": {base64.StdEncoding.EncodeToString(reference)}}
	body, err := d.fetchProviderText(ctx, hongguoPlaybackAPI+"?"+query.Encode(), hongguoBaseURL+"/")
	if err != nil {
		t.Logf("  fallback(video_platform=%d) request: %v", platform, err)
		return
	}
	decoded, err := decodeHongguoPlaybackResponse(body)
	if err != nil {
		t.Logf("  fallback(video_platform=%d) bytes=%d decode: %v", platform, len(body), err)
		return
	}
	var response hongguoPlaybackResponse
	if err := json.Unmarshal(decoded, &response); err != nil {
		t.Logf("  fallback(video_platform=%d) plain bytes=%d top-level keys=%v", platform, len(decoded),
			sortedKeys(func() map[string]any { var rows map[string]any; _ = json.Unmarshal(decoded, &rows); return rows }()))
		return
	}
	names := make([]string, 0, len(response.KeyURLs))
	for _, option := range response.KeyURLs {
		names = append(names, fmt.Sprintf("%s(kid=%d,spade=%d,src=%s)", option.Name, len(option.KeyID), len(option.SpadeA), liveProbeStringKind(option.URL)))
	}
	t.Logf("  fallback(video_platform=%d) plain bytes=%d parse=%v jx=%v key_urls=%d %v",
		platform, len(decoded), string(response.Parse), string(response.JX), len(response.KeyURLs), names)
}

// liveProbeDescribe reports value types and key names to a bounded depth.
func liveProbeDescribe(t *testing.T, path string, value any, depth int) {
	t.Helper()
	if depth > 3 || liveProbeNoisy(path) {
		return
	}
	switch value := value.(type) {
	case map[string]any:
		keys := sortedKeys(value)
		t.Logf("%s map(%d) keys=%v", path, len(keys), keys)
		if depth == 2 {
			return
		}
		for _, key := range keys {
			liveProbeDescribe(t, path+"."+key, value[key], depth+1)
		}
	case []any:
		t.Logf("%s list(%d)", path, len(value))
		if len(value) > 0 {
			liveProbeDescribe(t, path+"[0]", value[0], depth+1)
		}
	case string:
		kind := liveProbeStringKind(value)
		t.Logf("%s string(len=%d) %s", path, len(value), kind)
		if kind == "encoded-json" {
			var nested any
			if err := json.Unmarshal([]byte(value), &nested); err == nil {
				liveProbeDescribe(t, path+"(decoded)", nested, depth+1)
			}
		}
	case bool:
		t.Logf("%s bool %v", path, value)
	case json.Number:
		t.Logf("%s number %s", path, value.String())
	default:
		t.Logf("%s %T", path, value)
	}
}

// liveProbeNoisy drops the SSR request context and social counters, which carry
// page plumbing rather than playback data.
func liveProbeNoisy(path string) bool {
	for _, prefix := range []string{"player.req.", "player.seriesSocialInfo", "player.episodeSocialInfo"} {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func liveProbeStringKind(value string) string {
	switch {
	case strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://"):
		parsed, err := url.Parse(value)
		if err != nil {
			return "url(unparseable)"
		}
		names := make([]string, 0, len(parsed.Query()))
		for name := range parsed.Query() {
			names = append(names, name)
		}
		sort.Strings(names)
		return "url host=" + parsed.Host + " path=" + liveProbeRedactPath(parsed.Path) + " query=" + strings.Join(names, ",")
	case json.Valid([]byte(value)):
		var nested any
		if err := json.Unmarshal([]byte(value), &nested); err == nil && nested != nil {
			return "encoded-json"
		}
	}
	return "text"
}

func liveProbeRedactPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for index, part := range parts {
		if hongguoNumericID.MatchString(part) || len(part) >= 19 {
			parts[index] = "{" + liveProbeHexPrefix(part) + "}"
		}
	}
	return "/" + strings.Join(parts, "/")
}

func liveProbeHexPrefix(value string) string {
	if len(value) > 6 {
		return value[:6]
	}
	return value
}

func liveProbeRedact(value string) string {
	if len(value) <= 6 {
		return value
	}
	return value[:6] + "…"
}

func liveProbeDescribeJSONLD(t *testing.T, pageBody string) {
	t.Helper()
	blocks := liveProbeJSONLDBlocks(pageBody)
	t.Logf("ld+json blocks=%d markers: ld_json=%d tv_episode=%d contentUrl=%d public_mp4=%d next_data=%d",
		len(blocks), strings.Count(pageBody, "ld+json"), strings.Count(pageBody, "TVEpisode"),
		strings.Count(pageBody, "contentUrl"), strings.Count(pageBody, "/video/public/"),
		strings.Count(pageBody, "__NEXT_DATA__"))
	for _, block := range blocks {
		var decoded any
		if err := json.Unmarshal([]byte(block), &decoded); err != nil {
			t.Logf("ld+json unparseable len=%d", len(block))
			continue
		}
		liveProbeWalkJSONLD(t, "ld", decoded)
	}
	if len(blocks) == 0 {
		t.Logf("TVEpisode context: %s", liveProbeSanitizedAround(pageBody, "TVEpisode", 160))
		t.Logf("contentUrl context: %s", liveProbeSanitizedAround(pageBody, "contentUrl", 160))
	}
}

// liveProbeSanitizedAround returns a needle's neighbourhood with address values
// reduced to host plus path shape, so no signed media address reaches the log.
func liveProbeSanitizedAround(body, needle string, span int) string {
	index := strings.Index(body, needle)
	if index < 0 {
		return "(absent)"
	}
	start, end := max(0, index-span), min(len(body), index+span)
	sanitized := liveProbeURLPattern.ReplaceAllStringFunc(body[start:end], func(address string) string {
		parsed, err := url.Parse(address)
		if err != nil {
			return "https://[unparseable]"
		}
		return parsed.Scheme + "://" + parsed.Host + liveProbeRedactPath(parsed.Path)
	})
	return strings.ReplaceAll(sanitized, "\n", " ")
}

var liveProbeURLPattern = regexp.MustCompile(`https?://[^\s"'\\<>]+`)

func liveProbeJSONLDBlocks(pageBody string) []string {
	var blocks []string
	for _, match := range liveProbeScriptPattern.FindAllStringSubmatch(pageBody, -1) {
		if block := strings.TrimSpace(match[1]); block != "" {
			blocks = append(blocks, block)
		}
	}
	return blocks
}

var liveProbeScriptPattern = regexp.MustCompile(`(?is)<script[^>]*ld\+json[^>]*>(.*?)</script>`)

func liveProbeWalkJSONLD(t *testing.T, path string, value any) {
	switch value := value.(type) {
	case map[string]any:
		t.Logf("%s @type=%q keys=%v", path, mapString(value, "@type"), sortedKeys(value))
		for _, key := range []string{"hasPart", "subject", "@graph", "itemListElement"} {
			if child, ok := value[key]; ok {
				liveProbeWalkJSONLD(t, path+"."+key, child)
			}
		}
	case []any:
		t.Logf("%s list(%d)", path, len(value))
		if len(value) > 0 {
			liveProbeWalkJSONLD(t, path+"[0]", value[0])
		}
	case string:
		t.Logf("%s %s", path, liveProbeStringKind(value))
	default:
		t.Logf("%s %T", path, value)
	}
}
