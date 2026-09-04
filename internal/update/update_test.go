package update

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/bestruirui/octopus/internal/utils/httpbody"
)

type updatePatternReader struct {
	remaining int64
	value     byte
}

func (r *updatePatternReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.remaining {
		n = r.remaining
	}
	for i := int64(0); i < n; i++ {
		p[i] = r.value
	}
	r.remaining -= n
	return int(n), nil
}

func newUpdateResponse(status int, size int64) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(&updatePatternReader{remaining: size, value: 'x'}),
	}
}

func TestReadUpdateArchiveResponseUsesExplicitSuccessLimit(t *testing.T) {
	const limit = int64(32)

	body, err := readUpdateArchiveResponse(newUpdateResponse(http.StatusOK, limit), limit)
	if err != nil {
		t.Fatalf("exact archive limit returned error: %v", err)
	}
	if int64(len(body)) != limit {
		t.Fatalf("archive body length = %d, want %d", len(body), limit)
	}

	body, err = readUpdateArchiveResponse(newUpdateResponse(http.StatusOK, limit+1), limit)
	if !errors.Is(err, httpbody.ErrBodyTooLarge) {
		t.Fatalf("over-limit archive error = %v, want ErrBodyTooLarge", err)
	}
	if body != nil {
		t.Fatalf("over-limit archive returned %d bytes, want nil", len(body))
	}
}

func TestReadUpdateArchiveResponseCapsErrorBody(t *testing.T) {
	body, err := readUpdateArchiveResponse(
		newUpdateResponse(http.StatusBadGateway, httpbody.MaxErrorResponseBodyBytes+1),
		maxUpdateArchiveBodyBytes,
	)
	if !errors.Is(err, httpbody.ErrBodyTooLarge) {
		t.Fatalf("error response read error = %v, want ErrBodyTooLarge", err)
	}
	if body != nil {
		t.Fatalf("over-limit error response returned %d bytes, want nil", len(body))
	}
}

func TestReadUpdateJSONResponseUsesStatusAwareLimit(t *testing.T) {
	body, err := readUpdateJSONResponse(newUpdateResponse(http.StatusOK, httpbody.MaxLLMResponseBodyBytes+1))
	if !errors.Is(err, httpbody.ErrBodyTooLarge) {
		t.Fatalf("over-limit JSON success error = %v, want ErrBodyTooLarge", err)
	}
	if body != nil {
		t.Fatalf("over-limit JSON success returned %d bytes, want nil", len(body))
	}

	body, err = readUpdateJSONResponse(newUpdateResponse(http.StatusForbidden, httpbody.MaxErrorResponseBodyBytes))
	if err != nil {
		t.Fatalf("exact JSON error limit returned error: %v", err)
	}
	if int64(len(body)) != httpbody.MaxErrorResponseBodyBytes {
		t.Fatalf("JSON error body length = %d, want %d", len(body), httpbody.MaxErrorResponseBodyBytes)
	}
}

func TestUpdateArchiveLimitIsIndependentFromJSONLimit(t *testing.T) {
	if maxUpdateArchiveBodyBytes <= httpbody.MaxLLMResponseBodyBytes {
		t.Fatalf("archive limit = %d, want greater than JSON limit %d", maxUpdateArchiveBodyBytes, httpbody.MaxLLMResponseBodyBytes)
	}
}
