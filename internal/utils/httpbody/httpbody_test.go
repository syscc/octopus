package httpbody

import (
	"errors"
	"io"
	"math"
	"net/http"
	"testing"
)

// All test bodies are synthetic byte patterns. Failures report only lengths
// and byte indices, so no test can print potentially sensitive data.

// patternReader streams count copies of b without allocating the whole body
// up front, which keeps the 64 MiB boundary tests cheap.
type patternReader struct {
	count int64
	b     byte
}

func (r *patternReader) Read(p []byte) (int, error) {
	if r.count <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.count {
		n = r.count
	}
	for i := int64(0); i < n; i++ {
		p[i] = r.b
	}
	r.count -= n
	return int(n), nil
}

func bodyOf(count int64, b byte) io.Reader {
	return &patternReader{count: count, b: b}
}

func newResponse(status int, body io.Reader) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(body)}
}

func assertUniform(t *testing.T, body []byte, want byte, wantLen int64) {
	t.Helper()
	if int64(len(body)) != wantLen {
		t.Fatalf("body length = %d, want %d", len(body), wantLen)
	}
	for i, got := range body {
		if got != want {
			t.Fatalf("body byte %d does not match the expected pattern", i)
		}
	}
}

func TestLimitConstants(t *testing.T) {
	if MaxLLMResponseBodyBytes != 64<<20 {
		t.Fatalf("MaxLLMResponseBodyBytes = %d, want %d", MaxLLMResponseBodyBytes, int64(64<<20))
	}
	if MaxErrorResponseBodyBytes != 64<<10 {
		t.Fatalf("MaxErrorResponseBodyBytes = %d, want %d", MaxErrorResponseBodyBytes, int64(64<<10))
	}
	if MaxLLMRequestBodyBytes != 64<<20 {
		t.Fatalf("MaxLLMRequestBodyBytes = %d, want %d", MaxLLMRequestBodyBytes, int64(64<<20))
	}
	if MaxManagementImportBodyBytes != 256<<20 {
		t.Fatalf("MaxManagementImportBodyBytes = %d, want %d", MaxManagementImportBodyBytes, int64(256<<20))
	}
	if MaxSiteImportBodyBytes != 64<<20 {
		t.Fatalf("MaxSiteImportBodyBytes = %d, want %d", MaxSiteImportBodyBytes, int64(64<<20))
	}
}

func TestReadRequestUsesDeclaredAndStreamingLimits(t *testing.T) {
	request := &http.Request{
		Body:          io.NopCloser(bodyOf(16, 'x')),
		ContentLength: 16,
	}
	body, err := ReadRequest(request, 16)
	if err != nil {
		t.Fatalf("ReadRequest() exact declared limit error = %v, want nil", err)
	}
	assertUniform(t, body, 'x', 16)

	request = &http.Request{
		Body:          io.NopCloser(bodyOf(17, 'x')),
		ContentLength: 17,
	}
	if _, err := ReadRequest(request, 16); !errors.Is(err, ErrRequestBodyTooLarge) {
		t.Fatalf("ReadRequest() declared over-limit error = %v, want ErrRequestBodyTooLarge", err)
	}

	request = &http.Request{
		Body:          io.NopCloser(bodyOf(17, 'x')),
		ContentLength: -1,
	}
	if _, err := ReadRequest(request, 16); !errors.Is(err, ErrRequestBodyTooLarge) {
		t.Fatalf("ReadRequest() streamed over-limit error = %v, want ErrRequestBodyTooLarge", err)
	}
}

func TestIsErrorStatus(t *testing.T) {
	cases := []struct {
		status int
		want   bool
	}{
		{0, false},
		{http.StatusOK, false},
		{http.StatusCreated, false},
		{http.StatusAccepted, false},
		{http.StatusNoContent, false},
		{http.StatusPartialContent, false},
		{299, false},
		{199, true},
		{http.StatusMultipleChoices, true},
		{http.StatusMovedPermanently, true},
		{http.StatusBadRequest, true},
		{http.StatusUnauthorized, true},
		{http.StatusForbidden, true},
		{http.StatusNotFound, true},
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true},
	}
	for _, tc := range cases {
		if got := IsErrorStatus(tc.status); got != tc.want {
			t.Errorf("IsErrorStatus(%d) = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestReadAllExactLimitSuccess(t *testing.T) {
	const max = int64(512)
	body, err := ReadAll(bodyOf(max, 'x'), max)
	if err != nil {
		t.Fatalf("ReadAll() error = %v, want nil", err)
	}
	assertUniform(t, body, 'x', max)
}

func TestReadAllEmptyBodyWithinLimit(t *testing.T) {
	body, err := ReadAll(bodyOf(0, 'x'), 16)
	if err != nil {
		t.Fatalf("ReadAll() error = %v, want nil", err)
	}
	if len(body) != 0 {
		t.Fatalf("ReadAll() length = %d, want 0", len(body))
	}
}

func TestReadAllZeroLimit(t *testing.T) {
	if _, err := ReadAll(bodyOf(0, 'x'), 0); err != nil {
		t.Fatalf("ReadAll() error = %v, want nil", err)
	}
	if _, err := ReadAll(bodyOf(1, 'x'), 0); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("ReadAll() error = %v, want ErrBodyTooLarge", err)
	}
}

func TestReadAllOverLimitByOneByte(t *testing.T) {
	const max = int64(512)
	body, err := ReadAll(bodyOf(max+1, 'x'), max)
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("ReadAll() error = %v, want ErrBodyTooLarge", err)
	}
	if body != nil {
		t.Fatalf("ReadAll() body length = %d, want nil body on error", len(body))
	}
}

func TestReadRequestBodyRecognizesRequestSentinel(t *testing.T) {
	body, err := ReadRequestBody(bodyOf(17, 'x'), 16)
	if !errors.Is(err, ErrRequestBodyTooLarge) {
		t.Fatalf("ReadRequestBody() error = %v, want ErrRequestBodyTooLarge", err)
	}
	if body != nil {
		t.Fatalf("ReadRequestBody() body length = %d, want nil body on error", len(body))
	}
}

func TestReadAllNilReader(t *testing.T) {
	body, err := ReadAll(nil, 16)
	if err == nil {
		t.Fatal("ReadAll(nil reader) error = nil, want error")
	}
	if body != nil {
		t.Fatalf("ReadAll(nil reader) body length = %d, want nil body on error", len(body))
	}
}

func TestReadAllNegativeLimit(t *testing.T) {
	body, err := ReadAll(bodyOf(4, 'x'), -1)
	if err == nil {
		t.Fatal("ReadAll(negative limit) error = nil, want error")
	}
	if body != nil {
		t.Fatalf("ReadAll(negative limit) body length = %d, want nil body on error", len(body))
	}
}

// TestReadAllMaxBytesPlusOneOverflowEdge covers the maxBytes+1 computation at
// the int64 boundary: before the overflow guard, maxBytes = math.MaxInt64 made
// the read limit wrap negative, io.LimitReader reported EOF immediately, and
// the body was silently truncated to zero bytes with a nil error.
func TestReadAllMaxBytesPlusOneOverflowEdge(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxBytes int64
	}{
		{"max int64", math.MaxInt64},
		{"max int64 minus one", math.MaxInt64 - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const size = int64(32)
			body, err := ReadAll(bodyOf(size, 'x'), tc.maxBytes)
			if err != nil {
				t.Fatalf("ReadAll() error = %v, want nil", err)
			}
			assertUniform(t, body, 'x', size)
		})
	}
}

type failingReader struct{ err error }

func (r *failingReader) Read(p []byte) (int, error) {
	return 0, r.err
}

func TestReadAllPropagatesReadError(t *testing.T) {
	readErr := errors.New("synthetic read failure")
	body, err := ReadAll(&failingReader{err: readErr}, 64)
	if !errors.Is(err, readErr) {
		t.Fatalf("ReadAll() error = %v, want %v", err, readErr)
	}
	if body != nil {
		t.Fatalf("ReadAll() body length = %d, want nil body on error", len(body))
	}
}

func TestReadResponseNilResponse(t *testing.T) {
	body, err := ReadResponse(nil)
	if err == nil {
		t.Fatal("ReadResponse(nil) error = nil, want error")
	}
	if body != nil {
		t.Fatalf("ReadResponse(nil) body length = %d, want nil body on error", len(body))
	}
}

func TestReadResponseNilBody(t *testing.T) {
	resp := &http.Response{StatusCode: http.StatusOK} // Body is nil.
	body, err := ReadResponse(resp)
	if err == nil {
		t.Fatal("ReadResponse(nil body) error = nil, want error")
	}
	if body != nil {
		t.Fatalf("ReadResponse(nil body) body length = %d, want nil body on error", len(body))
	}
}

func TestReadResponseStatusAwareLimits(t *testing.T) {
	justOverErrorLimit := MaxErrorResponseBodyBytes + 1

	// Non-2xx responses are capped by the smaller error-body limit.
	resp := newResponse(http.StatusInternalServerError, bodyOf(justOverErrorLimit, 'x'))
	if _, err := ReadResponse(resp); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("status %d: ReadResponse() error = %v, want ErrBodyTooLarge", http.StatusInternalServerError, err)
	}

	// Redirects are treated as error statuses too.
	resp = newResponse(http.StatusMovedPermanently, bodyOf(justOverErrorLimit, 'x'))
	if _, err := ReadResponse(resp); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("status %d: ReadResponse() error = %v, want ErrBodyTooLarge", http.StatusMovedPermanently, err)
	}

	// Exactly the error-body limit is accepted for non-2xx responses.
	resp = newResponse(http.StatusTooManyRequests, bodyOf(MaxErrorResponseBodyBytes, 'x'))
	body, err := ReadResponse(resp)
	if err != nil {
		t.Fatalf("status %d: ReadResponse() error = %v, want nil", http.StatusTooManyRequests, err)
	}
	assertUniform(t, body, 'x', MaxErrorResponseBodyBytes)

	// The same body that overflows the error limit fits within the larger
	// LLM limit used for 2xx responses.
	resp = newResponse(http.StatusOK, bodyOf(justOverErrorLimit, 'x'))
	body, err = ReadResponse(resp)
	if err != nil {
		t.Fatalf("status %d: ReadResponse() error = %v, want nil", http.StatusOK, err)
	}
	assertUniform(t, body, 'x', justOverErrorLimit)

	// An empty error-status body reads successfully.
	resp = newResponse(http.StatusBadRequest, bodyOf(0, 'x'))
	body, err = ReadResponse(resp)
	if err != nil {
		t.Fatalf("status %d: ReadResponse() error = %v, want nil", http.StatusBadRequest, err)
	}
	if len(body) != 0 {
		t.Fatalf("status %d: ReadResponse() body length = %d, want 0", http.StatusBadRequest, len(body))
	}
}

func TestReadResponseLLMBoundaries(t *testing.T) {
	// Exactly 64 MiB is accepted for 2xx responses.
	resp := newResponse(http.StatusOK, bodyOf(MaxLLMResponseBodyBytes, 'x'))
	body, err := ReadResponse(resp)
	if err != nil {
		t.Fatalf("exactly %d bytes: ReadResponse() error = %v, want nil", MaxLLMResponseBodyBytes, err)
	}
	assertUniform(t, body, 'x', MaxLLMResponseBodyBytes)

	// One byte more is rejected.
	resp = newResponse(http.StatusOK, bodyOf(MaxLLMResponseBodyBytes+1, 'x'))
	if _, err := ReadResponse(resp); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("exactly %d bytes: ReadResponse() error = %v, want ErrBodyTooLarge", MaxLLMResponseBodyBytes+1, err)
	}
}
