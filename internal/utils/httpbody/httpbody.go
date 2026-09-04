package httpbody

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
)

const (
	MaxLLMResponseBodyBytes   int64 = 64 << 20
	MaxErrorResponseBodyBytes int64 = 64 << 10

	// MaxLLMRequestBodyBytes bounds regular chat/responses requests and the
	// serialized body after channel parameter overrides. Image endpoints use
	// their dedicated spill-to-disk body cache instead.
	MaxLLMRequestBodyBytes int64 = 64 << 20
	// Management imports can legitimately include logs and statistics, so they
	// retain a larger but still finite bound than ordinary JSON requests.
	MaxManagementImportBodyBytes int64 = 256 << 20
	MaxSiteImportBodyBytes       int64 = 64 << 20
)

var (
	ErrBodyTooLarge        = errors.New("response body exceeds size limit")
	ErrRequestBodyTooLarge = errors.New("request body exceeds size limit")
)

func IsErrorStatus(statusCode int) bool {
	return statusCode > 0 && (statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices)
}

func ReadResponse(response *http.Response) ([]byte, error) {
	if response == nil {
		return nil, errors.New("response is nil")
	}
	limit := MaxLLMResponseBodyBytes
	if IsErrorStatus(response.StatusCode) {
		limit = MaxErrorResponseBodyBytes
	}
	return ReadAll(response.Body, limit)
}

// ReadRequestBody reads a client or generated outbound request body with the
// supplied finite limit. It uses the same exact-limit/overflow behavior as
// response reads but exposes a request-specific sentinel for HTTP handlers and
// callers that need to map the failure to 413.
func ReadRequestBody(reader io.Reader, maxBytes int64) ([]byte, error) {
	body, err := ReadAll(reader, maxBytes)
	if errors.Is(err, ErrBodyTooLarge) || IsRequestBodyTooLarge(err) {
		return nil, fmt.Errorf("%w: limit %d bytes", ErrRequestBodyTooLarge, maxBytes)
	}
	return body, err
}

// IsRequestBodyTooLarge recognizes both the package sentinel and the standard
// library error produced by http.MaxBytesReader while parsing multipart data.
func IsRequestBodyTooLarge(err error) bool {
	if errors.Is(err, ErrRequestBodyTooLarge) {
		return true
	}
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr)
}

// RequestContentLengthTooLarge is a cheap early rejection for requests that
// advertise a known body length. Chunked requests still need ReadRequestBody
// or http.MaxBytesReader for enforcement.
func RequestContentLengthTooLarge(request *http.Request, maxBytes int64) bool {
	return request != nil && request.ContentLength > maxBytes
}

// ReadRequest reads an HTTP request body after applying its declared length
// check and then enforcing the limit while consuming the stream.
func ReadRequest(request *http.Request, maxBytes int64) ([]byte, error) {
	if request == nil {
		return nil, errors.New("request is nil")
	}
	if RequestContentLengthTooLarge(request, maxBytes) {
		return nil, fmt.Errorf("%w: limit %d bytes", ErrRequestBodyTooLarge, maxBytes)
	}
	return ReadRequestBody(request.Body, maxBytes)
}

// ReadAll reads at most maxBytes and reports an error when the body contains
// additional data. Exactly maxBytes is accepted. The internal maxBytes+1 read
// limit is clamped so maxBytes == math.MaxInt64 cannot wrap negative.
func ReadAll(reader io.Reader, maxBytes int64) ([]byte, error) {
	if reader == nil {
		return nil, errors.New("body is nil")
	}
	if maxBytes < 0 {
		return nil, errors.New("response body limit must not be negative")
	}

	// maxBytes+1 keeps one byte of over-limit data detectable. At the
	// maximum int64 limit no increment is possible; use the maximum directly.
	var readLimit int64 = math.MaxInt64
	if maxBytes < math.MaxInt64 {
		readLimit = maxBytes + 1
	}
	body, err := io.ReadAll(io.LimitReader(reader, readLimit))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("%w: limit %d bytes", ErrBodyTooLarge, maxBytes)
	}
	return body, nil
}
