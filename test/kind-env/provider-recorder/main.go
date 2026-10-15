package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

type response struct {
	Provider   string `json:"provider"`
	TLS        bool   `json:"tls"`
	SNI        bool   `json:"sni_ok"`
	Authority  bool   `json:"authority_ok"`
	Credential bool   `json:"credential_ok"`
}

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("required environment variable %s is empty", name)
	}
	return value
}

func main() {
	provider := required("PROVIDER_NAME")
	providerLabel := ""
	switch provider {
	case "proof-a":
		providerLabel = "proof-a"
	case "proof-b":
		providerLabel = "proof-b"
	default:
		log.Fatal("PROVIDER_NAME must be proof-a or proof-b")
	}
	expectedHost := required("EXPECTED_HOST")
	credentialFile := required("CREDENTIAL_FILE")
	credential, err := os.ReadFile(credentialFile)
	if err != nil {
		log.Fatalf("read expected credential file: %v", err)
	}
	expectedCredential := strings.TrimSpace(string(credential))
	if expectedCredential == "" {
		log.Fatal("expected credential file is empty")
	}
	certFile := "/etc/provider/tls/tls.crt"
	keyFile := "/etc/provider/tls/tls.key"
	requestIDPattern := regexp.MustCompile(`^[A-Za-z0-9._-]{1,80}$`)

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		sniOK := r.TLS != nil && r.TLS.ServerName == expectedHost
		authorityOK := r.Host == expectedHost
		const bearerPrefix = "Bearer "
		authorization := r.Header.Get("Authorization")
		credential := strings.TrimPrefix(authorization, bearerPrefix)
		credentialOK := strings.HasPrefix(authorization, bearerPrefix) &&
			len(credential) == len(expectedCredential) &&
			subtle.ConstantTimeCompare([]byte(credential), []byte(expectedCredential)) == 1
		status := http.StatusOK
		if !sniOK || !authorityOK {
			status = http.StatusMisdirectedRequest
		} else if !credentialOK {
			status = http.StatusUnauthorized
		}
		requestID := r.Header.Get("X-Issue28-Request-Id")
		if !requestIDPattern.MatchString(requestID) {
			requestID = ""
		}
		sessionHash := ""
		if sessionID := r.Header.Get("X-Session-Id"); sessionID != "" {
			digest := sha256.Sum256([]byte(sessionID))
			sessionHash = hex.EncodeToString(digest[:])
		}
		// #nosec G706 -- providerLabel is a fixed proof-a/proof-b enum; status and flags are derived from constants.
		log.Printf("provider=%s status=%d tls=%t sni_ok=%t authority_ok=%t credential_ok=%t request_id=%s session_id_sha256=%s", providerLabel, status, r.TLS != nil, sniOK, authorityOK, credentialOK, requestID, sessionHash)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(response{Provider: provider, TLS: r.TLS != nil, SNI: sniOK, Authority: authorityOK, Credential: credentialOK})
	})

	server := &http.Server{Addr: ":8443", ReadHeaderTimeout: 5 * time.Second}
	log.Printf("provider recorder ready provider=%s host=%s", providerLabel, expectedHost)
	if err := server.ListenAndServeTLS(certFile, keyFile); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
