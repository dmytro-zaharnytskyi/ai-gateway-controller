/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consumerportal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

const (
	keyClientID     = "OAUTH2_PROXY_CLIENT_ID"
	keyClientSecret = "OAUTH2_PROXY_CLIENT_SECRET" //nolint:gosec // Secret data key, not a credential
	keyCookieSecret = "OAUTH2_PROXY_COOKIE_SECRET" //nolint:gosec // Secret data key, not a credential

	// CredentialsHashAnnotation on the proxy pod template restarts the proxy
	// when its credentials change.
	CredentialsHashAnnotation = "ai-gateway-controller.opendatahub.io/credentials-hash"
	// DefaultRefreshInterval refreshes the session ahead of a five-minute
	// access token lifetime.
	DefaultRefreshInterval = 4 * time.Minute
	// DefaultCookieExpire is the consumer portal session lifetime used when
	// AITenant spec.oidc.cookieExpire is unset or invalid. The refresh
	// interval must remain below that lifetime.
	DefaultCookieExpire = 6 * time.Hour
)

// ClientSecretRef returns AITenant spec.oidc.clientSecretRef, the Secret in
// the tenant namespace that holds the OIDC client secret.
func ClientSecretRef(aitenant *unstructured.Unstructured) (name, key string, ok bool) {
	name, _, _ = unstructured.NestedString(aitenant.Object, "spec", "oidc", "clientSecretRef", "name")
	key, _, _ = unstructured.NestedString(aitenant.Object, "spec", "oidc", "clientSecretRef", "key")
	return name, key, name != "" && key != ""
}

// RefreshInterval returns AITenant spec.oidc.refreshInterval, or
// DefaultRefreshInterval when unset or invalid. The result is always kept
// below the effective cookie lifetime: the auth proxy refuses to start with
// --cookie-refresh at or above --cookie-expire.
func RefreshInterval(aitenant *unstructured.Unstructured) time.Duration {
	expire := CookieExpire(aitenant)
	value, _, _ := unstructured.NestedString(aitenant.Object, "spec", "oidc", "refreshInterval")
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 || interval >= expire {
		interval = DefaultRefreshInterval
	}
	if interval >= expire {
		interval = expire / 2
	}
	return interval
}

// CookieExpire returns AITenant spec.oidc.cookieExpire, or DefaultCookieExpire
// when unset or invalid.
func CookieExpire(aitenant *unstructured.Unstructured) time.Duration {
	value, _, _ := unstructured.NestedString(aitenant.Object, "spec", "oidc", "cookieExpire")
	expire, err := time.ParseDuration(value)
	if err != nil || expire <= 0 {
		return DefaultCookieExpire
	}
	return expire
}

// reconcileCredentials ensures the proxy credentials Secret exists and returns
// a hash of its contents. When the AITenant references a tenant client
// Secret, the controller builds the credentials from it and generates the
// cookie secret once. Otherwise it uses a credentials Secret created by an
// administrator. ready is false while a required Secret is missing.
func (r *Reconciler) reconcileCredentials(ctx context.Context, log logr.Logger, aitenant *unstructured.Unstructured, infraNamespace string) (hash string, ready bool, err error) {
	existing := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: infraNamespace, Name: CredentialsSecretName}, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return "", false, fmt.Errorf("get Secret %s/%s: %w", infraNamespace, CredentialsSecretName, err)
		}
		existing = nil
	}

	name, key, ok := ClientSecretRef(aitenant)
	if !ok {
		if existing == nil {
			log.Info("auth proxy credentials Secret does not exist yet and spec.oidc.clientSecretRef is unset; will retry",
				"namespace", infraNamespace, "secret", CredentialsSecretName)
			return "", false, nil
		}
		return credentialsHash(existing.Data), true, nil
	}

	tenantNamespace, ok := tenant.ConfigNamespace(aitenant)
	if !ok {
		return "", false, nil
	}
	source := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: tenantNamespace, Name: name}, source); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("OIDC client Secret named by spec.oidc.clientSecretRef does not exist; will retry",
				"namespace", tenantNamespace, "secret", name)
			return "", false, nil
		}
		return "", false, fmt.Errorf("get Secret %s/%s: %w", tenantNamespace, name, err)
	}
	clientSecret := source.Data[key]
	clientID := OIDCClientID(aitenant)
	if len(clientSecret) == 0 || clientID == "" {
		log.Info("OIDC client Secret key is empty or spec.oidc.clientId is unset; will retry",
			"namespace", tenantNamespace, "secret", name, "key", key)
		return "", false, nil
	}

	// Keep the cookie secret across reconciles; replacing it signs every
	// user out. Deleting the credentials Secret rotates it.
	var cookieSecret []byte
	if existing != nil {
		cookieSecret = existing.Data[keyCookieSecret]
	}
	if len(cookieSecret) == 0 {
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			return "", false, fmt.Errorf("generate cookie secret: %w", err)
		}
		cookieSecret = []byte(base64.URLEncoding.EncodeToString(raw))
	}

	data := map[string][]byte{
		keyClientID:     []byte(clientID),
		keyClientSecret: clientSecret,
		keyCookieSecret: cookieSecret,
	}
	if err := render.Apply(ctx, r.Client, []unstructured.Unstructured{credentialsSecret(infraNamespace, data)}); err != nil {
		return "", false, fmt.Errorf("apply credentials Secret: %w", err)
	}
	return credentialsHash(data), true, nil
}

// OIDCClientID returns AITenant spec.oidc.clientId.
func OIDCClientID(aitenant *unstructured.Unstructured) string {
	clientID, _, _ := unstructured.NestedString(aitenant.Object, "spec", "oidc", "clientId")
	return clientID
}

func credentialsSecret(namespace string, data map[string][]byte) unstructured.Unstructured {
	encoded := make(map[string]any, len(data))
	for k, v := range data {
		encoded[k] = base64.StdEncoding.EncodeToString(v)
	}
	u := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"type":       string(corev1.SecretTypeOpaque),
		"data":       encoded,
	}}
	u.SetName(CredentialsSecretName)
	u.SetNamespace(namespace)
	u.SetLabels(map[string]string{
		tenant.LabelManagedBy:       tenant.ManagedByAIGatewayController,
		"app.kubernetes.io/name":    AuthProxyName,
		"app.kubernetes.io/part-of": "maas-consumer-portal",
	})
	return u
}

func credentialsHash(data map[string][]byte) string {
	sum := sha256.New()
	for _, k := range []string{keyClientID, keyClientSecret, keyCookieSecret} {
		sum.Write([]byte(k))
		sum.Write([]byte{0})
		sum.Write(data[k])
		sum.Write([]byte{0})
	}
	return hex.EncodeToString(sum.Sum(nil))[:16]
}
