//go:build envtest

package envtest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authv1 "k8s.io/api/authorization/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const gatewayAPIGroup = "gateway.networking.k8s.io"

// testShippedPermissions asks the API server what the shipped ClusterRole lets
// the controller do, so packaged permissions are verified even before a
// reconciler exercises them.
func testShippedPermissions(t *testing.T, c client.Client) {
	t.Helper()
	// Controller-owned credential copies are applied and deleted in tenant namespaces.
	for _, verb := range []string{"create", "patch", "delete"} {
		assertAccess(t, c, true, authv1.ResourceAttributes{
			Namespace: maasNamespace, Resource: "secrets", Verb: verb,
		})
	}
	for _, verb := range []string{"get", "list", "watch"} {
		// ReferenceGrants authorize cross-namespace ExternalProvider references.
		assertAccess(t, c, true, authv1.ResourceAttributes{
			Group: gatewayAPIGroup, Resource: "referencegrants", Verb: verb,
		})
		// Gateway attachment is authorized through HTTPRoute parent status.
		assertAccess(t, c, false, authv1.ResourceAttributes{
			Group: gatewayAPIGroup, Resource: "gateways", Verb: verb,
		})
		assertAccess(t, c, false, authv1.ResourceAttributes{
			Resource: "namespaces", Verb: verb,
		})
	}
}

func assertAccess(t *testing.T, c client.Client, want bool, attributes authv1.ResourceAttributes) {
	t.Helper()
	review := &authv1.SubjectAccessReview{Spec: authv1.SubjectAccessReviewSpec{
		User:               controllerIdentity.Name,
		Groups:             controllerIdentity.Groups,
		ResourceAttributes: &attributes,
	}}
	require.NoError(t, c.Create(t.Context(), review))
	assert.Equal(t, want, review.Status.Allowed, "%s %s.%s in namespace %q: %s",
		attributes.Verb, attributes.Resource, attributes.Group, attributes.Namespace, review.Status.Reason)
}
