package bootstraptoken_test

import (
	"encoding/base64"
	"encoding/pem"
	"testing"

	"github.com/cirruslabs/orchard/internal/bootstraptoken"
	controllercmd "github.com/cirruslabs/orchard/internal/command/controller"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBootstrapTokenTwoWay(t *testing.T) {
	tlsCert, err := controllercmd.GenerateSelfSignedControllerCertificate()
	require.NoError(t, err)

	block := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: tlsCert.Certificate[0],
	}
	certificatePEM := pem.EncodeToMemory(block)

	bootstrapTokenOld, err := bootstraptoken.New(certificatePEM, uuid.NewString(), uuid.NewString())
	require.NoError(t, err)

	bootstrapTokenNew, err := bootstraptoken.NewFromString(bootstrapTokenOld.String())
	require.NoError(t, err)

	require.Equal(t, bootstrapTokenOld.ServiceAccountName(), bootstrapTokenNew.ServiceAccountName())
	require.Equal(t, bootstrapTokenOld.ServiceAccountToken(), bootstrapTokenNew.ServiceAccountToken())
	require.Equal(t, bootstrapTokenOld.Certificate(), bootstrapTokenNew.Certificate())
	require.Equal(t, bootstrapTokenOld.String(), bootstrapTokenNew.String())
}

func TestBootstrapTokenTwoWayEmptyCertificate(t *testing.T) {
	bootstrapTokenOld, err := bootstraptoken.New([]byte{}, uuid.NewString(), uuid.NewString())
	require.NoError(t, err)

	bootstrapTokenNew, err := bootstraptoken.NewFromString(bootstrapTokenOld.String())
	require.NoError(t, err)

	require.Equal(t, bootstrapTokenOld.ServiceAccountName(), bootstrapTokenNew.ServiceAccountName())
	require.Equal(t, bootstrapTokenOld.ServiceAccountToken(), bootstrapTokenNew.ServiceAccountToken())
	require.Equal(t, bootstrapTokenOld.Certificate(), bootstrapTokenNew.Certificate())
}

func TestNewFromStringNonPEMCertificate(t *testing.T) {
	rawBootstrapToken := "orchard-bootstrap-token-v0." +
		encodeTokenPart("name") + "." +
		encodeTokenPart("token") + "." +
		encodeTokenPart("not pem")

	_, err := bootstraptoken.NewFromString(rawBootstrapToken)

	require.ErrorIs(t, err, bootstraptoken.ErrInvalidBootstrapTokenFormat)
}

func TestNewFromStringInvalidCertificate(t *testing.T) {
	block := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: []byte("not der"),
	}
	rawBootstrapToken := "orchard-bootstrap-token-v0." +
		encodeTokenPart("name") + "." +
		encodeTokenPart("token") + "." +
		encodeTokenPart(string(pem.EncodeToMemory(block)))

	_, err := bootstraptoken.NewFromString(rawBootstrapToken)

	require.ErrorIs(t, err, bootstraptoken.ErrInvalidBootstrapTokenFormat)
}

func TestNewFromStringEmptyServiceAccountName(t *testing.T) {
	rawBootstrapToken := "orchard-bootstrap-token-v0.." + encodeTokenPart("token")

	_, err := bootstraptoken.NewFromString(rawBootstrapToken)

	require.ErrorIs(t, err, bootstraptoken.ErrInvalidBootstrapTokenFormat)
}

func TestNewFromStringEmptyServiceAccountToken(t *testing.T) {
	rawBootstrapToken := "orchard-bootstrap-token-v0." + encodeTokenPart("name") + "."

	_, err := bootstraptoken.NewFromString(rawBootstrapToken)

	require.ErrorIs(t, err, bootstraptoken.ErrInvalidBootstrapTokenFormat)
}

func encodeTokenPart(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}
