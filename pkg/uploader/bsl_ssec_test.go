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

package uploader

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testSSECNamespace  = "velero-ns"
	testSSECSecretName = "sse-c-key"
	testSSECSecretKey  = "customer-key"
)

func testSSECKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	return key
}

func testSSECBSL(customerKeySecret string) *velerov1.BackupStorageLocation {
	config := map[string]string{"region": "us-east-1"}
	if customerKeySecret != "" {
		config["customerKeyEncryptionSecret"] = customerKeySecret
	}
	return &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testSSECNamespace},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			Config:   config,
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{Bucket: "test-bucket", Prefix: "velero"},
			},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-credentials"},
				Key:                  "cloud",
			},
		},
	}
}

func testSSECObjects(sseKeyData map[string][]byte) []runtime.Object {
	objs := []runtime.Object{
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cloud-credentials", Namespace: testSSECNamespace},
			Data:       map[string][]byte{"cloud": []byte("[default]\naws_access_key_id=a\naws_secret_access_key=b\n")},
		},
	}
	if sseKeyData != nil {
		objs = append(objs, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testSSECSecretName, Namespace: testSSECNamespace},
			Data:       sseKeyData,
		})
	}
	return objs
}

func TestParseSSECSecretRef(t *testing.T) {
	tests := []struct {
		ref        string
		wantName   string
		wantKey    string
		wantErrMsg string
	}{
		{ref: "", wantName: "", wantKey: ""},
		{ref: "my-secret/my-key", wantName: "my-secret", wantKey: "my-key"},
		{ref: "my-secret/dir/key", wantName: "my-secret", wantKey: "dir/key"},
		{ref: "no-slash", wantErrMsg: "secretName/key format"},
		{ref: "/key", wantErrMsg: "secretName/key format"},
		{ref: "secret/", wantErrMsg: "secretName/key format"},
	}
	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			name, key, err := ParseSSECSecretRef(tt.ref)
			if tt.wantErrMsg != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErrMsg)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if name != tt.wantName || key != tt.wantKey {
				t.Errorf("got (%q, %q), want (%q, %q)", name, key, tt.wantName, tt.wantKey)
			}
		})
	}
}

// TestInitObjectStoreFromBSLPassesSSECKey verifies that the controller path
// hands the BSL's SSE-C key to the object store. Without it, reading the
// checkpoint index of an SSE-C encrypted BSL fails with 400 Bad Request and
// every backup silently falls back to a full backup.
func TestInitObjectStoreFromBSLPassesSSECKey(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	key := testSSECKey()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(testSSECObjects(map[string][]byte{testSSECSecretKey: key})...).Build()

	var got *common.ObjectStoreConfig
	factory := func(c *common.ObjectStoreConfig) (velero.ObjectStore, error) {
		got = c
		return nil, nil
	}

	bsl := testSSECBSL(testSSECSecretName + "/" + testSSECSecretKey)
	if _, _, err := InitObjectStoreFromBSL(context.Background(), k8sClient, testSSECNamespace, bsl, factory); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("factory was not called")
	}
	if !bytes.Equal(got.BSLCustomerKeyEncryptionData, key) {
		t.Errorf("BSLCustomerKeyEncryptionData = %v, want the key from the secret", got.BSLCustomerKeyEncryptionData)
	}
	if got.BSLCustomerKeyEncryptionFile != "" {
		t.Errorf("BSLCustomerKeyEncryptionFile = %q, want empty on the controller path", got.BSLCustomerKeyEncryptionFile)
	}
}

func TestInitObjectStoreFromBSLWithoutSSEC(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(testSSECObjects(nil)...).Build()

	var got *common.ObjectStoreConfig
	factory := func(c *common.ObjectStoreConfig) (velero.ObjectStore, error) {
		got = c
		return nil, nil
	}

	if _, _, err := InitObjectStoreFromBSL(context.Background(), k8sClient, testSSECNamespace, testSSECBSL(""), factory); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || len(got.BSLCustomerKeyEncryptionData) != 0 {
		t.Errorf("BSLCustomerKeyEncryptionData should be empty without customerKeyEncryptionSecret")
	}
}

func TestInitObjectStoreFromBSLSSECErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	tests := []struct {
		name       string
		ref        string
		secretData map[string][]byte
		wantErrMsg string
	}{
		{name: "secret missing", ref: testSSECSecretName + "/" + testSSECSecretKey, secretData: nil, wantErrMsg: "failed to get SSE-C key secret"},
		{name: "key missing", ref: testSSECSecretName + "/other", secretData: map[string][]byte{testSSECSecretKey: testSSECKey()}, wantErrMsg: "does not contain key"},
		{name: "malformed ref", ref: "no-slash", secretData: nil, wantErrMsg: "secretName/key format"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(testSSECObjects(tt.secretData)...).Build()
			called := false
			factory := func(*common.ObjectStoreConfig) (velero.ObjectStore, error) {
				called = true
				return nil, nil
			}
			_, _, err := InitObjectStoreFromBSL(context.Background(), k8sClient, testSSECNamespace, testSSECBSL(tt.ref), factory)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErrMsg)
			}
			if called {
				t.Error("factory must not be called when the SSE-C key cannot be read")
			}
		})
	}
}

func TestInitWithSSECKeyData(t *testing.T) {
	key := testSSECKey()

	fromData := &S3ObjectStore{}
	if err := fromData.Init(map[string]string{
		"bucket":                    "test-bucket",
		"customerKeyEncryptionData": base64.StdEncoding.EncodeToString(key),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fromFile := &S3ObjectStore{}
	if err := fromFile.Init(map[string]string{
		"bucket":                    "test-bucket",
		"customerKeyEncryptionFile": writeSSECKeyFile(t, key),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fromData.sseCustomerAlgorithm != testSSEAES256 {
		t.Errorf("sseCustomerAlgorithm = %q, want AES256", fromData.sseCustomerAlgorithm)
	}
	if fromData.sseCustomerKey == "" || fromData.sseCustomerKey != fromFile.sseCustomerKey {
		t.Errorf("in-memory key must produce the same sseCustomerKey as the key file")
	}
	if fromData.sseCustomerKeyMD5 == "" || fromData.sseCustomerKeyMD5 != fromFile.sseCustomerKeyMD5 {
		t.Errorf("in-memory key must produce the same sseCustomerKeyMD5 as the key file")
	}
}

func TestInitWithSSECKeyDataInvalid(t *testing.T) {
	tests := []struct {
		name       string
		data       string
		wantErrMsg string
	}{
		{name: "not base64", data: "!!!not-base64!!!", wantErrMsg: "failed to decode SSE-C key data"},
		{name: "wrong size", data: base64.StdEncoding.EncodeToString([]byte("too-short")), wantErrMsg: "exactly 32 bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &S3ObjectStore{}
			err := store.Init(map[string]string{"bucket": "test-bucket", "customerKeyEncryptionData": tt.data})
			if err == nil || !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErrMsg)
			}
		})
	}
}

func TestInitWithSSECKeyDataMutualExclusivity(t *testing.T) {
	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                    "test-bucket",
		"kmsKeyId":                  "arn:aws:kms:us-east-1:123456789012:key/abc",
		"customerKeyEncryptionData": base64.StdEncoding.EncodeToString(testSSECKey()),
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be used with kmsKeyId") {
		t.Fatalf("error = %v, want mutual exclusivity error", err)
	}
}
