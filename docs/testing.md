# Testing guide

This guide covers two things: running this repo's own automated test suites (unit tests and
the kubebuilder-scaffolded e2e suite), and setting up a full end-to-end environment to
manually exercise a real VM backup and restore through CBT. The manual flow is the same one
the automated OADP e2e suite runs, just done by hand so you can watch each step and inspect
intermediate state.

## Automated tests in this repo

### Unit tests

```bash
make test
```

This runs `go test` against `internal/controller/...` and `pkg/...` using
[envtest](https://book.kubebuilder.io/reference/envtest.html), which spins up a real
`kube-apiserver` and `etcd` (no full cluster, no kubelet) so reconcilers can be tested against
actual API server behavior instead of a fake client. `make test` also regenerates manifests
and DeepCopy methods first (`manifests generate fmt vet setup-envtest`), so it's safe to run
right after editing `*_types.go` or `+kubebuilder` markers.

Individual packages can be tested directly, for example:

```bash
go test ./internal/controller/... -run TestKubeVirtDataUpload -v
go test ./pkg/uploader/... -v
go test ./pkg/downloader/... -v
```

### Lint

```bash
make lint       # report only
make lint-fix   # auto-fix what it can
```

### Kubebuilder-scaffolded e2e suite

`test/e2e/e2e_test.go` is Kubebuilder's default scaffolded suite (Ginkgo/Gomega).
`test/e2e/e2e_suite_test.go`'s `BeforeSuite` builds and loads its own manager image
(`example.com/kubevirt-datamover-controller:v0.0.1`, hardcoded) and installs cert-manager if
it isn't already present. `e2e_test.go`'s own `BeforeAll` then installs the CRDs and deploys
the controller with `make deploy`, and its test cases check that the controller-manager pod
comes up and serves metrics. Run it against a disposable
[Kind](https://kind.sigs.k8s.io/) cluster, never a real dev or prod cluster:

```bash
kind create cluster --name kdm-e2e
export KIND_CLUSTER=kdm-e2e
go test ./test/e2e/... -tags e2e -v
```

Setting `KIND_CLUSTER` matters: the suite's own image-loading step
(`utils.LoadImageToKindClusterWithName`) loads into whatever cluster that variable names, and
falls back to a cluster literally named `kind` if it isn't set. You do not need to
`docker-build`/`kind load` anything yourself first, the suite does that for you with its own
hardcoded image name.

If cert-manager is already installed on the cluster, or you'd rather skip the automatic
install/teardown, set `CERT_MANAGER_INSTALL_SKIP=true` before running the suite.

This suite validates the manager deployment and metrics endpoint. It does **not** exercise
the CBT backup/restore flow, since that needs a real KubeVirt installation with a running VM,
which a plain Kind cluster doesn't provide. For that, use the manual flow below.

## End-to-end manual test: backup and restore a VM with CBT

This walks through the same flow the OADP e2e suite automates, using the sample manifests
kept in
[`openshift/oadp-operator/tests/e2e/sample-applications/virtual-machines/kubevirt-dm`](https://github.com/openshift/oadp-operator/tree/oadp-dev/tests/e2e/sample-applications/virtual-machines/kubevirt-dm).
Because this needs OpenShift Virtualization, KubeVirt CBT, and a real object store, run it
against a dedicated OpenShift cluster, not a local Kind cluster.

### Prerequisites

- An OpenShift cluster with OpenShift Virtualization installed (HCO `>= 1.18`, KubeVirt
  `>= v1.8.2`). HCO 1.18+ and the `backup.kubevirt.io` CRDs are required for CBT support, and
  KubeVirt `>= v1.8.2` includes a QEMU backup-abort fix this controller depends on.
- The OADP operator installed.
- A working `BackupStorageLocation` (an S3 bucket, or an S3-compatible/Azure/GCP equivalent,
  with credentials already set up).
- `oc` configured against the target cluster.

### 1. Enable CBT on the HyperConverged Operator (HCO)

Two separate HCO configurations are needed.

Enable the `incrementalBackup` feature gate:

```bash
oc patch hyperconverged kubevirt-hyperconverged -n openshift-cnv --type merge -p '
spec:
  featureGates:
    incrementalBackup: true
'
```

This also turns on the `IncrementalBackup` and `UtilityVolumes` feature gates on the
underlying KubeVirt CR.

Enable the CBT label selector. This field lives on the KubeVirt CR, which HCO manages, so it
has to go through a jsonpatch annotation on the HCO CR instead of a direct field:

```bash
oc annotate hyperconverged kubevirt-hyperconverged -n openshift-cnv --overwrite \
  kubevirt.kubevirt.io/jsonpatch='[{"op":"add","path":"/spec/configuration/changedBlockTrackingLabelSelectors","value":{"virtualMachineLabelSelector":{"matchLabels":{"changedBlockTracking":"true"}}}}]'
```

Verify it took effect:

```bash
oc get kubevirt kubevirt-hyperconverged -n openshift-cnv \
  -o jsonpath='{.spec.configuration.changedBlockTrackingLabelSelectors}'
```

Expected output:

```json
{"virtualMachineLabelSelector":{"matchLabels":{"changedBlockTracking":"true"}}}
```

### 2. Configure the DataProtectionApplication (DPA)

The DPA needs both the `kubevirt` and `kubevirt-datamover` default plugins. Adding
`kubevirt-datamover` is what causes the OADP operator to deploy the
`kubevirt-datamover-plugin` (as a Velero init container) and the
`kubevirt-datamover-controller` `Deployment` from this repo.

```yaml
apiVersion: oadp.openshift.io/v1alpha1
kind: DataProtectionApplication
metadata:
  name: velero-test
  namespace: openshift-adp
spec:
  configuration:
    velero:
      defaultPlugins:
      - openshift
      - csi
      - aws
      - kubevirt
      - kubevirt-datamover
    nodeAgent:
      enable: true
      uploaderType: kopia
  backupLocations:
  - velero:
      provider: aws
      default: true
      objectStorage:
        bucket: <YOUR_BUCKET>
        prefix: velero
      config:
        region: <YOUR_REGION>
      credential:
        name: cloud-credentials
        key: cloud
```

Confirm the controller deployed:

```bash
oc get deployment -n openshift-adp | grep datamover
oc get pods -n openshift-adp | grep datamover
```

### 3. Deploy a test VM with the CBT label

Using the CirrOS sample VM (small, boots fast) as an example:

```bash
oc apply -f cirros-vm-cbt.yaml
oc get vm -n cirros-test cirros-test -w
```

Wait for `status.printableStatus` to reach `Running`.

### 4. Verify CBT is enabled on the VM

With KubeVirt `>= v1.8.2` and the feature gate and label selector from step 1 in place, CBT
activates the first time the VM boots, no manual restart needed.

```bash
oc get vm cirros-test -n cirros-test -o jsonpath='{.status.changedBlockTracking.state}'
```

Expected output: `Enabled`. If it isn't (older KubeVirt, or CBT didn't activate on boot),
trigger it with a stop/start cycle:

```bash
virtctl stop cirros-test -n cirros-test
oc wait vm cirros-test -n cirros-test --for=jsonpath='{.status.printableStatus}'=Stopped --timeout=5m

virtctl start cirros-test -n cirros-test
oc wait vm cirros-test -n cirros-test --for=jsonpath='{.status.printableStatus}'=Running --timeout=5m
```

### 5. Create the volume policy

This tells Velero to route the VM's PVCs to the KubeVirt datamover plugin's custom action
instead of a CSI snapshot:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kubevirt-volume-policy
  namespace: openshift-adp
data:
  policy.yaml: |
    version: v1
    volumePolicies:
      - conditions: {}
        action:
          type: custom
          parameters:
            datamover: kubevirt
```

```bash
oc apply -f volume-policy.yaml
```

### 6. Run the backup

```yaml
apiVersion: velero.io/v1
kind: Backup
metadata:
  name: kubevirt-dm-backup-1
  namespace: openshift-adp
spec:
  includedNamespaces:
  - cirros-test
  defaultVolumesToFsBackup: false
  snapshotMoveData: true
  resourcePolicy:
    kind: ConfigMap
    name: kubevirt-volume-policy
```

```bash
oc apply -f backup-cirros.yaml
oc get backup kubevirt-dm-backup-1 -n openshift-adp -w
```

While it's running, you can confirm the datamover path is active by watching for the CRs this
controller creates:

```bash
oc get virtualmachinebackuptrackers -A
oc get virtualmachinebackups -A
oc get datauploads -n openshift-adp
```

Confirm it finished:

```bash
oc get backup kubevirt-dm-backup-1 -n openshift-adp -o jsonpath='{.status.phase}'
```

Expected output: `Completed`.

### 7. Run a second backup to confirm incremental behavior

Run another backup against the same VM (a new `Backup` CR pointing at the same namespace).
Check the checkpoint index in your bucket at
`<bsl-prefix>-kubevirt-datamover/checkpoints/cirros-test/cirros-test/index.json` to confirm
the second backup was recorded as `"type": "incremental"` with a `parent` pointing at the
first checkpoint. That confirms the checkpoint chain logic described in
[`docs/architecture.md`](architecture.md#object-storage-layout-and-the-checkpoint-chain) is
working end to end.

### 8. Restore the VM

Delete (or scale down) the original VM's namespace, or restore into a namespace mapping, then
create a `Restore` pointing at the backup:

```yaml
apiVersion: velero.io/v1
kind: Restore
metadata:
  name: kubevirt-dm-restore-1
  namespace: openshift-adp
spec:
  backupName: kubevirt-dm-backup-1
```

```bash
oc apply -f restore-cirros.yaml
oc get restore kubevirt-dm-restore-1 -n openshift-adp -w
```

Watch the download side the same way as the upload side:

```bash
oc get datadownloads -n openshift-adp
```

Once the restore completes, confirm the VM comes back up and its data is intact:

```bash
oc get vm cirros-test -n cirros-test -o jsonpath='{.status.printableStatus}'
```

### Cleanup

```bash
oc delete restore kubevirt-dm-restore-1 -n openshift-adp
oc delete backup kubevirt-dm-backup-1 -n openshift-adp
oc delete configmap kubevirt-volume-policy -n openshift-adp
oc delete namespace cirros-test
```

## Other sample VMs

The same oadp-operator sample directory also has manifests for a Fedora and a CentOS Stream
10 VM, both running a todolist/MariaDB workload, useful for testing backup/restore of a VM
with an actual application and database rather than an empty CirrOS image. They follow the
same steps as above, just against a different namespace and VM name; see the `README.md` in
that directory for the exact file names and target namespaces.

## Debugging tips

- The datamover and downloader pods are short-lived and get cleaned up automatically after a
  successful backup or restore, but you don't need to catch them mid-flight to see their
  output. The controller streams each pod's logs into its own log output (as
  `"Datamover pod log"` entries with the source pod name) right before it removes the pod, so
  the pod's own output is always available afterward from the controller manager's logs.
- A stuck `DataUpload`/`DataDownload` will eventually fail once `spec.operationTimeout`
  elapses. If you want a phase to sit and let you inspect it (VMB status, PVC state, and so
  on), watch the phase transitions with `oc get dataupload <name> -n openshift-adp -w -o
  jsonpath='{.status.phase}{"\n"}'` rather than deleting resources mid-flight.
- If a backup or restore fails, check `oc get events -n openshift-adp --sort-by=.lastTimestamp`
  and the controller manager's own logs:

  ```bash
  oc logs -n openshift-adp deployment/kubevirt-datamover-controller-manager
  ```

  This single log stream has both the reconciler's own phase-transition and failure-reason
  messages and the datamover/downloader pod's forwarded output, since the controller emits
  the pod's log lines into its own logger before deleting the pod. Cross-reference the
  reconciliation phases described in [`docs/architecture.md`](architecture.md) to figure out
  which step failed.
