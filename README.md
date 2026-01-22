# KubeVirt Datamover Controller

This project provides a Kubernetes controller for handling incremental qcow2-based VM backups using KubeVirt/libvirt Changed Block Tracking (CBT) instead of CSI snapshots.

## Current Implementation Status

**Phase 1: Controller Scaffolding**
- [x] Controller watching DataUpload CRs
- [x] Filter for `Spec.DataMover == "kubevirt"`
- [x] Phase-based reconciliation logic (New, Accepted, Prepared, InProgress, Canceling)
- [x] MaxConcurrentReconciles configuration via CLI flag
- [x] RBAC manifests generated

**Phase 2: VMBT/VMB Creation (In Development)**
- [ ] Extract VirtualMachine reference from DataUpload annotation
- [ ] Create temporary PVC for backup output
- [ ] Create/update VirtualMachineBackupTracker (VMBT)
- [ ] Create VirtualMachineBackup (VMB) CR
- [ ] Monitor VMB status until completion

**Phase 3: Write to BSL (Pending)**
- [ ] Launch datamover pod with temp PVC mount and BSL credentials
- [ ] Upload qcow2 files to object storage
- [ ] Create/update checkpoint index.json

**Phase 4: Read from BSL (Pending)**
- [ ] Query checkpoint index before backup
- [ ] Validate checkpoint chain
- [ ] Handle missing/invalid checkpoints

**Phase 5: Cleanup & Completion (Pending)**
- [ ] Delete temporary PVC
- [ ] Update DataUpload status to Completed

## Design Overview

This controller enables **incremental qcow2-based VM backups** using KubeVirt/libvirt tooling instead of CSI snapshots:

| Aspect | CSI Approach | KubeVirt qcow2 Approach |
|--------|--------------|-------------------------|
| Layer | Storage (CSI driver) | Hypervisor (QEMU/libvirt) |
| Snapshot mechanism | CSI VolumeSnapshot | VirtualMachineBackup CR |
| Incremental | Kopia deduplication (scans whole volume) | True block-level CBT (only changed blocks) |
| Data mover | Velero node-agent + kopia | This controller + qemu-img |
| VM awareness | None (just sees PVC) | Full (knows it's a VM disk) |

For full design details, see the [OADP KubeVirt Datamover Design Document](https://github.com/openshift/oadp-operator/blob/oadp-dev/docs/design/kubevirt-datamover.md).

## Prerequisites

- OpenShift cluster with OADP operator installed
- KubeVirt with Changed Block Tracking (CBT) enabled
- Virtual machines with `status.ChangedBlockTracking: Enabled`
- oc CLI configured to access the cluster

## Deployment and Testing

### 1. Verify OADP Setup

Before deploying the controller, ensure OADP is properly configured:

```bash
# Check OADP operator is installed
oc get csv -n openshift-adp | grep oadp

# Verify DataProtectionApplication (DPA) is configured
oc get dpa -n openshift-adp

# Check BackupStorageLocation is ready
oc get bsl -n openshift-adp
```

### 2. Deploy the Controller

#### Option A: Run Locally (Development)
```bash
# Install CRDs to the cluster (if any)
make install

# Run controller locally (recommended for testing)
make run
```

#### Option B: Deploy to OpenShift Cluster
```bash
# Build and deploy the controller
make docker-build docker-push IMG=<your-registry>/kubevirt-datamover-controller:latest
make deploy IMG=<your-registry>/kubevirt-datamover-controller:latest

# Check deployment status
oc get pods -n kubevirt-datamover-system
```

#### Option C: Deploy to ttl.sh (Temporary Testing)
```bash
# Build for amd64 and push to ttl.sh (expires in 1 hour)
docker build --platform linux/amd64 -t ttl.sh/kubevirt-datamover-controller:1h .
docker push ttl.sh/kubevirt-datamover-controller:1h

# Deploy using the ttl.sh image
make deploy IMG=ttl.sh/kubevirt-datamover-controller:1h

# Check deployment
oc get pods -n kubevirt-datamover-system
```

### 3. Testing with DataUpload Resources

The controller watches Velero DataUpload resources where `spec.datamover: kubevirt`.

**Note**: Currently, Velero's built-in DataUpload controller also processes these resources. Upstream changes are required for Velero to skip reconciling when `spec.datamover` is set to an external value.

#### Create a Test DataUpload

```yaml
apiVersion: velero.io/v2alpha1
kind: DataUpload
metadata:
  name: test-kubevirt-du
  namespace: openshift-adp
spec:
  datamover: kubevirt
  snapshotType: kubevirt
  sourceNamespace: my-vm-namespace
  operationTimeout: 4h0m0s
  csiSnapshot:
    volumeSnapshot: ""
    storageClass: ""
    snapshotClass: ""
  backupStorageLocation: default
  sourceTargetPVC:
    namespace: my-vm-namespace
    name: my-vm-pvc
```

#### Monitor Controller Activity

```bash
# Watch controller logs
oc logs -f -n kubevirt-datamover-system deployment/kubevirt-datamover-controller-manager

# Watch DataUpload status
oc get datauploads -n openshift-adp -w

# Check DataUpload details
oc get dataupload test-kubevirt-du -n openshift-adp -o yaml
```

### 4. Configuration Options

The controller supports the following CLI flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--metrics-bind-address` | `0` | Address for metrics endpoint (`:8443` for HTTPS, `:8080` for HTTP) |
| `--health-probe-bind-address` | `:8081` | Address for health probe endpoint |
| `--leader-elect` | `false` | Enable leader election for HA |
| `--max-concurrent-reconciles` | `3` | Maximum concurrent DataUpload reconciliations |
| `--metrics-secure` | `true` | Serve metrics via HTTPS |

### 5. Troubleshooting

#### Controller Pod Not Starting
```bash
# Check pod status
oc describe pod -n kubevirt-datamover-system -l control-plane=controller-manager

# Check events
oc get events -n kubevirt-datamover-system --sort-by='.lastTimestamp'
```

#### DataUpload Not Being Processed
```bash
# Verify datamover field is set correctly (lowercase!)
oc get dataupload <name> -n openshift-adp -o jsonpath='{.spec.datamover}'

# Check controller is watching
oc logs -n kubevirt-datamover-system deployment/kubevirt-datamover-controller-manager | grep -i kubevirt
```

#### Architecture Mismatch (Exec Format Error)
If running on an amd64 cluster but built on arm64 Mac:
```bash
# Rebuild with correct platform
docker build --platform linux/amd64 -t <image> .
docker push <image>

# Use unique tag to avoid cached images
docker build --platform linux/amd64 -t ttl.sh/kubevirt-datamover-controller:amd64-$(date +%s) .
```

### 6. Development Commands

```bash
# Run tests
make test

# Build locally
make build

# Generate manifests after API changes
make manifests generate

# Format and lint code
make fmt vet lint

# Run locally against cluster
make run
```

## Kubebuilder

The project was generated using kubebuilder version `v4.6.0`, running the following commands:
```sh
kubebuilder init \
    --plugins go.kubebuilder.io/v4 \
    --project-version 3 \
    --project-name=kubevirt-datamover-controller \
    --repo=github.com/migtools/kubevirt-datamover-controller \
    --domain=openshift.io

# Note: This controller watches Velero's DataUpload CRD rather than defining its own
```

## Related Resources

- [OADP Operator](https://github.com/openshift/oadp-operator)
- [KubeVirt Incremental Backup VEP](https://github.com/kubevirt/enhancements/blob/main/veps/sig-storage/incremental-backup.md)
- [Velero](https://velero.io/)
- [OADP Project Board](https://github.com/orgs/migtools/projects/7/views/20)
