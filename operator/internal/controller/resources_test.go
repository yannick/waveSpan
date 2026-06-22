package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	dbv1alpha1 "github.com/cwire/wavespan/operator/api/v1alpha1"
)

func sampleCluster() *dbv1alpha1.WaveSpanCluster {
	c := &dbv1alpha1.WaveSpanCluster{}
	c.Name = "demo"
	c.Namespace = "ns"
	c.Spec = dbv1alpha1.WaveSpanClusterSpec{
		Image: "wavespan/node:dev", Replicas: 3, ClusterID: "prod",
		Storage:    dbv1alpha1.StorageSpec{VolumeSize: "50Gi", StorageClassName: "fast"},
		Gateway:    dbv1alpha1.GatewaySpec{Enabled: true, Replicas: 2},
		Disruption: dbv1alpha1.DisruptionSpec{MaxUnavailable: 1},
	}
	return c
}

func TestBuildStatefulSet(t *testing.T) {
	ss := BuildStatefulSet(sampleCluster())
	if *ss.Spec.Replicas != 3 {
		t.Fatalf("replicas = %d", *ss.Spec.Replicas)
	}
	if ss.Spec.ServiceName != "demo-peers" {
		t.Fatalf("serviceName = %q", ss.Spec.ServiceName)
	}
	ctr := ss.Spec.Template.Spec.Containers[0]
	if ctr.Image != "wavespan/node:dev" {
		t.Fatalf("image = %q", ctr.Image)
	}
	ports := map[string]int32{}
	for _, p := range ctr.Ports {
		ports[p.Name] = p.ContainerPort
	}
	if ports["gossip"] != 7700 || ports["data"] != 7800 || ports["admin"] != 7900 {
		t.Fatalf("ports wrong: %+v", ports)
	}
	// env: kubernetes runtime + member id from pod name + advertise host
	env := map[string]corev1.EnvVar{}
	for _, e := range ctr.Env {
		env[e.Name] = e
	}
	if env["WAVESPAN_RUNTIME"].Value != "kubernetes" {
		t.Fatal("runtime env not kubernetes")
	}
	if env["WAVESPAN_MEMBER_ID"].ValueFrom == nil || env["WAVESPAN_MEMBER_ID"].ValueFrom.FieldRef.FieldPath != "metadata.name" {
		t.Fatal("member id should come from pod name fieldRef")
	}
	// volumeClaimTemplate sized from spec with storage class
	if len(ss.Spec.VolumeClaimTemplates) != 1 {
		t.Fatal("expected one volumeClaimTemplate")
	}
	vct := ss.Spec.VolumeClaimTemplates[0]
	if got := vct.Spec.Resources.Requests.Storage().String(); got != "50Gi" {
		t.Fatalf("PVC size = %q", got)
	}
	if vct.Spec.StorageClassName == nil || *vct.Spec.StorageClassName != "fast" {
		t.Fatal("storage class not set")
	}
}

func TestTopologySpreadScheduleAnyway(t *testing.T) {
	ss := BuildStatefulSet(sampleCluster())
	tsc := ss.Spec.Template.Spec.TopologySpreadConstraints
	if len(tsc) != 2 {
		t.Fatalf("expected hostname + zone spread, got %d", len(tsc))
	}
	for _, c := range tsc {
		if c.WhenUnsatisfiable != corev1.ScheduleAnyway {
			t.Fatalf("topology spread must be ScheduleAnyway (spot-heavy), got %s on %s", c.WhenUnsatisfiable, c.TopologyKey)
		}
	}
	if tsc[0].TopologyKey != "kubernetes.io/hostname" || tsc[0].MaxSkew != 1 {
		t.Fatalf("hostname constraint wrong: %+v", tsc[0])
	}
	if tsc[1].MaxSkew != 2 {
		t.Fatalf("zone maxSkew should be 2: %+v", tsc[1])
	}
}

func TestBuildHeadlessService(t *testing.T) {
	svc := BuildHeadlessService(sampleCluster())
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatal("peer service must be headless (ClusterIP: None)")
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Fatal("headless service must publish not-ready addresses for gossip form-up")
	}
	if svc.Name != "demo-peers" {
		t.Fatalf("headless service name = %q", svc.Name)
	}
}

func TestBuildPDB(t *testing.T) {
	pdb := BuildPodDisruptionBudget(sampleCluster())
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 {
		t.Fatalf("PDB maxUnavailable should be 1: %+v", pdb.Spec.MaxUnavailable)
	}
}

func TestBuildGateway(t *testing.T) {
	c := sampleCluster()
	dep := BuildGatewayDeployment(c)
	if *dep.Spec.Replicas != 2 {
		t.Fatalf("gateway replicas = %d", *dep.Spec.Replicas)
	}
	svc := BuildGatewayService(c)
	if svc.Name != "demo-gateway" {
		t.Fatalf("gateway service name = %q", svc.Name)
	}
}
