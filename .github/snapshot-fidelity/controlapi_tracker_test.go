package controlapi

import (
	"context"
	"errors"
	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"os"
	"sync/atomic"
	"testing"
)

func TestLocalSuspendTrackerReplay(t *testing.T) {
	for _, tc := range []struct {
		name        string
		first       ateapi.SnapshotFidelity
		replacement ateapi.SnapshotFidelity
	}{
		{"same fidelity", ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
		{"memory to rootfs", ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS},
		{"rootfs to memory", ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			w, atelet := newWireCaptureWorkflow(t, persistence)
			address := os.Getenv("SUBSTRATE_TRACKER_ADDR")
			if address == "" {
				t.Fatal("SUBSTRATE_TRACKER_ADDR is required")
			}
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			trackerClient := ateletpb.NewAteletClient(conn)
			impl := newServiceImpl(persistence, nil)
			impl.clusterSnapshotLocation = testStorageLocation
			svc := &RPCService{impl: impl, actorWorkflow: w}
			ref := suspendTestTemplateRef()
			storetest.MustCreateAtespace(t, ctx, persistence, ref.GetAtespace())
			createTemplate := func(fidelity ateapi.SnapshotFidelity) *ateapi.ActorTemplate {
				t.Helper()
				tmpl, err := svc.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
					tmpl.Metadata = &ateapi.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()}
					tmpl.SnapshotConfig.PreferredFidelity = fidelity
					tmpl.SnapshotConfig.StorageLocation = testStorageLocation
				})})
				if err != nil {
					t.Fatalf("CreateActorTemplate: %v", err)
				}
				return tmpl
			}
			originalTemplate := createTemplate(tc.first)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: ref,
				Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING, AssignedNode: wireTestAssignment().GetNodeName(), WorkerAssignment: wireTestAssignment()},
			})
			seedWireWorker(t, ctx, persistence, assignmentOf(actor))
			firstCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			var checkpoints atomic.Int32
			atelet.onCheckpoint = func(callCtx context.Context) error {
				response, err := trackerClient.Checkpoint(callCtx, atelet.checkpointRequest())
				if err != nil {
					if status.Code(err) == codes.FailedPrecondition {
						return apierror.FailedPrecondition("%s", status.Convert(err).Message())
					}
					return err
				}
				atelet.mu.Lock()
				atelet.checkpointState = response.GetState()
				atelet.mu.Unlock()
				if checkpoints.Add(1) == 1 {
					cancel()
					return apierror.Aborted("checkpoint committed; response lost")
				}
				return nil
			}
			request := &ateapi.SuspendActorRequest{Actor: actorRef.ToObjectRef()}
			if _, err := svc.SuspendActor(firstCtx, request); !errors.Is(err, context.Canceled) {
				t.Fatalf("first SuspendActor = %v, want cancellation", err)
			}
			first := atelet.checkpointRequest().GetSnapshot()
			pending := mustReadActor(t, ctx, persistence, actorRef)
			if pending.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDING {
				t.Fatalf("actor state = %v, want SUSPENDING", pending.GetStatus().GetState())
			}
			if _, err := svc.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref}); err != nil {
				t.Fatalf("DeleteActorTemplate: %v", err)
			}
			replacement := createTemplate(tc.replacement)
			if replacement.GetMetadata().GetUid() == originalTemplate.GetMetadata().GetUid() {
				t.Fatal("template recreation retained the old UID")
			}
			// Switching upload mode is supported while retrying the same checkpoint.
			request.Options = &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}
			if _, err := svc.SuspendActor(ctx, request); err != nil {
				t.Errorf("retried SuspendActor: %v", err)
			}
			retry := atelet.checkpointRequest()
			if diff := cmp.Diff(first, retry.GetSnapshot(), protocmp.Transform()); diff != "" {
				t.Errorf("retry changed the checkpoint descriptor (-first +retry):\n%s", diff)
			}
			if retry.GetUploadConfig().GetMode() != ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
				t.Errorf("retry upload mode = %v, want immediate", retry.GetUploadConfig().GetMode())
			}
			finished := mustReadActor(t, ctx, persistence, actorRef)
			t.Logf("first=%v retry=%v persisted_state=%v crash=%v", first.GetFidelity(), retry.GetSnapshot().GetFidelity(), finished.GetStatus().GetState(), finished.GetStatus().GetCrash())
			if finished.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
				t.Errorf("actor state = %v, want SUSPENDED", finished.GetStatus().GetState())
			}
			if snapshotAtLatestGeneration(finished.GetStatus()).GetFidelity() != tc.first {
				t.Errorf("retry changed persisted fidelity to %v", snapshotAtLatestGeneration(finished.GetStatus()).GetFidelity())
			}
		})
	}
}
