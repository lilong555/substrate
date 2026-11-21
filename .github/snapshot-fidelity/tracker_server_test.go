// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshottracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"k8s.io/utils/clock"
)

// This server synthesizes runtime completion and upload completion. Tracker
// admission, state transitions, and actor state files use production code.
type evidenceTrackerAtelet struct {
	ateletpb.UnimplementedAteletServer
	tracker *Tracker
	locks   *actorlock.Locks
	log     *os.File
	logMu   sync.Mutex
	seq     int
}

type evidenceCheckpointEvent struct {
	Sequence       int             `json:"sequence"`
	Time           time.Time       `json:"time"`
	Request        json.RawMessage `json:"request"`
	Response       json.RawMessage `json:"response,omitempty"`
	Committed      bool            `json:"committed"`
	Action         string          `json:"action"`
	GRPCCode       string          `json:"grpc_code"`
	Error          string          `json:"error,omitempty"`
	StateFile      string          `json:"state_file,omitempty"`
	PersistedState json.RawMessage `json:"persisted_state,omitempty"`
	Synthetic      bool            `json:"synthetic_runtime_completion"`
}

func (s *evidenceTrackerAtelet) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (resp *ateletpb.CheckpointResponse, err error) {
	request, marshalErr := protojson.Marshal(req)
	if marshalErr != nil {
		return nil, status.Error(codes.Internal, marshalErr.Error())
	}
	event := evidenceCheckpointEvent{Time: time.Now().UTC(), Request: request, Action: "rejected", Synthetic: true}
	defer func() {
		wireStatus, _ := apierror.FromError(err)
		err = wireStatus.Err()
		event.GRPCCode = wireStatus.Code().String()
		if err != nil {
			event.Error = wireStatus.Message()
		}
		if resp != nil {
			event.Response, marshalErr = protojson.Marshal(resp)
			if marshalErr != nil {
				err = status.Error(codes.Internal, marshalErr.Error())
			}
		}
		s.logMu.Lock()
		defer s.logMu.Unlock()
		s.seq++
		event.Sequence = s.seq
		if logErr := json.NewEncoder(s.log).Encode(event); logErr != nil {
			err = status.Errorf(codes.Internal, "write evidence log: %v", logErr)
			resp = nil
			return
		}
		if logErr := s.log.Sync(); logErr != nil {
			err = status.Errorf(codes.Internal, "sync evidence log: %v", logErr)
			resp = nil
		}
	}()

	uid := req.GetActorUid()
	if uid == "" || uid == "." || uid == ".." || strings.ContainsAny(uid, "/\\") || req.GetSnapshot() == nil {
		return nil, apierror.InvalidArgument("actor UID and snapshot are required")
	}
	mode := req.GetUploadConfig().GetMode()
	if mode != ateletpb.UploadMode_UPLOAD_MODE_DEFERRED && mode != ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
		return nil, apierror.InvalidArgument("unsupported upload mode %v", mode)
	}
	if !s.locks.Lock(ctx, uid) {
		return nil, ctx.Err()
	}
	defer s.locks.Unlock(uid)
	if err := os.MkdirAll(ateletpath.ActorPath(uid), 0o700); err != nil {
		return nil, err
	}
	event.StateFile = ateletpath.ActorSnapshotStateFile(uid)
	defer func() {
		data, readErr := os.ReadFile(event.StateFile)
		if readErr == nil {
			event.PersistedState = data
		} else if err == nil || !errors.Is(readErr, os.ErrNotExist) {
			err = fmt.Errorf("read persisted actor state: %w", readErr)
			resp = nil
		}
	}()
	committed, end, err := s.tracker.BeginCheckpoint(ctx, uid, req.GetSnapshot(), mode)
	if err != nil {
		return nil, err
	}
	defer end()
	event.Committed = committed
	if committed {
		event.Action = "committed_replay"
		if mode == ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
			// Preserve the original commit time when synthesizing a deferred upload.
			saved := readSnapshotState(uid)
			if saved == nil {
				return nil, fmt.Errorf("committed actor state is absent")
			}
			ref := saved.LatestLocal
			if ref == nil {
				ref = saved.LatestDurable
			}
			if ref == nil {
				return nil, fmt.Errorf("committed actor has no snapshot")
			}
			durable := *ref
			durable.UploadTime = time.Now().UTC()
			s.tracker.CommitDurable(uid, durable)
		}
		return &ateletpb.CheckpointResponse{State: s.tracker.CommittedCheckpointState(uid)}, nil
	}
	event.Action = "new_commit"
	ref := SnapshotRef{
		Snapshot: proto.CloneOf(req.GetSnapshot()), CommitTime: time.Now().UTC(),
		Actor: resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
	}
	if mode == ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
		durable := ref
		durable.UploadTime = time.Now().UTC()
		s.tracker.CommitDurable(uid, durable)
	}
	return &ateletpb.CheckpointResponse{State: s.tracker.CommitLocal(uid, ref)}, nil
}

func startEvidenceTracker(t *testing.T, logPath string) string {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	root := t.TempDir()
	origActors, origSnapshots := nodepath.ActorsDir, nodepath.SnapshotsDir
	nodepath.ActorsDir, nodepath.SnapshotsDir = filepath.Join(root, "actors"), filepath.Join(root, "snapshots")
	t.Cleanup(func() { nodepath.ActorsDir, nodepath.SnapshotsDir = origActors, origSnapshots })
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tracker := New(ctx, nil, clock.RealClock{})
	server := grpc.NewServer()
	ateletpb.RegisterAteletServer(server, &evidenceTrackerAtelet{tracker: tracker, locks: actorlock.New(), log: logFile})
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.GracefulStop()
		cancel()
		if err := <-served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
		if err := tracker.Wait(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return listener.Addr().String()
}

func TestEvidenceTrackerServer(t *testing.T) {
	readyPath, logPath := os.Getenv("SUBSTRATE_TRACKER_READY_FILE"), os.Getenv("SUBSTRATE_TRACKER_LOG_FILE")
	if readyPath == "" || logPath == "" {
		t.Fatal("SUBSTRATE_TRACKER_READY_FILE and SUBSTRATE_TRACKER_LOG_FILE are required")
	}
	ctx, stop := signal.NotifyContext(t.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	address := startEvidenceTracker(t, logPath)
	data, err := json.Marshal(map[string]string{"address": address})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readyPath+".tmp", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(readyPath+".tmp", readyPath); err != nil {
		t.Fatal(err)
	}
	t.Logf("tracker evidence server listening on %s; runtime and upload completion are synthetic", address)
	<-ctx.Done()
}

func TestEvidenceTrackerProtocol(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "requests.jsonl")
	address := startEvidenceTracker(t, logPath)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := ateletpb.NewAteletClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req := &ateletpb.CheckpointRequest{
		ActorUid: "evidence-actor", Atespace: "test", ActorName: "actor",
		Snapshot: &ateletpb.Snapshot{SnapshotUuid: "snapshot-1", Generation: 1,
			Fidelity: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:   &ateletpb.ObjectStorage{SnapshotUri: "gs://test/snapshot-1"}},
		UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
	}
	first, err := client.Checkpoint(ctx, req)
	if err != nil || first.GetState().GetLatestLocal() == nil || first.GetState().GetLatestDurable() != nil {
		t.Fatalf("deferred checkpoint = %v, %v", first, err)
	}
	req.UploadConfig.Mode = ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE
	replayed, err := client.Checkpoint(ctx, req)
	if err != nil || replayed.GetState().GetLatestDurable() == nil || !proto.Equal(first.GetState().GetLatestLocal(), replayed.GetState().GetLatestLocal()) {
		t.Fatalf("immediate replay = %v, %v", replayed, err)
	}
	drift := proto.CloneOf(req)
	drift.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	if _, err := client.Checkpoint(ctx, drift); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("descriptor drift = %v, want FailedPrecondition", err)
	}
	again, err := client.Checkpoint(ctx, req)
	if err != nil || !proto.Equal(replayed, again) {
		t.Fatalf("healthy replay = %v, %v; want %v", again, err, replayed)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 4 {
		t.Fatalf("event count = %d, want 4", len(lines))
	}
	for i, action := range []string{"new_commit", "committed_replay", "rejected", "committed_replay"} {
		var event evidenceCheckpointEvent
		if err := json.Unmarshal([]byte(lines[i]), &event); err != nil {
			t.Fatal(err)
		}
		if event.Action != action || len(event.PersistedState) == 0 {
			t.Fatalf("event %d = %+v", i, event)
		}
	}
}
