// Copyright 2026 PingCAP, Inc.
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

package txn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

// TestTwoPCCommitReLocatesAfterSplit proves that a commit resolves a fresh
// region after a successful prewrite. The interceptor holds the response after
// TiKV has accepted prewrite, placing the split strictly between the two
// phases rather than relying on scheduler timing.
func TestTwoPCCommitReLocatesAfterSplit(t *testing.T) {
	var cluster testutils.Cluster
	store, err := mockstore.NewMockStore(
		mockstore.WithStoreType(mockstore.MockTiKV),
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			cluster = c
			mockstore.BootstrapWithSingleStore(c)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	key := kv.Key("z")
	prewriteDone := make(chan struct{})
	allowCommit := make(chan struct{})
	var pausePrewrite sync.Once

	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, txn.Set(key, []byte("committed")))
	txn.SetOption(kv.RPCInterceptor, interceptor.NewRPCInterceptor(
		"split-between-prewrite-and-commit",
		func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
			return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
				resp, err := next(target, req)
				if req.Type == tikvrpc.CmdPrewrite && err == nil {
					pausePrewrite.Do(func() {
						close(prewriteDone)
						<-allowCommit
					})
				}
				return resp, err
			}
		},
	))

	commitDone := make(chan error, 1)
	go func() { commitDone <- txn.Commit(context.Background()) }()

	select {
	case <-prewriteDone:
	case <-time.After(5 * time.Second):
		t.Fatal("prewrite did not reach TiKV")
	}

	parent, _, _, _ := cluster.GetRegionByKey(key)
	require.NotNil(t, parent)
	newRegionID := cluster.AllocID()
	newPeerID := cluster.AllocID()
	cluster.Split(parent.GetId(), newRegionID, []byte("m"), []uint64{newPeerID}, newPeerID)
	close(allowCommit)

	select {
	case err := <-commitDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not finish after the split")
	}

	reader, err := store.Begin()
	require.NoError(t, err)
	value, err := reader.Get(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, []byte("committed"), value.Value)
}
