package connector

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/prototext"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// loadListTopics reads a text-format ListTopicsResponse fixture from testdata.
func loadListTopics(t *testing.T, name string) *pb.ListTopicsResponse {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	resp := &pb.ListTopicsResponse{}
	if err := prototext.Unmarshal(raw, resp); err != nil {
		t.Fatalf("parse fixture %s: %v", name, err)
	}
	return resp
}

// fixtureFetch runs FetchMessages against a fixture and returns the response and the request sent.
func fixtureFetch(t *testing.T, fixture string, portal *bridgev2.Portal, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, *pb.ListTopicsRequest) {
	t.Helper()
	var sent *pb.ListTopicsRequest
	gc := newBackfillTestClient("1001", func(_ context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		sent = req
		return loadListTopics(t, fixture), nil
	})
	params.Portal = portal
	resp, err := gc.FetchMessages(context.Background(), params)
	if err != nil {
		t.Fatalf("FetchMessages: %v", err)
	}
	return resp, sent
}

func backfillIDs(resp *bridgev2.FetchMessagesResponse) []string {
	ids := make([]string, len(resp.Messages))
	for i, m := range resp.Messages {
		ids[i] = string(m.ID)
	}
	return ids
}

func sameIDs(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func threadedBackfillPortal() *bridgev2.Portal {
	portal := flatBackfillPortal(gcid.GroupID{ID: "space-1"})
	portal.Metadata = &PortalMetadata{ThreadsEnabled: true}
	return portal
}

func anchorAt(id string, micros int64) *database.Message {
	return &database.Message{ID: gcid.MakeMessageID(id), Timestamp: gchatmeow.MicrosToTime(micros)}
}

// TestBackfillThreadSendsHeadAndGapFillSendsReplies: history leaves replies to the thread backfill, a gap fill inlines them.
func TestBackfillThreadSendsHeadAndGapFillSendsReplies(t *testing.T) {
	resp, sent := fixtureFetch(t, "list_topics_threaded.txtpb", threadedBackfillPortal(), bridgev2.FetchMessagesParams{Count: 10})
	if got := backfillIDs(resp); !sameIDs(got, "topic-a", "topic-b") {
		t.Fatalf("history messages = %v, want the two topic heads", got)
	}
	head := resp.Messages[0]
	if !head.ShouldBackfillThread || head.LastThreadMessage != "reply-a2" || resp.Messages[1].ShouldBackfillThread {
		t.Errorf("thread head: ShouldBackfillThread=%v LastThreadMessage=%q, want true and reply-a2, and no flag on the flat topic", head.ShouldBackfillThread, head.LastThreadMessage)
	}
	if sent.GetPageSizeForReplies() != threadReplyPageSize || sent.GetPageSizeForTopics() != 10 {
		t.Errorf("request page sizes = %d topics / %d replies, want 10 / %d", sent.GetPageSizeForTopics(), sent.GetPageSizeForReplies(), threadReplyPageSize)
	}

	gap, _ := fixtureFetch(t, "list_topics_threaded.txtpb", threadedBackfillPortal(), bridgev2.FetchMessagesParams{
		Forward: true, Count: 10, AnchorMessage: anchorAt("older", 50),
	})
	if got := backfillIDs(gap); !sameIDs(got, "topic-a", "reply-a1", "reply-a2", "topic-b") {
		t.Fatalf("gap fill = %v, want every reply in chronological order", got)
	}
	if reply := gap.Messages[1]; reply.ThreadRoot == nil || *reply.ThreadRoot != "topic-a" {
		t.Errorf("reply ThreadRoot = %v, want topic-a", reply.ThreadRoot)
	}
	for _, m := range gap.Messages {
		if m.ShouldBackfillThread {
			t.Errorf("gap-fill message %s asks for a thread backfill; its replies are already inline", m.ID)
		}
	}
}

func TestBackfillHasMoreFollowsContainsFirstTopic(t *testing.T) {
	resp, sent := fixtureFetch(t, "list_topics_first_page.txtpb", flatBackfillPortal(gcid.GroupID{ID: "dm-1", IsDM: true}), bridgev2.FetchMessagesParams{Count: 10})
	if resp.HasMore || !sameIDs(backfillIDs(resp), "dm-1", "dm-2") {
		t.Errorf("first page: HasMore=%v messages=%v, want false with both messages", resp.HasMore, backfillIDs(resp))
	}
	if sent.PageSizeForReplies != nil {
		t.Error("the request for a flat chat sets page_size_for_replies, want its old shape")
	}

	resp, _ = fixtureFetch(t, "list_topics_threaded.txtpb", threadedBackfillPortal(), bridgev2.FetchMessagesParams{Count: 10})
	if !resp.HasMore {
		t.Error("HasMore = false for a page that does not contain the first topic")
	}

	// With no page token, a call that adds nothing must end the queue instead of looping.
	resp, _ = fixtureFetch(t, "list_topics_threaded.txtpb", threadedBackfillPortal(), bridgev2.FetchMessagesParams{
		Count: 10, AnchorMessage: anchorAt("topic-a", 100),
	})
	if resp.HasMore || len(resp.Messages) != 0 {
		t.Errorf("no progress: HasMore=%v messages=%v, want false and none", resp.HasMore, backfillIDs(resp))
	}
}

// TestForwardBackfillFillsTheGapAfterTheAnchor: what arrived while the bridge was down, replies included.
func TestForwardBackfillFillsTheGapAfterTheAnchor(t *testing.T) {
	resp, _ := fixtureFetch(t, "list_topics_threaded.txtpb", threadedBackfillPortal(), bridgev2.FetchMessagesParams{
		Forward: true, Count: 10, AnchorMessage: anchorAt("reply-a1", 200),
	})
	if got := backfillIDs(resp); !sameIDs(got, "reply-a2", "topic-b") {
		t.Errorf("gap fill = %v, want only what is newer than the anchor", got)
	}
	if !resp.Forward || resp.HasMore {
		t.Errorf("Forward=%v HasMore=%v, want a forward batch with no more", resp.Forward, resp.HasMore)
	}
}

func TestBackwardBackfillKeepsOnlyWhatIsOlderThanTheAnchor(t *testing.T) {
	resp, _ := fixtureFetch(t, "list_topics_threaded.txtpb", threadedBackfillPortal(), bridgev2.FetchMessagesParams{
		Count: 10, AnchorMessage: anchorAt("reply-a2", 300),
	})
	if got := backfillIDs(resp); !sameIDs(got, "topic-a") {
		t.Errorf("backward batch = %v, want only what is older than the anchor", got)
	}
}

// TestForwardGapFillsAreSpacedOut: the startup resync of many chats must not send a burst of list_topics.
func TestForwardGapFillsAreSpacedOut(t *testing.T) {
	var calls []time.Time
	gc := newBackfillTestClient("1001", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		calls = append(calls, time.Now())
		return &pb.ListTopicsResponse{}, nil
	})
	gc.gapFillSpacing = 100 * time.Millisecond
	fetch := func(forward bool) {
		t.Helper()
		_, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
			Portal: threadedBackfillPortal(), Forward: forward, Count: 5, AnchorMessage: anchorAt("m", 1),
		})
		if err != nil {
			t.Fatalf("FetchMessages: %v", err)
		}
	}
	fetch(true)
	fetch(true)
	fetch(true)
	if gap := calls[2].Sub(calls[0]); gap < 150*time.Millisecond {
		t.Errorf("three gap fills took %v, want about two 100ms spacings", gap)
	}
	start := time.Now()
	fetch(false)
	if took := time.Since(start); took > 90*time.Millisecond {
		t.Errorf("a backward fetch waited %v, want it unpaced", took)
	}
}
