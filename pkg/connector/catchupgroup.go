package connector

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// catchUpPortals is the fallback when catch_up_user gives up: catch_up_group per chat, else a
// resync whose forward backfill fills the gap from list_topics.
func (c *GChatClient) catchUpPortals(ctx context.Context) {
	log := zerolog.Ctx(ctx)
	if !c.portalFallbackDue() {
		log.Debug().Msg("googlechat: per-chat catch-up ran recently, not repeating it")
		return
	}
	portals, err := c.loginPortals(ctx)
	if err != nil {
		log.Err(err).Msg("googlechat: failed to list portals for the per-chat catch-up")
		return
	}
	for _, portal := range portals {
		group, err := gcid.ParsePortalID(portal.ID)
		if err != nil {
			continue
		}
		var revision int64
		if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil {
			revision = meta.Revision
		}
		if revision > 0 && c.catchUpGroup(ctx, group, revision) {
			continue
		}
		// A current LatestMessageTS makes bridgev2 run a forward backfill for this portal.
		c.queueChatResync(&simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventChatResync,
				PortalKey: portal.PortalKey,
			},
			LatestMessageTS: time.Now(),
		})
		log.Debug().Str("gcid", group.ID).Msg("googlechat: queued a resync to gap-fill a chat after catch-up gave up")
	}
}

// catchUpGroup drains catch_up_group for one chat from revision; false means the caller must resync instead.
func (c *GChatClient) catchUpGroup(ctx context.Context, group gcid.GroupID, revision int64) bool {
	log := zerolog.Ctx(ctx)
	fetch := c.catchUpGroupFn
	if fetch == nil {
		conn := c.getConn()
		if conn == nil {
			return false
		}
		fetch = conn.CatchUpGroup
	}
	cursor := revision
	for page := 0; page < catchUpMaxPages; page++ {
		if err := c.paceGapFill(ctx); err != nil {
			return false
		}
		resp, err := fetch(ctx, &pb.CatchUpGroupRequest{
			GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
			Range:   &pb.CatchUpRange{FromRevisionTimestamp: proto.Int64(cursor)},
		})
		if err != nil {
			log.Err(err).Str("gcid", group.ID).Msg("googlechat: catch_up_group failed")
			return false
		}
		status := resp.GetStatus()
		if status != pb.CatchUpResponse_COMPLETED && status != pb.CatchUpResponse_PAGINATED {
			log.Warn().Str("gcid", group.ID).Str("status", status.String()).Msg("googlechat: catch_up_group did not complete")
			return false
		}
		pageStart := cursor
		for _, evt := range resp.GetEvents() {
			if r := groupRevision(evt); r > cursor {
				cursor = r
			}
			for _, flat := range gchatmeow.SplitEventBodies(evt) {
				if res := c.handleGChatEvent(ctx, flat); !res.Success {
					return false
				}
			}
		}
		if status == pb.CatchUpResponse_COMPLETED {
			return true
		}
		if cursor <= pageStart {
			return false
		}
	}
	return false
}

// loginPortals returns the bridged portals this login is in; listPortalsFn is a test seam.
func (c *GChatClient) loginPortals(ctx context.Context) ([]*bridgev2.Portal, error) {
	if c.listPortalsFn != nil {
		return c.listPortalsFn(ctx)
	}
	if c.UserLogin == nil || c.UserLogin.Bridge == nil {
		return nil, nil
	}
	userPortals, err := c.UserLogin.Bridge.DB.UserPortal.GetAllForLogin(ctx, c.UserLogin.UserLogin)
	if err != nil {
		return nil, err
	}
	portals := make([]*bridgev2.Portal, 0, len(userPortals))
	for _, up := range userPortals {
		portal, err := c.UserLogin.Bridge.GetExistingPortalByKey(ctx, up.Portal)
		if err != nil {
			return nil, err
		}
		if portal != nil && portal.MXID != "" {
			portals = append(portals, portal)
		}
	}
	return portals, nil
}

// beginCatchUp returns the revision to replay from. It stays stored until a catch-up finishes, because
// live events move the Revision watermark past a gap that a failed or interrupted drain has not replayed.
func (c *GChatClient) beginCatchUp(ctx context.Context) int64 {
	var from int64
	err := c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		if meta.CatchUpFrom > 0 && meta.CatchUpFrom <= meta.Revision {
			from = meta.CatchUpFrom
			return false
		}
		from = meta.Revision
		meta.CatchUpFrom = meta.Revision
		return meta.Revision > 0
	})
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to persist the pending catch-up revision")
	}
	return from
}

// endCatchUp clears the pending catch-up revision after a completed drain or per-chat fallback.
func (c *GChatClient) endCatchUp(ctx context.Context) {
	err := c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		if meta.CatchUpFrom == 0 {
			return false
		}
		meta.CatchUpFrom = 0
		return true
	})
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to clear the pending catch-up revision")
	}
}

// portalFallbackMinInterval keeps a revision the server keeps refusing from redoing the fallback on every reconnect.
const portalFallbackMinInterval = time.Hour

// portalFallbackDue reports whether the per-chat fallback may run now, and claims the slot if so.
func (c *GChatClient) portalFallbackDue() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.lastPortalFallback.IsZero() && time.Since(c.lastPortalFallback) < portalFallbackMinInterval {
		return false
	}
	c.lastPortalFallback = time.Now()
	return true
}
