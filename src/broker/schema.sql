-- Broker coordination.db schema
-- Keep persisted schemas and idempotent migrations in broker.server aligned.

-- agent_status: per-agent registration + current status (sole writer = broker)
-- v1.3.1 (D29): display_name TEXT — optional human-readable label that consumers
-- (e.g., claude-relay) can supply at status POST so the dashboard's Agent Messages
-- panel can render friendly names alongside UUID-shaped agent ids. Migration for
-- existing coordination.db files is handled in server.ensure_schema via PRAGMA
-- table_info detect + ALTER TABLE ADD COLUMN (mirrors D27 pattern).
-- v1.5+ post-cwd-derive-revert (APPENDIX 2): project column REMOVED. Existing
-- DBs that were migrated to v1.5 carry an orphan column dropped by ensure_schema's
-- reverse migration (PRAGMA-detect + DROP COLUMN). Fresh installs never see it.
CREATE TABLE IF NOT EXISTS agent_status (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  channel TEXT NOT NULL,
  thread_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('idle','queued','running','blocked','done','failed')),
  current_task TEXT,
  capabilities TEXT,
  last_activity_ts INTEGER NOT NULL,
  registered_at INTEGER NOT NULL,
  display_name TEXT,
  relay_name TEXT,
  transport TEXT,
  binding_epoch TEXT,
  omp_process_epoch TEXT,
  omp_instance_id TEXT,
  control_epoch TEXT,
  control_token_hash TEXT,
  bind_attempt_id TEXT,
  omp_mutation_seq INTEGER NOT NULL DEFAULT 0
);
-- Composite index for cursor-paginated GET /api/state (iter-4 MED fix).
-- Without this, large agent_status table forces a full scan + sort per request.
CREATE INDEX IF NOT EXISTS idx_agent_status_registered_at_id
  ON agent_status(registered_at, id);

-- message_log: append-only event log + SSE source (broker is sole writer)
-- event_id is the SSE event id (per contract G1 unification)
CREATE TABLE IF NOT EXISTS message_log (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_event_id TEXT,
  from_agent TEXT NOT NULL,
  to_agent TEXT,
  channel TEXT NOT NULL,
  thread_id TEXT,
  kind TEXT NOT NULL,
  content TEXT NOT NULL,
  ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_message_log_ts ON message_log(ts);
CREATE INDEX IF NOT EXISTS idx_message_log_channel_thread ON message_log(channel, thread_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_message_log_client_event_id
  ON message_log(client_event_id)
  WHERE client_event_id IS NOT NULL;
-- v1.3: speed up GET /api/agent/{id}/messages cursor pagination per recipient
CREATE INDEX IF NOT EXISTS idx_message_log_to_agent_kind_event_id
  ON message_log(to_agent, kind, event_id);

-- v1.4 (D30): partial composite index for GET /api/agent-messages aggregate query.
-- Leading-equality on kind, range+ORDER BY on event_id, partial-WHERE bounds index size.
-- Index-perfect for: WHERE kind=? AND event_id>? AND to_agent IS NOT NULL ORDER BY event_id ASC LIMIT ?
CREATE INDEX IF NOT EXISTS idx_message_log_kind_event_id_to_agent
  ON message_log(kind, event_id)
  WHERE to_agent IS NOT NULL;

-- Stable delivery ledger. OMP address text is immutable audit data; legacy rows
-- are readdressed on rename, takeover fallback, and prune rehoming.
CREATE TABLE IF NOT EXISTS delivery (
  to_agent     TEXT    NOT NULL,
  event_id     INTEGER NOT NULL,
  status       TEXT    NOT NULL DEFAULT 'outstanding'
               CHECK (status IN ('outstanding','acked','abandoned','rejected')),
  attempts     INTEGER NOT NULL DEFAULT 0,
  next_push_ts INTEGER,
  created_ts   INTEGER NOT NULL,
  updated_ts   INTEGER NOT NULL,
  acked_ts     INTEGER,
  alerted_ts   INTEGER,
  sender_id    TEXT,
  recipient_id TEXT,
  omp_ack_seq INTEGER NOT NULL DEFAULT 0,
  rejected_ts  INTEGER,
  reject_reason_code TEXT
    CHECK (reject_reason_code IS NULL OR reject_reason_code IN
      ('USER_REJECTED','PROTOCOL_INVALID','RECIPIENT_POLICY_REFUSED',
       'PERSISTENCE_UNRECOVERABLE','DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN')),
  rejection_notice_event_id INTEGER,
  CHECK ((status='rejected') = (reject_reason_code IS NOT NULL)),
  PRIMARY KEY (to_agent, event_id)
);
CREATE INDEX IF NOT EXISTS idx_delivery_outstanding ON delivery(to_agent, status, event_id);
CREATE INDEX IF NOT EXISTS idx_delivery_next_push
  ON delivery(next_push_ts) WHERE status='outstanding';
CREATE INDEX IF NOT EXISTS idx_delivery_event_id ON delivery(event_id);
CREATE INDEX IF NOT EXISTS idx_delivery_recipient_status_event
  ON delivery(recipient_id, status, event_id);

-- Retained OMP relay-name authority. Renaming leaves the old namespace owned by
-- the same stable identity; only an authenticated R1 takeover can transfer it.
-- The table also lets rollback-era writers refuse silent reuse without changing
-- stable delivery rows.
CREATE TABLE IF NOT EXISTS omp_relay_namespace (
  relay_name TEXT PRIMARY KEY COLLATE NOCASE,
  owner_id TEXT NOT NULL,
  created_ts INTEGER NOT NULL,
  updated_ts INTEGER NOT NULL,
  CHECK (length(relay_name) BETWEEN 1 AND 64),
  CHECK (relay_name NOT GLOB '*[^A-Za-z0-9._-]*')
);

-- Namespace-scoped durable idempotency. Only the legacy namespace is TTL swept;
-- OMP bind/status/operator history is retained as protocol authority.
CREATE TABLE IF NOT EXISTS idempotency_ledger (
  namespace TEXT NOT NULL DEFAULT 'legacy',
  client_event_id TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  agent_id TEXT,
  response_event_id INTEGER,
  request_hash TEXT,
  response_status INTEGER,
  response_json TEXT,
  new_binding_epoch TEXT,
  created_ts INTEGER NOT NULL,
  PRIMARY KEY (namespace, client_event_id)
);
CREATE INDEX IF NOT EXISTS idx_idemp_created_ts ON idempotency_ledger(created_ts);
CREATE UNIQUE INDEX IF NOT EXISTS idx_idemp_omp_bind_epoch
  ON idempotency_ledger(new_binding_epoch)
  WHERE namespace='omp-bind' AND new_binding_epoch IS NOT NULL;

CREATE TABLE IF NOT EXISTS delivery_offer (
  recipient_id TEXT NOT NULL,
  event_id INTEGER NOT NULL,
  offer_token TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL CHECK (state IN ('offered','injected','accepted','rejected')),
  reason TEXT CHECK (reason IS NULL OR reason IN
    ('USER_REJECTED','PROTOCOL_INVALID','RECIPIENT_POLICY_REFUSED',
     'PERSISTENCE_UNRECOVERABLE','DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN')),
  omp_process_epoch TEXT,
  created_ts INTEGER NOT NULL,
  updated_ts INTEGER NOT NULL,
  CHECK ((state='rejected') = (reason IS NOT NULL)),
  PRIMARY KEY (recipient_id,event_id)
);
CREATE INDEX IF NOT EXISTS idx_delivery_offer_recipient_state_event
  ON delivery_offer(recipient_id,state,event_id);

CREATE TABLE IF NOT EXISTS omp_rejection_audit (
  audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT,
  actor_type TEXT NOT NULL CHECK (actor_type IN ('recipient','operator')),
  actor_id TEXT NOT NULL,
  route TEXT NOT NULL,
  recipient_id TEXT,
  event_id INTEGER NOT NULL,
  requested_reason_code TEXT NOT NULL,
  effective_reason_code TEXT NOT NULL CHECK (effective_reason_code IN
    ('USER_REJECTED','PROTOCOL_INVALID','RECIPIENT_POLICY_REFUSED',
     'PERSISTENCE_UNRECOVERABLE','DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN')),
  offer_token_present INTEGER NOT NULL CHECK (offer_token_present IN (0,1)),
  sender_notice TEXT NOT NULL CHECK (sender_notice IN ('created','unavailable')),
  rejection_notice_event_id INTEGER,
  created_ts INTEGER NOT NULL,
  CHECK ((actor_type='recipient' AND requested_reason_code='USER_REJECTED') OR
         (actor_type='operator' AND requested_reason_code IN
          ('PROTOCOL_INVALID','RECIPIENT_POLICY_REFUSED','PERSISTENCE_UNRECOVERABLE'))),
  CHECK ((sender_notice='created') = (rejection_notice_event_id IS NOT NULL)),
  UNIQUE(actor_type, request_id)
);

-- Persistent rollback protection. Legacy explicit-column delivery inserts bind
-- to the identity that owns the addressed relay name or exact stable ID at
-- insertion time only.
CREATE TRIGGER IF NOT EXISTS trg_delivery_fill_recipient_r1
AFTER INSERT ON delivery
WHEN NEW.recipient_id IS NULL AND NEW.status='outstanding'
BEGIN
  UPDATE delivery
     SET recipient_id=COALESCE(
       (SELECT owner_id FROM omp_relay_namespace
         WHERE relay_name=NEW.to_agent),
       (SELECT id FROM agent_status
         WHERE lower(relay_name)=lower(NEW.to_agent)),
       (SELECT id FROM agent_status WHERE id=NEW.to_agent))
   WHERE rowid=NEW.rowid;
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_binding_immutable_r1
BEFORE UPDATE OF to_agent,event_id,sender_id,recipient_id ON delivery
WHEN (NEW.to_agent<>OLD.to_agent AND (
       EXISTS (SELECT 1 FROM agent_status a
                WHERE a.id=OLD.recipient_id AND a.binding_epoch IS NOT NULL)
       OR EXISTS (SELECT 1 FROM delivery_offer o
                   WHERE o.recipient_id=OLD.recipient_id AND o.event_id=OLD.event_id)))
 OR NEW.event_id<>OLD.event_id
 OR NEW.sender_id IS NOT OLD.sender_id
 OR (OLD.recipient_id IS NOT NULL AND NEW.recipient_id IS NOT OLD.recipient_id)
 OR (OLD.recipient_id IS NULL AND NEW.recipient_id IS NOT NULL AND
     (OLD.status<>'outstanding' OR NOT EXISTS
       (SELECT 1 FROM agent_status a
         WHERE a.id=NEW.recipient_id AND (
           lower(a.relay_name)=lower(OLD.to_agent)
           OR a.id=OLD.to_agent
           OR a.id=(SELECT owner_id FROM omp_relay_namespace
                     WHERE relay_name=OLD.to_agent)))))
BEGIN
  SELECT CASE WHEN NEW.to_agent<>OLD.to_agent
                   AND NEW.event_id=OLD.event_id
                   AND NEW.sender_id IS OLD.sender_id
                   AND NEW.recipient_id IS OLD.recipient_id
    THEN RAISE(IGNORE)
    ELSE RAISE(ABORT,'omp_delivery_binding_immutable') END;
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_terminal_immutable_r1
BEFORE UPDATE OF status,acked_ts,rejected_ts,reject_reason_code,rejection_notice_event_id
ON delivery
WHEN (OLD.status IN ('acked','rejected') AND NEW.status<>OLD.status)
 OR (OLD.status='acked' AND NEW.acked_ts IS NOT OLD.acked_ts)
 OR (OLD.status='rejected' AND
     (NEW.rejected_ts IS NOT OLD.rejected_ts
      OR NEW.reject_reason_code IS NOT OLD.reject_reason_code
      OR (OLD.rejection_notice_event_id IS NOT NULL AND
          NEW.rejection_notice_event_id IS NOT OLD.rejection_notice_event_id)))
BEGIN
  SELECT CASE WHEN OLD.status='rejected' AND NEW.status='acked'
    THEN RAISE(IGNORE)
    ELSE RAISE(ABORT,'omp_delivery_terminal_immutable') END;
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_omp_insert_guard_r1
BEFORE INSERT ON delivery
WHEN EXISTS (
       SELECT 1 FROM agent_status a
        WHERE a.binding_epoch IS NOT NULL AND (
          lower(a.relay_name)=lower(NEW.to_agent) OR a.id=(
            SELECT owner_id FROM omp_relay_namespace
             WHERE relay_name=NEW.to_agent)))
 AND (NEW.status<>'outstanding' OR
      (NEW.recipient_id IS NOT NULL AND NOT EXISTS
       (SELECT 1 FROM agent_status a
         WHERE a.id=NEW.recipient_id AND a.binding_epoch IS NOT NULL
           AND (lower(a.relay_name)=lower(NEW.to_agent) OR a.id=(
             SELECT owner_id FROM omp_relay_namespace
              WHERE relay_name=NEW.to_agent)))))
BEGIN
  SELECT RAISE(ABORT,'omp_delivery_insert_invalid');
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_omp_status_guard_r1
BEFORE UPDATE OF status ON delivery
WHEN EXISTS (SELECT 1 FROM agent_status a
             WHERE a.id=OLD.recipient_id AND a.binding_epoch IS NOT NULL)
 AND NEW.status NOT IN ('outstanding','acked','rejected')
BEGIN
  SELECT RAISE(ABORT,'omp_delivery_status_invalid');
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_omp_ack_guard_r1
BEFORE UPDATE OF status,omp_ack_seq ON delivery
WHEN EXISTS (SELECT 1 FROM agent_status a
             WHERE a.id=OLD.recipient_id AND a.binding_epoch IS NOT NULL)
 AND (
   (NEW.status='acked' AND OLD.status NOT IN ('acked','rejected') AND
    (NEW.omp_ack_seq<>OLD.omp_ack_seq+1 OR NOT EXISTS
      (SELECT 1 FROM delivery_offer o
        WHERE o.recipient_id=OLD.recipient_id
          AND o.event_id=OLD.event_id AND o.state='accepted')))
   OR
   (NEW.omp_ack_seq<>OLD.omp_ack_seq AND NOT (
      OLD.status='outstanding' AND NEW.status='acked'
      AND NEW.omp_ack_seq=OLD.omp_ack_seq+1
      AND EXISTS (SELECT 1 FROM delivery_offer o
                   WHERE o.recipient_id=OLD.recipient_id
                     AND o.event_id=OLD.event_id AND o.state='accepted')))
 )
BEGIN
  SELECT RAISE(IGNORE);
END;

-- A compatible pre-R1 ACK is name-keyed. Allow it only while that name still
-- belongs to the row's stable recipient, or while the row uses that recipient's
-- unambiguous ID fallback. An R1 credentialed OMP ACK carries
-- its own sequence increment and is checked by trg_delivery_omp_ack_guard_r1.
CREATE TRIGGER IF NOT EXISTS trg_delivery_stable_ack_guard_r1
BEFORE UPDATE OF status,omp_ack_seq ON delivery
WHEN OLD.recipient_id IS NOT NULL
 AND OLD.status<>'acked' AND NEW.status='acked'
 AND NEW.omp_ack_seq=OLD.omp_ack_seq
 AND OLD.to_agent<>OLD.recipient_id
 AND NOT EXISTS (
   SELECT 1 FROM agent_status a
    WHERE a.id=OLD.recipient_id
      AND lower(a.relay_name)=lower(OLD.to_agent))
BEGIN
  SELECT RAISE(IGNORE);
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_rejection_check_insert_r1
BEFORE INSERT ON delivery
WHEN (NEW.status='rejected') <> (NEW.reject_reason_code IS NOT NULL)
 OR (NEW.reject_reason_code IS NOT NULL AND NEW.reject_reason_code NOT IN
   ('USER_REJECTED','PROTOCOL_INVALID','RECIPIENT_POLICY_REFUSED',
    'PERSISTENCE_UNRECOVERABLE','DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN'))
BEGIN
  SELECT RAISE(ABORT,'omp_reject_reason_invalid');
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_rejection_check_update_r1
BEFORE UPDATE OF status,reject_reason_code ON delivery
WHEN NOT (OLD.status='rejected' AND NEW.status='acked') AND (
 (NEW.status='rejected') <> (NEW.reject_reason_code IS NOT NULL)
 OR (NEW.reject_reason_code IS NOT NULL AND NEW.reject_reason_code NOT IN
   ('USER_REJECTED','PROTOCOL_INVALID','RECIPIENT_POLICY_REFUSED',
    'PERSISTENCE_UNRECOVERABLE','DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN')))
BEGIN
  SELECT RAISE(ABORT,'omp_reject_reason_invalid');
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_offer_immutable_r1
BEFORE UPDATE ON delivery_offer
WHEN NEW.recipient_id<>OLD.recipient_id OR NEW.event_id<>OLD.event_id
 OR NEW.offer_token<>OLD.offer_token OR NEW.created_ts<>OLD.created_ts
 OR NEW.omp_process_epoch IS NOT OLD.omp_process_epoch
 OR (OLD.state IN ('accepted','rejected') AND NEW.state<>OLD.state)
 OR (OLD.state='rejected' AND NEW.reason IS NOT OLD.reason)
BEGIN
  SELECT RAISE(ABORT,'omp_offer_immutable');
END;

-- A rolled-back pre-R1 retention statement may target every old agent_message.
-- Skip protected rows while allowing the same bulk statement to delete legacy rows.
CREATE TRIGGER IF NOT EXISTS trg_delivery_delete_guard_r1
BEFORE DELETE ON delivery
WHEN EXISTS (SELECT 1 FROM agent_status a
             WHERE a.id=OLD.recipient_id AND a.binding_epoch IS NOT NULL)
 OR EXISTS (SELECT 1 FROM delivery_offer o
            WHERE o.recipient_id=OLD.recipient_id AND o.event_id=OLD.event_id)
BEGIN
  SELECT RAISE(IGNORE);
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_offer_delete_guard_r1
BEFORE DELETE ON delivery_offer
BEGIN
  SELECT RAISE(IGNORE);
END;

CREATE TRIGGER IF NOT EXISTS trg_delivery_terminal_winner_r1
BEFORE UPDATE OF status ON delivery
WHEN NEW.status='rejected'
 AND EXISTS (SELECT 1 FROM delivery_offer o
              WHERE o.recipient_id=OLD.recipient_id
                AND o.event_id=OLD.event_id AND o.state='accepted')
BEGIN
  SELECT RAISE(ABORT,'omp_terminal_conflict');
END;

CREATE TRIGGER IF NOT EXISTS trg_offer_terminal_winner_r1
BEFORE UPDATE OF state ON delivery_offer
WHEN NEW.state='accepted'
 AND EXISTS (SELECT 1 FROM delivery d
              WHERE d.recipient_id=OLD.recipient_id
                AND d.event_id=OLD.event_id AND d.status='rejected')
BEGIN
  SELECT RAISE(ABORT,'omp_terminal_conflict');
END;

CREATE TRIGGER IF NOT EXISTS trg_agent_ever_omp_update_r1
BEFORE UPDATE OF transport,binding_epoch,relay_name,status,kind,channel,thread_id,
                 current_task,capabilities,last_activity_ts,display_name,
                 omp_mutation_seq ON agent_status
WHEN OLD.binding_epoch IS NOT NULL
 AND (
   NEW.binding_epoch IS NULL OR NEW.transport IS NOT 'omp-mcp'
   OR NEW.omp_mutation_seq NOT IN (OLD.omp_mutation_seq, OLD.omp_mutation_seq+1)
   OR ((NEW.relay_name IS NOT OLD.relay_name OR NEW.status IS NOT OLD.status
        OR NEW.kind IS NOT OLD.kind OR NEW.channel IS NOT OLD.channel
        OR NEW.thread_id IS NOT OLD.thread_id
        OR NEW.current_task IS NOT OLD.current_task
        OR NEW.capabilities IS NOT OLD.capabilities
        OR NEW.last_activity_ts IS NOT OLD.last_activity_ts
        OR NEW.display_name IS NOT OLD.display_name)
       AND NEW.omp_mutation_seq<>OLD.omp_mutation_seq+1)
 )
BEGIN
  SELECT RAISE(ABORT,'omp_identity_legacy_mutation');
END;

CREATE TRIGGER IF NOT EXISTS trg_agent_omp_binding_insert_r1
BEFORE INSERT ON agent_status
WHEN (NEW.binding_epoch IS NULL AND
      (NEW.omp_process_epoch IS NOT NULL OR NEW.omp_instance_id IS NOT NULL
       OR NEW.control_epoch IS NOT NULL OR NEW.control_token_hash IS NOT NULL
       OR NEW.bind_attempt_id IS NOT NULL))
 OR (NEW.binding_epoch IS NOT NULL AND
      (NEW.transport IS NOT 'omp-mcp'
       OR NEW.omp_process_epoch IS NULL OR NEW.omp_instance_id IS NULL
       OR NEW.control_epoch IS NULL OR NEW.control_token_hash IS NULL
       OR NEW.bind_attempt_id IS NULL
       OR length(NEW.binding_epoch)<>36 OR length(NEW.omp_process_epoch)<>36
       OR length(NEW.omp_instance_id)<>36 OR length(NEW.control_epoch)<>36
       OR length(NEW.bind_attempt_id)<>36 OR length(NEW.control_token_hash)<>64
       OR NEW.control_token_hash GLOB '*[^0-9a-f]*'))
BEGIN
  SELECT RAISE(ABORT,'omp_binding_invalid');
END;

CREATE TRIGGER IF NOT EXISTS trg_agent_omp_binding_update_r1
BEFORE UPDATE OF transport,binding_epoch,omp_process_epoch,omp_instance_id,
                 control_epoch,control_token_hash,bind_attempt_id ON agent_status
WHEN (NEW.binding_epoch IS NULL AND
      (NEW.omp_process_epoch IS NOT NULL OR NEW.omp_instance_id IS NOT NULL
       OR NEW.control_epoch IS NOT NULL OR NEW.control_token_hash IS NOT NULL
       OR NEW.bind_attempt_id IS NOT NULL))
 OR (NEW.binding_epoch IS NOT NULL AND
      (NEW.transport IS NOT 'omp-mcp'
       OR NEW.omp_process_epoch IS NULL OR NEW.omp_instance_id IS NULL
       OR NEW.control_epoch IS NULL OR NEW.bind_attempt_id IS NULL
       OR length(NEW.binding_epoch)<>36 OR length(NEW.omp_process_epoch)<>36
       OR length(NEW.omp_instance_id)<>36 OR length(NEW.control_epoch)<>36
       OR length(NEW.bind_attempt_id)<>36
       OR (NEW.control_token_hash IS NOT NULL AND
           (length(NEW.control_token_hash)<>64 OR
            NEW.control_token_hash GLOB '*[^0-9a-f]*'))
       OR ((OLD.binding_epoch IS NULL OR NEW.binding_epoch IS NOT OLD.binding_epoch)
           AND NEW.control_token_hash IS NULL)))
 OR (OLD.binding_epoch IS NOT NULL AND NEW.binding_epoch=OLD.binding_epoch AND
      (NEW.omp_process_epoch IS NOT OLD.omp_process_epoch
       OR NEW.omp_instance_id IS NOT OLD.omp_instance_id
       OR NEW.control_epoch IS NOT OLD.control_epoch
       OR NEW.bind_attempt_id IS NOT OLD.bind_attempt_id
       OR (NEW.control_token_hash IS NOT OLD.control_token_hash
           AND NEW.control_token_hash IS NOT NULL)))
 OR (OLD.binding_epoch IS NOT NULL AND NEW.binding_epoch<>OLD.binding_epoch AND
      (NEW.bind_attempt_id=OLD.bind_attempt_id
       OR NEW.control_token_hash IS OLD.control_token_hash))
BEGIN
  SELECT RAISE(ABORT,'omp_binding_invalid');
END;

CREATE TRIGGER IF NOT EXISTS trg_agent_namespace_seed_insert_r1
AFTER INSERT ON agent_status
WHEN NEW.binding_epoch IS NOT NULL AND NEW.relay_name IS NOT NULL
BEGIN
  INSERT OR IGNORE INTO omp_relay_namespace
    (relay_name,owner_id,created_ts,updated_ts)
  VALUES (NEW.relay_name,NEW.id,CAST(strftime('%s','now') AS INTEGER)*1000,
          CAST(strftime('%s','now') AS INTEGER)*1000);
END;

CREATE TRIGGER IF NOT EXISTS trg_agent_namespace_seed_update_r1
AFTER UPDATE OF relay_name ON agent_status
WHEN NEW.binding_epoch IS NOT NULL AND NEW.relay_name IS NOT NULL
BEGIN
  INSERT OR IGNORE INTO omp_relay_namespace
    (relay_name,owner_id,created_ts,updated_ts)
  VALUES (NEW.relay_name,NEW.id,CAST(strftime('%s','now') AS INTEGER)*1000,
          CAST(strftime('%s','now') AS INTEGER)*1000);
END;

-- A pre-R1 process does not know omp_relay_namespace. It may keep using names
-- that were never OMP-controlled, but cannot silently claim a retained OMP name.
CREATE TRIGGER IF NOT EXISTS trg_agent_namespace_insert_r1
BEFORE INSERT ON agent_status
WHEN NEW.relay_name IS NOT NULL
 AND EXISTS (SELECT 1 FROM omp_relay_namespace n
              WHERE n.relay_name=NEW.relay_name AND n.owner_id<>NEW.id)
BEGIN
  SELECT RAISE(ABORT,'omp_relay_namespace_retained');
END;

CREATE TRIGGER IF NOT EXISTS trg_agent_namespace_update_r1
BEFORE UPDATE OF relay_name ON agent_status
WHEN NEW.relay_name IS NOT NULL
 AND EXISTS (SELECT 1 FROM omp_relay_namespace n
              WHERE n.relay_name=NEW.relay_name AND n.owner_id<>NEW.id)
BEGIN
  SELECT RAISE(ABORT,'omp_relay_namespace_retained');
END;

-- Pre-R1 name claims have no stable-recipient predicate. Refuse a claim while
-- any retained delivery under that name belongs to another identity. R1 moves
-- a displaced legacy owner's rows to its stable fallback before takeover.
CREATE TRIGGER IF NOT EXISTS trg_agent_stable_namespace_insert_r1
BEFORE INSERT ON agent_status
WHEN NEW.relay_name IS NOT NULL
 AND EXISTS (
   SELECT 1 FROM delivery d JOIN agent_status a ON a.id=d.recipient_id
    WHERE a.binding_epoch IS NULL AND d.recipient_id<>NEW.id
      AND lower(d.to_agent)=lower(NEW.relay_name))
BEGIN
  SELECT RAISE(ABORT,'stable_relay_namespace_retained');
END;

CREATE TRIGGER IF NOT EXISTS trg_agent_stable_namespace_update_r1
BEFORE UPDATE OF relay_name ON agent_status
WHEN NEW.relay_name IS NOT NULL
 AND EXISTS (
   SELECT 1 FROM delivery d JOIN agent_status a ON a.id=d.recipient_id
    WHERE a.binding_epoch IS NULL AND d.recipient_id<>NEW.id
      AND lower(d.to_agent)=lower(NEW.relay_name))
BEGIN
  SELECT RAISE(ABORT,'stable_relay_namespace_retained');
END;

-- Stable outstanding delivery must remain resolvable. Terminal delivery no
-- longer needs the identity row, so ordinary legacy lifecycle cleanup remains.
CREATE TRIGGER IF NOT EXISTS trg_agent_outstanding_delivery_delete_r1
BEFORE DELETE ON agent_status
WHEN EXISTS (SELECT 1 FROM delivery d
              WHERE d.recipient_id=OLD.id AND d.status='outstanding')
BEGIN
  SELECT RAISE(IGNORE);
END;

CREATE TRIGGER IF NOT EXISTS trg_agent_ever_omp_delete_r1
BEFORE DELETE ON agent_status
WHEN OLD.binding_epoch IS NOT NULL
BEGIN
  SELECT RAISE(IGNORE);
END;

-- Once a legacy owner has no outstanding rows, deletion is allowed. Rehome its
-- terminal history to the stable ID so a later pre-R1 name claimant cannot read
-- another identity's old mailbox.
CREATE TRIGGER IF NOT EXISTS trg_agent_stable_history_rehome_r1
AFTER DELETE ON agent_status
WHEN OLD.binding_epoch IS NULL
BEGIN
  UPDATE message_log SET to_agent=OLD.id
   WHERE event_id IN (SELECT event_id FROM delivery WHERE recipient_id=OLD.id);
  UPDATE delivery SET to_agent=OLD.id,
         updated_ts=CAST(strftime('%s','now') AS INTEGER)*1000
   WHERE recipient_id=OLD.id;
END;

-- SQLite length(TEXT) counts Unicode scalar values; BLOB length counts UTF-8 bytes.
CREATE TRIGGER IF NOT EXISTS trg_message_log_omp_bounds_r1
BEFORE INSERT ON message_log
WHEN NEW.to_agent IS NOT NULL
 AND EXISTS (SELECT 1 FROM agent_status a
              WHERE a.binding_epoch IS NOT NULL AND (
                lower(a.relay_name)=lower(NEW.to_agent)
                OR a.id=NEW.to_agent
                OR a.id=(SELECT owner_id FROM omp_relay_namespace
                          WHERE relay_name=NEW.to_agent)))
BEGIN
  SELECT CASE WHEN NEW.kind NOT IN ('agent_message','agent_reply','handoff')
    THEN RAISE(ABORT,'omp_bound_kind') END;
  SELECT CASE WHEN typeof(NEW.from_agent)<>'text'
                   OR typeof(NEW.to_agent)<>'text'
                   OR typeof(NEW.channel)<>'text'
                   OR typeof(NEW.content)<>'text'
                   OR (NEW.thread_id IS NOT NULL AND typeof(NEW.thread_id)<>'text')
                   OR (NEW.client_event_id IS NOT NULL AND typeof(NEW.client_event_id)<>'text')
    THEN RAISE(ABORT,'omp_bound_type') END;
  SELECT CASE WHEN length(NEW.from_agent)<1 OR length(NEW.from_agent)>256
                   OR length(NEW.to_agent)<1 OR length(NEW.to_agent)>256
    THEN RAISE(ABORT,'omp_bound_agent') END;
  SELECT CASE WHEN NEW.to_agent GLOB '*[^A-Za-z0-9._-]*'
                   OR length(NEW.to_agent)<1 OR length(NEW.to_agent)>64
    THEN RAISE(ABORT,'omp_bound_relay_name') END;
  SELECT CASE WHEN length(NEW.channel)<1 OR length(NEW.channel)>256
    THEN RAISE(ABORT,'omp_bound_channel') END;
  SELECT CASE WHEN NEW.thread_id IS NOT NULL AND length(NEW.thread_id)>256
    THEN RAISE(ABORT,'omp_bound_thread') END;
  SELECT CASE WHEN NEW.client_event_id IS NOT NULL AND length(NEW.client_event_id)>256
    THEN RAISE(ABORT,'omp_bound_client_event') END;
  SELECT CASE WHEN typeof(NEW.ts)<>'integer' OR NEW.ts<0 OR NEW.ts>9007199254740991
    THEN RAISE(ABORT,'omp_bound_ts') END;
  SELECT CASE WHEN length(CAST(NEW.content AS BLOB))>1048576
    THEN RAISE(ABORT,'omp_bound_content') END;
END;

-- Pre-R1 rename code updates message_log before delivery. Skip only that benign
-- address rewrite for stable OMP events so the bulk statement can continue;
-- reject any other mutation of broker-authored OMP message history.
CREATE TRIGGER IF NOT EXISTS trg_message_log_omp_update_guard_r1
BEFORE UPDATE OF event_id,from_agent,to_agent,channel,thread_id,kind,content,ts
ON message_log
WHEN EXISTS (
  SELECT 1 FROM delivery d JOIN agent_status a ON a.id=d.recipient_id
   WHERE d.event_id=OLD.event_id AND a.binding_epoch IS NOT NULL)
BEGIN
  SELECT CASE WHEN NEW.to_agent IS NOT OLD.to_agent
                   AND NEW.event_id=OLD.event_id
                   AND NEW.from_agent=OLD.from_agent
                   AND NEW.channel=OLD.channel
                   AND NEW.thread_id IS OLD.thread_id
                   AND NEW.kind=OLD.kind
                   AND NEW.content=OLD.content
                   AND NEW.ts=OLD.ts
    THEN RAISE(IGNORE)
    ELSE RAISE(ABORT,'omp_message_immutable') END;
END;

CREATE TRIGGER IF NOT EXISTS trg_message_log_delete_guard_r1
BEFORE DELETE ON message_log
WHEN EXISTS (
       SELECT 1 FROM delivery d JOIN agent_status a ON a.id=d.recipient_id
        WHERE d.event_id=OLD.event_id AND a.binding_epoch IS NOT NULL)
 OR EXISTS (SELECT 1 FROM delivery_offer o WHERE o.event_id=OLD.event_id)
BEGIN
  SELECT RAISE(IGNORE);
END;

CREATE TRIGGER IF NOT EXISTS trg_omp_history_update_r1
BEFORE UPDATE ON idempotency_ledger
WHEN OLD.namespace IN ('omp-bind','omp-status','omp-operator')
BEGIN
  SELECT RAISE(ABORT,'omp_history_immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_omp_history_delete_r1
BEFORE DELETE ON idempotency_ledger
WHEN OLD.namespace IN ('omp-bind','omp-status','omp-operator')
BEGIN
  SELECT RAISE(IGNORE);
END;

CREATE TRIGGER IF NOT EXISTS trg_omp_rejection_audit_update_r1
BEFORE UPDATE ON omp_rejection_audit
BEGIN
  SELECT RAISE(ABORT,'omp_audit_immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_omp_rejection_audit_delete_r1
BEFORE DELETE ON omp_rejection_audit
BEGIN
  SELECT RAISE(ABORT,'omp_audit_immutable');
END;

-- discord_cursors: per-(channel_id, thread_id) catch-up cursor
-- thread_id NOT NULL DEFAULT '' (sentinel for channel-level cursors)
-- because SQLite composite PRIMARY KEY requires all columns NOT NULL.
CREATE TABLE IF NOT EXISTS discord_cursors (
  channel_id TEXT NOT NULL,
  thread_id TEXT NOT NULL DEFAULT '',
  last_seen_message_id TEXT NOT NULL,
  last_check_ts INTEGER NOT NULL,
  PRIMARY KEY (channel_id, thread_id)
);

-- discord_poll_targets: M2 broker-poller registry (D16 § (a)).
-- M2-pending non-contract addition; absorbed when contract.md bumps to v1.1.
-- thread_id '' sentinel = channel-level (no thread). source distinguishes
-- bootstrap-from-access.json rows from runtime-registered rows so a future
-- access.json reload can refresh the bootstrap set without clobbering
-- runtime-registered threads.
CREATE TABLE IF NOT EXISTS discord_poll_targets (
  channel_id TEXT NOT NULL,
  thread_id TEXT NOT NULL DEFAULT '',
  registered_at INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('access_json','runtime_register')),
  last_polled_ts INTEGER,
  PRIMARY KEY (channel_id, thread_id)
);

-- project_registry: registered per-project state.db files (must precede pipelines_view due to FK)
-- D27 iter-2 (M1): local_validation column persists registration mode so the
-- broker can recall whether a row is local-FS-readable (1) or remote-only (0)
-- across restarts. Default 1 preserves existing rows on in-place migration.
-- Migration for existing coordination.db files is handled in
-- server.ensure_schema via PRAGMA table_info detect + ALTER TABLE ADD COLUMN.
CREATE TABLE IF NOT EXISTS project_registry (
  project_id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  root_path TEXT NOT NULL,
  state_db_path TEXT NOT NULL,
  registered_at INTEGER NOT NULL,
  last_refresh_ts INTEGER,
  local_validation INTEGER NOT NULL DEFAULT 1
);

-- pipelines_view: cached materialized view of cross-project pipeline state
-- Refreshed via file watcher on project state.db mtime change (debounced 5s)
-- OR explicit POST /api/project/{name}/refresh OR broker startup.
-- GET /api/state reads only from this cache; never walks state.db files synchronously.
-- NOTE: GET /api/state returns this collection IN FULL, not bounded by ?limit (per
-- iter-4 HIGH-4 fix and contract.md). v1.1 will add a separate pipeline cursor
-- if pipelines_view grows beyond ~1000 rows.
CREATE TABLE IF NOT EXISTS pipelines_view (
  project_name TEXT NOT NULL,
  pipeline_id TEXT NOT NULL,
  shape TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  completed_at INTEGER,
  status TEXT NOT NULL CHECK (status IN ('open','in_progress','blocked','done','failed','cancelled')),
  step_count INTEGER NOT NULL,
  steps_done INTEGER NOT NULL,
  last_refresh_ts INTEGER NOT NULL,
  PRIMARY KEY (project_name, pipeline_id),
  FOREIGN KEY (project_name) REFERENCES project_registry(name)
);
CREATE INDEX IF NOT EXISTS idx_pipelines_view_status ON pipelines_view(status);
