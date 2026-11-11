// Backend-only checkpoint bridge. No public table policies or project-wide keys in Render.
const TOKEN_SHA256 = "e546da41b37450feb8e3e566d1af24f6c01d33954b67fc84cccdcdb4193a9a18";
const ID = "ALT-MOMENTUM-V1";
const TABLE = "alt_momentum_v1_checkpoint";
const BACKUP = "alt_momentum_v1_recovery_backup";
const reply = (data: unknown, status = 200) => Response.json(data, {status});

async function decode(value: string) {
  const bytes = Uint8Array.from(atob(value), c => c.charCodeAt(0));
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return await new Response(stream).json();
}
function identity(t: Record<string, unknown>) {
  return JSON.stringify([t.variant, t.symbol, t.entry_ms]);
}
function indexTrades(payload: any) {
  if (!payload || !Array.isArray(payload.trades) || !payload.seen ||
      typeof payload.seen !== "object" || !Number.isInteger(payload.signals)) throw Error("schema");
  const map = new Map<string, any>();
  for (const t of payload.trades) {
    if (typeof t.variant !== "string" || typeof t.symbol !== "string" ||
        !Number.isFinite(t.entry_ms) || typeof t.status !== "string") throw Error("trade");
    const k = identity(t);
    if (map.has(k)) throw Error("duplicate");
    map.set(k, t);
  }
  return map;
}

Deno.serve(async (req: Request) => {
  // Authenticate before reading a body or querying any database table.
  const token = req.headers.get("x-paper-token") || "";
  if (token.length < 32 || token.length > 128) return reply({error: "unauthorized"}, 401);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(token));
  const hash = Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, "0")).join("");
  if (hash !== TOKEN_SHA256) return reply({error: "unauthorized"}, 401);
  if (!["GET", "PUT"].includes(req.method)) return reply({error: "method"}, 405);
  try {
    const base = Deno.env.get("SUPABASE_URL")!;
    const newKeys = Deno.env.get("SUPABASE_SECRET_KEYS");
    const key = (newKeys ? JSON.parse(newKeys).default : null) || Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const headers: Record<string,string> = {apikey: key, "Content-Type": "application/json"};
    if (!key.startsWith("sb_secret_")) headers.Authorization = `Bearer ${key}`;
    const url = `${base}/rest/v1/${TABLE}?id=eq.${ID}`;
    const read = await fetch(url + "&select=checkpoint_b64,revision", {headers});
    if (!read.ok) return reply({error: "database_unavailable"}, 503);
    const current = (await read.json())[0];
    if (req.method === "GET") return current ? reply(current) : reply({error:"not_initialized"},404);
    const text = await req.text();
    if (text.length > 2100000) return reply({error: "too_large"}, 413);
    const body = JSON.parse(text);
    if (!Number.isSafeInteger(body.expected_revision) || body.expected_revision < 0 ||
        typeof body.checkpoint_b64 !== "string" || body.checkpoint_b64.length > 2000000)
      return reply({error:"invalid_request"},400);
    if ((current?.revision ?? 0) !== body.expected_revision) return reply({error:"revision_conflict"},409);
    const incoming = await decode(body.checkpoint_b64);
    const incomingTrades = indexTrades(incoming);
    if (current) {
      const previous = await decode(current.checkpoint_b64);
      for (const [k, t] of indexTrades(previous)) {
        const next = incomingTrades.get(k);
        if (!next || (t.status === "CLOSED_SHADOW" &&
            (next.status !== t.status || next.net_pct !== t.net_pct || next.entry_price !== t.entry_price)))
          return reply({error:"history_removal_or_rewrite_refused"},422);
      }
    }
    const revision = body.expected_revision + 1;
    const value = {id:ID, checkpoint_b64:body.checkpoint_b64, revision, updated_at:new Date().toISOString()};
    let saved: Response;
    if (current) {
      saved = await fetch(url + `&revision=eq.${body.expected_revision}&select=revision`,
        {method:"PATCH",headers:{...headers,Prefer:"return=representation"},body:JSON.stringify(value)});
    } else {
      // Keep a separate immutable recovery copy before creating the working checkpoint.
      const backup = await fetch(`${base}/rest/v1/${BACKUP}?on_conflict=id`,
        {method:"POST",headers:{...headers,Prefer:"resolution=ignore-duplicates"},
         body:JSON.stringify({id:ID,checkpoint_b64:body.checkpoint_b64})});
      if (!backup.ok) return reply({error:"backup_failed"},503);
      saved = await fetch(`${base}/rest/v1/${TABLE}?select=revision`,
        {method:"POST",headers:{...headers,Prefer:"return=representation"},body:JSON.stringify(value)});
    }
    if (!saved.ok) return reply({error: saved.status===409 ? "revision_conflict" : "save_failed"},saved.status===409?409:503);
    const result = await saved.json();
    if (result.length !== 1) return reply({error:"revision_conflict"},409);
    return reply({revision:result[0].revision, trade_count:incomingTrades.size});
  } catch {
    // Never log requests, credentials or underlying HTTP errors.
    return reply({error:"checkpoint_request_failed"},400);
  }
});
