-- Applied with Supabase MCP: alt_momentum_v1_private_paper_checkpoints.
create table public.alt_momentum_v1_checkpoint (
  id text primary key check (id = 'ALT-MOMENTUM-V1'),
  checkpoint_b64 text not null check (length(checkpoint_b64) between 1 and 2000000),
  revision bigint not null check (revision > 0),
  updated_at timestamptz not null default now()
);
alter table public.alt_momentum_v1_checkpoint enable row level security;
revoke all on public.alt_momentum_v1_checkpoint from public, anon, authenticated;
grant select, insert, update on public.alt_momentum_v1_checkpoint to service_role;
create table public.alt_momentum_v1_recovery_backup (
  id text primary key check (id = 'ALT-MOMENTUM-V1'),
  checkpoint_b64 text not null,
  created_at timestamptz not null default now()
);
alter table public.alt_momentum_v1_recovery_backup enable row level security;
revoke all on public.alt_momentum_v1_recovery_backup from public, anon, authenticated;
grant select, insert on public.alt_momentum_v1_recovery_backup to service_role;
