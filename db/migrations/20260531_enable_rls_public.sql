-- Phase: Supabase security lint remediation (2026-05-31)
-- Problem: Supabase exposes a public PostgREST API over the `public` schema.
-- All app tables had RLS disabled, so anyone with the project's anon key could
-- read/write them directly via https://<project>.supabase.co/rest/v1/<table>,
-- bypassing the Fly backend.
--
-- Fix: enable RLS on every app table with NO policies (deny-all to anon /
-- authenticated). The backend connects as role `postgres` (rolbypassrls=true,
-- table owner) and is therefore UNAFFECTED — it continues to read/write freely.
-- Also flip the SECURITY DEFINER view v_today_ideas to security_invoker, so it
-- no longer leaks base-table rows past RLS when queried via PostgREST.
--
-- Idempotent: ENABLE ROW LEVEL SECURITY is safe to re-run.

ALTER TABLE public.daily_market_narratives        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.daily_pitch_decisions          ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.client_focus_daily_verdicts    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.daily_recommendation_history   ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.earnings_calendar              ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.device_favorites               ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.device_tokens                  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.drawdown_attribution_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.drawdown_attributions          ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.daily_best_history             ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.late_cycle_pillar_history      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.macro_regime_snapshots         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.risk_flags                     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.price_history                  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.recommendation_tracker         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.theme_basket_results           ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.idea_runs                      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.option_snapshots               ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.house_overrides                ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.indicator_history              ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.indicator_persistence          ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.underlying_status_log          ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.idea_candidates                ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.underlyings                    ENABLE ROW LEVEL SECURITY;

-- SECURITY DEFINER view -> security_invoker so it respects the querying role's
-- RLS (anon hits deny-all base tables; backend/postgres still bypasses).
ALTER VIEW public.v_today_ideas SET (security_invoker = on);
