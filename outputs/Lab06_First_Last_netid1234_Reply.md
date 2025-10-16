# Stakeholder Reply — DashDash Data Quality & KPIs

**KPI summary:** On-Time Delivery: 50.00% · Average Delivery Minutes: 35.67 · Cancel/Return Rate: 25.00%

**What changed after remediation**
- Deduplicated orders by `order_id` (kept earliest record); removed duplicates from KPI universe.
- Normalized mixed/invalid `status` values to {delivered|canceled|returned|unknown} to prevent leakage into dashboards.
- Enforced referential integrity to restaurants/couriers; excluded rows with bad foreign keys from facts.
- Added a DQ Exceptions view to track excluded rows and their reasons.

**Recommendation**
- Proceed with the “10‑Minute Free Delivery Insurance” promo **if** daily on‑time % remains ≥ 85% during soft‑launch; otherwise, pause the offer in regions where scooter/car availability is thin.

_Generated with GPT-5 Pro on 2025-10-16 01:08 UTC. Student verified the numbers and process._
