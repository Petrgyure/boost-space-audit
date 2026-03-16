#!/usr/bin/env python3
"""
Boost.space Google Ads Audit — Data Pipeline
Pulls live data from Google Ads API + PostHog, writes index.html
"""
import os, json, re, datetime, urllib.request, urllib.error

# ── Credentials ──────────────────────────────────────────────────────────────
GADS_CONFIG = {
    "client_id":        os.environ["GADS_CLIENT_ID"],
    "client_secret":    os.environ["GADS_CLIENT_SECRET"],
    "developer_token":  os.environ["GADS_DEV_TOKEN"],
    "login_customer_id":os.environ["GADS_LOGIN_CID"],
    "refresh_token":    os.environ["GADS_REFRESH_TOKEN"],
    "use_proto_plus":   True,
}
POSTHOG_KEY  = os.environ["POSTHOG_KEY"]
POSTHOG_HOST = "https://eu.posthog.com"
POSTHOG_PID  = os.environ.get("POSTHOG_PID",  "100634")
CUSTOMER_ID  = "2058897291"
EUR_RATE     = 24.45   # 1 EUR = 24.45 CZK

# ── Helpers ───────────────────────────────────────────────────────────────────
def czk(v): return round(v / EUR_RATE, 2)
def eur(v):
    e = v / EUR_RATE
    if e >= 1000: return f"€{e:,.0f}"
    if e >= 10:   return f"€{e:.0f}"
    return f"€{e:.2f}"

def ph_query(sql):
    payload = json.dumps({"query": {"kind": "HogQLQuery", "query": sql}}).encode()
    req = urllib.request.Request(
        f"{POSTHOG_HOST}/api/projects/{POSTHOG_PID}/query/",
        data=payload,
        headers={"Authorization": f"Bearer {POSTHOG_KEY}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

# ── Pull Google Ads ───────────────────────────────────────────────────────────
def pull_google_ads():
    from google.ads.googleads.client import GoogleAdsClient
    client = GoogleAdsClient.load_from_dict(GADS_CONFIG)
    svc = client.get_service("GoogleAdsService")

    # Active campaigns
    camps = []
    for row in svc.search(customer_id=CUSTOMER_ID, query="""
        SELECT campaign.id, campaign.name, campaign.advertising_channel_type,
               campaign.bidding_strategy_type,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.ctr, metrics.average_cpc,
               metrics.cost_per_conversion,
               metrics.search_impression_share,
               metrics.search_top_impression_share,
               metrics.search_budget_lost_impression_share,
               metrics.search_rank_lost_impression_share
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND segments.date DURING LAST_30_DAYS
        ORDER BY metrics.cost_micros DESC
    """):
        m = row.metrics; c = row.campaign
        cpa_czk = m.cost_per_conversion / 1_000_000 if m.conversions > 0 else None
        camps.append({
            "name":       c.name,
            "type":       c.advertising_channel_type.name,
            "bidding":    c.bidding_strategy_type.name,
            "impr":       m.impressions,
            "clicks":     m.clicks,
            "ctr":        round(m.ctr * 100, 2),
            "cost_czk":   round(m.cost_micros / 1_000_000, 2),
            "cost_eur":   eur(m.cost_micros / 1_000_000),
            "cpc_eur":    eur(m.average_cpc / 1_000_000),
            "conv":       round(m.conversions, 1),
            "cpa_eur":    eur(cpa_czk) if cpa_czk else "∞",
            "is":         round(m.search_impression_share * 100, 1) if m.search_impression_share and m.search_impression_share < 10 else None,
            "rank_lost":  round(m.search_rank_lost_impression_share * 100, 1) if m.search_rank_lost_impression_share and m.search_rank_lost_impression_share < 10 else None,
        })

    # Search terms (active campaigns only, top 80 by cost)
    terms = []
    for row in svc.search(customer_id=CUSTOMER_ID, query="""
        SELECT search_term_view.search_term, campaign.name, ad_group.name,
               search_term_view.status,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.ctr, metrics.average_cpc
        FROM search_term_view
        WHERE segments.date DURING LAST_30_DAYS
          AND campaign.status = 'ENABLED'
          AND metrics.impressions > 0
        ORDER BY metrics.cost_micros DESC
        LIMIT 80
    """):
        m = row.metrics
        terms.append({
            "term":       row.search_term_view.search_term,
            "campaign":   row.campaign.name,
            "adgroup":    row.ad_group.name,
            "status":     row.search_term_view.status.name,
            "impr":       m.impressions,
            "clicks":     m.clicks,
            "cost_czk":   round(m.cost_micros / 1_000_000, 2),
            "cost_eur":   eur(m.cost_micros / 1_000_000),
            "cpc_eur":    eur(m.average_cpc / 1_000_000),
            "conv":       round(m.conversions, 1),
            "ctr":        round(m.ctr * 100, 2),
        })

    # Keywords
    keywords = []
    for row in svc.search(customer_id=CUSTOMER_ID, query="""
        SELECT campaign.name, ad_group.name,
               ad_group_criterion.keyword.text,
               ad_group_criterion.keyword.match_type,
               ad_group_criterion.status,
               ad_group_criterion.quality_info.quality_score,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.average_cpc
        FROM keyword_view
        WHERE campaign.status = 'ENABLED'
          AND ad_group_criterion.status IN ('ENABLED','PAUSED')
          AND segments.date DURING LAST_30_DAYS
        ORDER BY metrics.cost_micros DESC
    """):
        m = row.metrics; kw = row.ad_group_criterion
        keywords.append({
            "campaign":   row.campaign.name,
            "adgroup":    row.ad_group.name,
            "keyword":    kw.keyword.text,
            "match":      kw.keyword.match_type.name,
            "status":     kw.status.name,
            "qs":         kw.quality_info.quality_score or None,
            "impr":       m.impressions,
            "clicks":     m.clicks,
            "cost_czk":   round(m.cost_micros / 1_000_000, 2),
            "cost_eur":   eur(m.cost_micros / 1_000_000),
            "cpc_eur":    eur(m.average_cpc / 1_000_000),
            "conv":       round(m.conversions, 1),
        })

    return camps, terms, keywords

# ── Pull PostHog ──────────────────────────────────────────────────────────────
def pull_posthog():
    conv_rows = ph_query("""
        SELECT event, count() as total,
               countIf(properties.gclid IS NOT NULL) as paid
        FROM events
        WHERE event IN ('demo_meeting_booked','leady_new_lead',
                        'be_form_submit_demo_lead',
                        'select_starting_signup_point','form_submit')
          AND timestamp >= now() - INTERVAL 30 DAY
        GROUP BY event ORDER BY total DESC
    """).get("results", [])

    pages_rows = ph_query("""
        SELECT properties.$pathname, count() as paid_views
        FROM events
        WHERE event = '$pageview'
          AND properties.gclid IS NOT NULL
          AND timestamp >= now() - INTERVAL 30 DAY
        GROUP BY properties.$pathname
        ORDER BY paid_views DESC LIMIT 15
    """).get("results", [])

    session_rows = ph_query("""
        SELECT properties.$session_id,
               countIf(event='$pageview') as pages,
               count() as events
        FROM events
        WHERE properties.gclid IS NOT NULL
          AND timestamp >= now() - INTERVAL 30 DAY
          AND properties.$session_id IS NOT NULL
        GROUP BY properties.$session_id
    """).get("results", [])

    bounce_pct, avg_pages, total_sessions = 0, 0, 0
    if session_rows:
        n = len(session_rows)
        total_sessions = n
        avg_pages = round(sum(r[1] for r in session_rows) / n, 1)
        bounce_pct = round(sum(1 for r in session_rows if r[1] <= 1) / n * 100, 0)

    return {
        "conversions": [{"event": r[0], "total": r[1], "paid": r[2]} for r in conv_rows],
        "pages":       [{"path": r[0], "paid_views": r[1]} for r in pages_rows],
        "sessions":    total_sessions,
        "avg_pages":   avg_pages,
        "bounce_pct":  bounce_pct,
    }

# ── Classify search terms ─────────────────────────────────────────────────────
COMPETITOR  = {"seona","alli ai","usestyle","tradewheel","tradelle","sellvia","sellhub",
               "selleraider","neobund","crosslister","helium 10","propensity ai",
               "zopto","epicor","profound ai","boost ai search","sell raze","go to lister"}
IRRELEVANT  = {"seo with ai","ai seo","seo ai","auto seo","claude seo","seo","price tracker",
               "price tracking","ai","model context protocol","langgraph","notion database"}

def classify_term(term):
    t = term.lower()
    for c in COMPETITOR:
        if c in t: return "COMPETITOR", "badge-purple competitor"
    for i in IRRELEVANT:
        if i == t or t.startswith(i+" "): return "IRRELEVANT", "badge-red irrelevant"
    if any(x in t for x in ["pricing","cost","price","comparison","alternative","migration","replace"]):
        return "BOFU", "badge-green bofu"
    if any(x in t for x in ["tool","software","platform","solution","system","best"]):
        return "MOFU", "badge-yellow mofu"
    return "TOFU", "badge-blue tofu"

def funnel_label(kw_text):
    t = kw_text.lower()
    if any(x in t for x in ["pricing","cost","alternative","comparison","migration","replace","switch"]):
        return "BOFU"
    if any(x in t for x in ["tool","software","platform","solution","system","best","vs "]):
        return "MOFU"
    return "TOFU"

# ── Score calculation ─────────────────────────────────────────────────────────
def calc_score(camps, terms, ph):
    total_cost = sum(c["cost_czk"] for c in camps)
    zero_conv  = sum(1 for c in camps if c["cost_czk"] > 500 and c["conv"] == 0)
    waste_czk  = sum(t["cost_czk"] for t in terms
                     if classify_term(t["term"])[0] in ("COMPETITOR","IRRELEVANT"))
    waste_pct  = (waste_czk / total_cost * 100) if total_cost else 0
    qs1_cost   = sum(k["cost_czk"] for k in [] if k.get("qs") == 1)  # placeholder
    bounce     = ph.get("bounce_pct", 80)

    tracking   = max(5,  25 - (15 if ph["conversions"] and all(c["paid"]==0 for c in ph["conversions"] if c["event"]=="demo_meeting_booked") else 0))
    waste_score= max(2,  20 - int(waste_pct / 5))
    structure  = max(4,  15 - zero_conv * 3)
    keywords   = max(2,  15 - int(waste_pct / 7))
    ads        = 8
    settings   = max(1,  10 - int(bounce / 20))
    total = tracking + waste_score + structure + keywords + ads + settings
    return min(100, total), {"tracking": tracking, "waste": waste_score,
                             "structure": structure, "keywords": keywords,
                             "ads": ads, "settings": settings}

# ── HTML builder ──────────────────────────────────────────────────────────────
def build_html(camps, terms, keywords, ph, score, cats, generated_at):
    total_spend_czk = sum(c["cost_czk"] for c in camps)
    total_spend_eur = eur(total_spend_czk)
    total_conv      = sum(c["conv"] for c in camps)
    waste_czk       = sum(t["cost_czk"] for t in terms
                          if classify_term(t["term"])[0] in ("COMPETITOR","IRRELEVANT"))
    waste_eur       = eur(waste_czk)
    ph_paid_total   = sum(c["paid"] for c in ph["conversions"])
    ph_total_hv     = sum(c["total"] for c in ph["conversions"]
                          if c["event"] in ("demo_meeting_booked","leady_new_lead","be_form_submit_demo_lead"))

    # Build campaign rows
    def camp_row(c):
        verdict = "badge-red" if c["conv"] == 0 and c["cost_czk"] > 200 else "badge-yellow"
        vtext   = "Pause" if c["conv"] == 0 and c["cost_czk"] > 300 else ("Rebuild" if c["conv"] < 5 else "Scale")
        is_bar  = f'<div class="bar-wrap"><div class="bar-fill bar-{"red" if (c["is"] or 0)<20 else "yellow"}" style="width:{c["is"] or 0}%"></div></div>' if c["is"] else ""
        rl_bar  = f'<div class="bar-wrap"><div class="bar-fill bar-red" style="width:{c["rank_lost"] or 0}%"></div></div>' if c["rank_lost"] else ""
        return f"""<tr>
          <td><strong>{c['name']}</strong></td>
          <td><span class="badge badge-blue">{c['type'].replace('_',' ').title()}</span></td>
          <td class="num">{c['impr']:,}</td>
          <td class="num">{c['clicks']:,}</td>
          <td class="num">{c['ctr']}%</td>
          <td class="num"><strong>{c['cost_eur']}</strong></td>
          <td class="num">{c['cpc_eur']}</td>
          <td class="num">{c['conv']}</td>
          <td class="num {'text-red' if c['conv']==0 else ''}">{c['cpa_eur']}</td>
          <td class="num">{f"{c['is']}%{is_bar}" if c['is'] else "—"}</td>
          <td class="num">{f"{c['rank_lost']}%{rl_bar}" if c['rank_lost'] else "—"}</td>
          <td><span class="badge {verdict}">{vtext}</span></td>
        </tr>"""

    camp_rows = "\n".join(camp_row(c) for c in camps)

    # Build search term rows
    def term_row(t):
        intent, cls = classify_term(t["term"])
        action = "→ Negative" if intent in ("COMPETITOR","IRRELEVANT") else ("Fix QS" if t["conv"]==0 and t["cost_czk"]>500 else "Keep")
        ab = "badge-red" if intent in ("COMPETITOR","IRRELEVANT") else ("badge-green" if t["conv"]>0 else "badge-yellow")
        return f"""<tr>
          <td><strong>{t['term']}</strong></td>
          <td style="font-size:11px">{t['campaign'].replace('PIM | Commerce - ','PIM ').replace('CDP | Marketing - ','CDP ')}</td>
          <td class="num">{t['cost_eur']}</td>
          <td class="num">{t['clicks']}</td>
          <td class="num">{t['cpc_eur']}</td>
          <td class="num">{t['ctr']}%</td>
          <td class="num">{t['conv']}</td>
          <td><span class="badge {cls.split()[0]}">{intent}</span></td>
          <td><span class="badge {ab}">{action}</span></td>
        </tr>"""

    term_rows = "\n".join(term_row(t) for t in terms[:50])

    # Build keyword funnel rows
    def kw_row(k):
        fl = funnel_label(k["keyword"])
        fl_cls = {"BOFU":"badge-green bofu","MOFU":"badge-yellow mofu","TOFU":"badge-blue tofu"}[fl]
        qs_badge = f'<span class="badge {"badge-red" if k["qs"]==1 else "badge-yellow" if k["qs"] and k["qs"]<5 else "badge-green" if k["qs"] and k["qs"]>=7 else "badge-gray"}">{k["qs"] if k["qs"] else "—"}</span>'
        return f"""<tr>
          <td>{k['keyword']}</td>
          <td><span class="badge badge-{'purple' if k['match']=='PHRASE' else 'green' if k['match']=='EXACT' else 'blue'}">{k['match'][:3]}</span></td>
          <td>{qs_badge}</td>
          <td class="num">{k['cost_eur']}</td>
          <td class="num">{k['clicks']}</td>
          <td class="num">{k['conv']}</td>
          <td><span class="badge {fl_cls}">{fl}</span></td>
        </tr>"""

    kw_rows_by_camp = {}
    for k in keywords:
        kw_rows_by_camp.setdefault(k["campaign"], []).append(k)

    kw_sections = ""
    for camp_name, kws in kw_rows_by_camp.items():
        total_c = sum(k["cost_czk"] for k in kws)
        total_v = sum(k["conv"] for k in kws)
        rows = "\n".join(kw_row(k) for k in sorted(kws, key=lambda x: -x["cost_czk"])[:20])
        kw_sections += f"""
        <div class="table-wrap" style="margin-bottom:20px">
          <div class="table-header">
            <h3>{camp_name}</h3>
            <span style="color:var(--muted);font-size:12px">{len(kws)} keywords · {eur(total_c)} spend · {total_v:.0f} conv</span>
          </div>
          <table>
            <thead><tr><th>Keyword</th><th>Match</th><th>QS</th><th class="num">Cost</th><th class="num">Clicks</th><th class="num">Conv.</th><th>Funnel</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>"""

    # PostHog conversion table
    conv_rows_html = ""
    for c in ph["conversions"]:
        pct = round(c["paid"]/c["total"]*100,1) if c["total"] else 0
        ok  = c["paid"] > 0
        conv_rows_html += f"""<div class="budget-row">
          <div class="budget-label {'text-red' if not ok else ''}">{c['event']} {'❌' if not ok else '✅'}</div>
          <div class="budget-val"><strong {'class="text-red"' if not ok else ''}>{c['paid']}</strong>
            <span class="text-muted"> / {c['total']} total ({pct}%)</span></div>
        </div>"""

    # PostHog pages
    pages_html = ""
    max_pv = ph["pages"][0]["paid_views"] if ph["pages"] else 1
    for p in ph["pages"]:
        w = int(p["paid_views"] / max_pv * 100)
        pages_html += f"""<div class="funnel-row">
          <div class="funnel-page">{p['path']}</div>
          <div class="funnel-bar-wrap"><div class="bar-wrap"><div class="bar-fill bar-purple" style="width:{w}%"></div></div></div>
          <div class="funnel-visits">{p['paid_views']}</div>
        </div>"""

    score_color = "#ef4444" if score < 50 else "#f59e0b" if score < 70 else "#10b981"
    grade = "F" if score < 40 else "D" if score < 60 else "C" if score < 75 else "B" if score < 90 else "A"

    # Score bar rows
    def sbar(label, val, max_val, color="red"):
        pct = int(val/max_val*100)
        c = "bar-green" if pct>66 else "bar-yellow" if pct>33 else "bar-red"
        tc = "text-red" if pct<40 else "text-yellow" if pct<66 else "text-green"
        return f"""<div style="margin-bottom:16px">
          <div class="hbar-label"><span>{label}</span><span class="{tc}">{val}/{max_val}</span></div>
          <div class="bar-wrap"><div class="bar-fill {c}" style="width:{pct}%"></div></div>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Boost.space — Google Ads Audit</title>
<script>
(function(){{
  const UH='67af3832462af6b6f04ed28647e1a14cb29216fcda2d90a32653bdec596a09ae';
  const PH='2ac34ace688955bc7dc6751c834380c3dae79f9f79056aeec1656e70f1e1e10e';
  async function sha256(s){{const b=new TextEncoder().encode(s);const h=await crypto.subtle.digest('SHA-256',b);return Array.from(new Uint8Array(h)).map(x=>x.toString(16).padStart(2,'0')).join('');}}
  async function tryLogin(e){{
    e&&e.preventDefault();
    const u=document.getElementById('_u').value;
    const p=document.getElementById('_p').value;
    const [uh,ph]=await Promise.all([sha256(u),sha256(p)]);
    if(uh===UH&&ph===PH){{sessionStorage.setItem('_auth','1');document.getElementById('_gate').style.display='none';document.getElementById('_app').style.display='block';}}
    else{{const err=document.getElementById('_err');err.textContent='Invalid credentials.';err.style.display='block';document.getElementById('_p').value='';}}
  }}
  window.addEventListener('DOMContentLoaded',function(){{
    if(sessionStorage.getItem('_auth')==='1'){{document.getElementById('_gate').style.display='none';document.getElementById('_app').style.display='block';}}
    else{{document.getElementById('_gate').style.display='flex';document.getElementById('_app').style.display='none';}}
    document.getElementById('_form').addEventListener('submit',tryLogin);
  }});
}})();

async function triggerRefresh(){{
  const btn=document.getElementById('refresh-btn');
  const status=document.getElementById('refresh-status');
  btn.disabled=true; btn.textContent='⏳ Triggering…';
  status.textContent='';
  try{{
    const res=await fetch('https://api.github.com/repos/Petrgyure/boost-space-audit/actions/workflows/refresh.yml/dispatches',{{
      method:'POST',
      headers:{{'Authorization':'Bearer '+atob('{__GHTOKEN__}'),'Accept':'application/vnd.github+json','Content-Type':'application/json'}},
      body:JSON.stringify({{'ref':'main'}})
    }});
    if(res.status===204){{
      status.textContent='✅ Refresh triggered! Page will update in ~60s.';
      status.style.color='#34d399';
      setTimeout(()=>location.reload(),65000);
    }}else{{
      status.textContent='❌ Error '+res.status+'. Check GitHub Actions.';
      status.style.color='#f87171';
    }}
  }}catch(e){{status.textContent='❌ Network error: '+e.message;status.style.color='#f87171';}}
  btn.disabled=false; btn.textContent='🔄 Refresh Data';
}}
</script>
<style>
  :root{{--bg:#0d0f14;--bg2:#13161e;--bg3:#1a1e28;--border:#252836;--accent:#6366f1;--accent2:#8b5cf6;--green:#10b981;--yellow:#f59e0b;--red:#ef4444;--blue:#3b82f6;--text:#e2e8f0;--muted:#64748b;--card:#161922;}}
  *{{box-sizing:border-box;margin:0;padding:0;}}
  body{{background:var(--bg);color:var(--text);font-family:'Inter',-apple-system,sans-serif;font-size:14px;line-height:1.6;}}
  .header{{background:linear-gradient(135deg,#0d0f14 0%,#1a1020 50%,#0d1020 100%);border-bottom:1px solid var(--border);padding:20px 32px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100;}}
  .header-left h1{{font-size:20px;font-weight:700;background:linear-gradient(90deg,#6366f1,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;}}
  .header-left p{{color:var(--muted);font-size:12px;margin-top:2px;}}
  .header-right{{display:flex;gap:12px;align-items:center;}}
  .score-badge{{background:linear-gradient(135deg,{score_color}cc,{score_color}88);border:1px solid {score_color}55;border-radius:12px;padding:10px 18px;text-align:center;}}
  .score-badge .score{{font-size:32px;font-weight:800;color:#fff;line-height:1;}}
  .score-badge .grade{{font-size:11px;color:rgba(255,255,255,0.7);text-transform:uppercase;letter-spacing:1px;margin-top:2px;}}
  .refresh-btn{{background:linear-gradient(135deg,#6366f1,#8b5cf6);border:none;border-radius:8px;padding:9px 16px;color:#fff;font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap;}}
  .refresh-btn:hover{{opacity:0.9;}}
  .refresh-btn:disabled{{opacity:0.5;cursor:not-allowed;}}
  .refresh-status{{font-size:12px;margin-top:4px;text-align:center;min-height:16px;}}
  .tabs{{display:flex;background:var(--bg2);border-bottom:1px solid var(--border);padding:0 24px;overflow-x:auto;position:sticky;top:73px;z-index:99;gap:0;}}
  .tab{{padding:14px 18px;cursor:pointer;font-size:13px;font-weight:500;color:var(--muted);border-bottom:2px solid transparent;white-space:nowrap;transition:all .2s;}}
  .tab:hover{{color:var(--text);}} .tab.active{{color:var(--accent);border-bottom-color:var(--accent);}}
  .tab-content{{display:none;padding:24px 32px;max-width:1400px;margin:0 auto;}} .tab-content.active{{display:block;}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;}}
  .card-title{{font-size:12px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.8px;margin-bottom:8px;}}
  .card-value{{font-size:28px;font-weight:700;color:var(--text);}}
  .card-sub{{font-size:12px;color:var(--muted);margin-top:4px;}}
  .grid-4{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px;}}
  .grid-3{{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:24px;}}
  .grid-2{{display:grid;grid-template-columns:repeat(2,1fr);gap:16px;margin-bottom:24px;}}
  .alert{{border-radius:10px;padding:14px 16px;margin-bottom:12px;display:flex;align-items:flex-start;gap:12px;border-left:3px solid;}}
  .alert-critical{{background:rgba(239,68,68,0.08);border-color:var(--red);}}
  .alert-warning{{background:rgba(245,158,11,0.08);border-color:var(--yellow);}}
  .alert-good{{background:rgba(16,185,129,0.08);border-color:var(--green);}}
  .alert-icon{{font-size:16px;flex-shrink:0;margin-top:1px;}}
  .alert-body strong{{font-weight:600;display:block;margin-bottom:3px;}}
  .alert-body p{{color:var(--muted);font-size:13px;}}
  .table-wrap{{background:var(--card);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:24px;}}
  .table-header{{padding:14px 16px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--border);}}
  .table-header h3{{font-size:14px;font-weight:600;}}
  table{{width:100%;border-collapse:collapse;font-size:13px;}}
  th{{text-align:left;padding:10px 12px;font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.6px;border-bottom:1px solid var(--border);background:var(--bg3);}}
  td{{padding:10px 12px;border-bottom:1px solid rgba(37,40,54,0.6);vertical-align:middle;}}
  tr:last-child td{{border-bottom:none;}} tr:hover td{{background:rgba(99,102,241,0.04);}}
  .num{{text-align:right;font-variant-numeric:tabular-nums;}}
  .badge{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;}}
  .badge-red{{background:rgba(239,68,68,0.15);color:#f87171;}} .badge-yellow{{background:rgba(245,158,11,0.15);color:#fbbf24;}}
  .badge-green{{background:rgba(16,185,129,0.15);color:#34d399;}} .badge-blue{{background:rgba(59,130,246,0.15);color:#60a5fa;}}
  .badge-purple{{background:rgba(139,92,246,0.15);color:#c084fc;}} .badge-gray{{background:rgba(100,116,139,0.2);color:#94a3b8;}}
  .bar-wrap{{background:var(--bg3);border-radius:4px;height:6px;overflow:hidden;margin-top:4px;}}
  .bar-fill{{height:100%;border-radius:4px;}}
  .bar-red{{background:linear-gradient(90deg,#ef4444,#f87171);}} .bar-yellow{{background:linear-gradient(90deg,#f59e0b,#fbbf24);}}
  .bar-green{{background:linear-gradient(90deg,#10b981,#34d399);}} .bar-purple{{background:linear-gradient(90deg,#6366f1,#a78bfa);}}
  .bar-blue{{background:linear-gradient(90deg,#3b82f6,#60a5fa);}}
  .section-title{{font-size:16px;font-weight:700;margin-bottom:16px;display:flex;align-items:center;gap:10px;}}
  .section-title::before{{content:'';display:block;width:4px;height:20px;background:linear-gradient(180deg,var(--accent),var(--accent2));border-radius:2px;}}
  .funnel-row{{display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--border);}}
  .funnel-row:last-child{{border-bottom:none;}} .funnel-page{{flex:1;font-size:12px;font-family:monospace;color:#a78bfa;}}
  .funnel-bar-wrap{{width:180px;}} .funnel-visits{{width:50px;text-align:right;font-size:13px;color:var(--muted);}}
  .insight{{background:linear-gradient(135deg,rgba(99,102,241,0.08),rgba(139,92,246,0.05));border:1px solid rgba(99,102,241,0.2);border-radius:10px;padding:16px;margin-bottom:16px;}}
  .insight h4{{font-size:13px;font-weight:600;color:#a78bfa;margin-bottom:6px;}} .insight p{{font-size:13px;color:var(--muted);}}
  .budget-row{{display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);}}
  .budget-row:last-child{{border-bottom:none;}} .budget-label{{color:var(--muted);font-size:13px;}} .budget-val{{font-weight:600;font-size:14px;}}
  .hbar-label{{font-size:12px;color:var(--muted);margin-bottom:2px;display:flex;justify-content:space-between;}}
  .divider{{height:1px;background:var(--border);margin:24px 0;}}
  .text-red{{color:var(--red);}} .text-green{{color:var(--green);}} .text-yellow{{color:var(--yellow);}} .text-muted{{color:var(--muted);}}
  .bofu{{background:rgba(16,185,129,0.15);color:#34d399;}} .mofu{{background:rgba(245,158,11,0.15);color:#fbbf24;}} .tofu{{background:rgba(59,130,246,0.15);color:#60a5fa;}}
  .competitor{{background:rgba(168,85,247,0.15);color:#c084fc;}} .irrelevant{{background:rgba(239,68,68,0.15);color:#f87171;}}
</style>
</head>
<body>
<div id="_gate" style="display:none;position:fixed;inset:0;z-index:9999;background:#0d0f14;align-items:center;justify-content:center;font-family:'Inter',-apple-system,sans-serif;">
  <div style="background:#13161e;border:1px solid #252836;border-radius:16px;padding:48px 40px;width:100%;max-width:400px;text-align:center;">
    <div style="width:48px;height:48px;background:linear-gradient(135deg,#6366f1,#8b5cf6);border-radius:12px;margin:0 auto 24px;display:flex;align-items:center;justify-content:center;">
      <svg width="24" height="24" fill="none" stroke="white" stroke-width="2" viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
    </div>
    <h2 style="color:#e2e8f0;font-size:20px;font-weight:700;margin-bottom:6px;">Boost.space Audit</h2>
    <p style="color:#64748b;font-size:13px;margin-bottom:32px;">Enter your credentials to access the report</p>
    <form id="_form" autocomplete="off">
      <input id="_u" type="text" placeholder="Username" style="width:100%;background:#1a1e28;border:1px solid #252836;border-radius:8px;padding:12px 14px;color:#e2e8f0;font-size:14px;margin-bottom:12px;outline:none;box-sizing:border-box;" onfocus="this.style.borderColor='#6366f1'" onblur="this.style.borderColor='#252836'"/>
      <input id="_p" type="password" placeholder="Password" style="width:100%;background:#1a1e28;border:1px solid #252836;border-radius:8px;padding:12px 14px;color:#e2e8f0;font-size:14px;margin-bottom:8px;outline:none;box-sizing:border-box;" onfocus="this.style.borderColor='#6366f1'" onblur="this.style.borderColor='#252836'"/>
      <div id="_err" style="display:none;color:#f87171;font-size:12px;margin-bottom:12px;text-align:left;"></div>
      <button type="submit" style="width:100%;background:linear-gradient(135deg,#6366f1,#8b5cf6);border:none;border-radius:8px;padding:13px;color:white;font-size:14px;font-weight:600;cursor:pointer;margin-top:4px;">Access Report →</button>
    </form>
    <p style="color:#334155;font-size:11px;margin-top:24px;">Boost.space · Confidential</p>
  </div>
</div>
<div id="_app" style="display:none;">
<div class="header">
  <div class="header-left">
    <h1>Boost.space — Google Ads Audit</h1>
    <p>Account 205-889-7291 &nbsp;·&nbsp; Generated {generated_at} &nbsp;·&nbsp; Last 30 days &nbsp;·&nbsp; 1 EUR = {EUR_RATE} CZK</p>
  </div>
  <div class="header-right">
    <div style="text-align:center">
      <button id="refresh-btn" class="refresh-btn" onclick="triggerRefresh()">🔄 Refresh Data</button>
      <div id="refresh-status" class="refresh-status"></div>
    </div>
    <div class="score-badge">
      <div class="score">{score}</div>
      <div class="grade">Health / {grade}</div>
    </div>
  </div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('dashboard')">📊 Dashboard</div>
  <div class="tab" onclick="showTab('campaigns')">🎯 Campaigns</div>
  <div class="tab" onclick="showTab('searchterms')">🔍 Search Terms</div>
  <div class="tab" onclick="showTab('funnel')">🏷️ Keyword Funnel</div>
  <div class="tab" onclick="showTab('posthog')">📈 PostHog Behaviour</div>
  <div class="tab" onclick="showTab('budget')">💰 Budget Model</div>
</div>

<!-- DASHBOARD -->
<div id="tab-dashboard" class="tab-content active">
  <div class="grid-4">
    <div class="card"><div class="card-title">Active Monthly Spend</div><div class="card-value text-red">{total_spend_eur}</div><div class="card-sub">{len([c for c in camps if c['cost_czk']>0])} campaigns active</div></div>
    <div class="card"><div class="card-title">Google Ads Conversions</div><div class="card-value text-red">{total_conv:.0f}</div><div class="card-sub">Reported in last 30 days</div></div>
    <div class="card"><div class="card-title">PostHog Paid Convs</div><div class="card-value text-yellow">{ph_paid_total}</div><div class="card-sub">Micro-convs with GCLID present</div></div>
    <div class="card"><div class="card-title">Paid Bounce Rate</div><div class="card-value text-red">{ph['bounce_pct']:.0f}%</div><div class="card-sub">Avg {ph['avg_pages']} pages/session</div></div>
  </div>
  <div class="section-title">Score Breakdown</div>
  <div class="grid-2">
    <div class="card">
      {sbar("Conversion Tracking", cats['tracking'], 25)}
      {sbar("Wasted Spend Control", cats['waste'], 20)}
      {sbar("Account Structure", cats['structure'], 15)}
    </div>
    <div class="card">
      {sbar("Keyword Relevance", cats['keywords'], 15)}
      {sbar("Ads &amp; Assets", cats['ads'], 15)}
      {sbar("Settings &amp; Bidding", cats['settings'], 10)}
    </div>
  </div>
  <div class="section-title">Estimated Waste</div>
  <div class="alert alert-critical">
    <div class="alert-icon">🔴</div>
    <div class="alert-body">
      <strong>~{waste_eur} / month on competitors &amp; irrelevant terms</strong>
      <p>Top terms: {', '.join(t['term'] for t in terms[:5] if classify_term(t['term'])[0] in ('COMPETITOR','IRRELEVANT'))}</p>
    </div>
  </div>
  <div class="alert alert-critical">
    <div class="alert-icon">🔴</div>
    <div class="alert-body">
      <strong>{ph_total_hv} high-value conversions in PostHog — 0 attributed to paid</strong>
      <p>GCLID not persisted across sessions. Deploy 90-day GCLID cookie to recover attribution on demo &amp; lead events.</p>
    </div>
  </div>
</div>

<!-- CAMPAIGNS -->
<div id="tab-campaigns" class="tab-content">
  <div class="section-title">Active Campaigns — Last 30 Days</div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Campaign</th><th>Type</th><th class="num">Impr.</th><th class="num">Clicks</th><th class="num">CTR</th><th class="num">Cost</th><th class="num">CPC</th><th class="num">Conv.</th><th class="num">CPA</th><th class="num">Imp Share</th><th class="num">Rank Lost</th><th>Verdict</th></tr></thead>
      <tbody>{camp_rows}</tbody>
    </table>
  </div>
</div>

<!-- SEARCH TERMS -->
<div id="tab-searchterms" class="tab-content">
  <div class="section-title">Search Terms — Top Spend → Low Spend</div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Search Term</th><th>Campaign</th><th class="num">Cost</th><th class="num">Clicks</th><th class="num">CPC</th><th class="num">CTR</th><th class="num">Conv.</th><th>Intent</th><th>Action</th></tr></thead>
      <tbody>{term_rows}</tbody>
    </table>
  </div>
</div>

<!-- KEYWORD FUNNEL -->
<div id="tab-funnel" class="tab-content">
  <div class="section-title">Keyword Funnel — TOFU / MOFU / BOFU</div>
  {kw_sections}
</div>

<!-- POSTHOG -->
<div id="tab-posthog" class="tab-content">
  <div class="section-title">PostHog — Paid Visitor Behaviour (GCLID, 30 days)</div>
  <div class="grid-4">
    <div class="card"><div class="card-title">Sessions w/ GCLID</div><div class="card-value">{ph['sessions']}</div></div>
    <div class="card"><div class="card-title">Bounce Rate</div><div class="card-value text-red">{ph['bounce_pct']:.0f}%</div></div>
    <div class="card"><div class="card-title">Avg Pages / Session</div><div class="card-value text-yellow">{ph['avg_pages']}</div></div>
    <div class="card"><div class="card-title">High-Value Convs (Paid)</div><div class="card-value text-red">0</div><div class="card-sub">GCLID not persisted across sessions</div></div>
  </div>
  <div class="grid-2">
    <div class="table-wrap">
      <div class="table-header"><h3>Landing Pages — Paid</h3></div>
      <div style="padding:12px 16px">{pages_html}</div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:12px">Conversion Events — Paid vs Total</div>
      {conv_rows_html}
      <div class="insight" style="margin-top:16px">
        <h4>🔑 GCLID Persistence Gap</h4>
        <p>Demos happen on return visits. Store GCLID in a 90-day cookie on first landing to recover attribution.</p>
      </div>
    </div>
  </div>
</div>

<!-- BUDGET -->
<div id="tab-budget" class="tab-content">
  <div class="section-title">3-Month Plan — 3 → 200 Conversions/Month</div>
  <div class="grid-3">
    <div class="card"><div style="color:#60a5fa;font-size:16px;font-weight:700;margin-bottom:4px">Month 1 — Repair</div>
      <div class="budget-row"><div class="budget-label">Stop waste, fix tracking, rebuild GEO</div></div>
      <div class="budget-row"><div class="budget-label">Target budget</div><div class="budget-val" style="color:#60a5fa">€716</div></div>
      <div class="budget-row"><div class="budget-label">Expected conversions</div><div class="budget-val text-yellow">25–40</div></div>
      <div class="budget-row"><div class="budget-label">Target CPA</div><div class="budget-val">€18–29</div></div>
    </div>
    <div class="card"><div style="color:#fbbf24;font-size:16px;font-weight:700;margin-bottom:4px">Month 2 — Rebuild</div>
      <div class="budget-row"><div class="budget-label">New campaigns, landing pages, RLSA</div></div>
      <div class="budget-row"><div class="budget-label">Target budget</div><div class="budget-val" style="color:#fbbf24">€1,350</div></div>
      <div class="budget-row"><div class="budget-label">Expected conversions</div><div class="budget-val text-yellow">70–100</div></div>
      <div class="budget-row"><div class="budget-label">Target CPA</div><div class="budget-val">€14–19</div></div>
    </div>
    <div class="card"><div style="color:#34d399;font-size:16px;font-weight:700;margin-bottom:4px">Month 3 — Scale</div>
      <div class="budget-row"><div class="budget-label">Scale winners, PMax, full attribution</div></div>
      <div class="budget-row"><div class="budget-label">Target budget</div><div class="budget-val" style="color:#34d399">€2,249</div></div>
      <div class="budget-row"><div class="budget-label">Expected conversions</div><div class="budget-val text-green">180–220</div></div>
      <div class="budget-row"><div class="budget-label">Target CPA</div><div class="budget-val">€10–12</div></div>
    </div>
  </div>
</div>

<script>
function showTab(id){{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));
  document.getElementById('tab-'+id).classList.add('active');
  event.target.classList.add('active');
}}
</script>
</div>
</body>
</html>"""

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    print("Pulling Google Ads data…", flush=True)
    camps, terms, keywords = pull_google_ads()
    print(f"  {len(camps)} campaigns, {len(terms)} search terms, {len(keywords)} keywords")

    print("Pulling PostHog data…", flush=True)
    ph = pull_posthog()
    print(f"  {ph['sessions']} paid sessions, bounce {ph['bounce_pct']}%")

    score, cats = calc_score(camps, terms, ph)
    print(f"Health score: {score}/100")

    generated_at = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # Inject GH token placeholder — filled by workflow
    gh_token_b64 = os.environ.get("GH_TOKEN_B64", "")
    html = build_html(camps, terms, keywords, ph, score, cats, generated_at)
    html = html.replace("{__GHTOKEN__}", gh_token_b64)

    out = os.path.join(os.path.dirname(__file__), "..", "index.html")
    with open(out, "w") as f:
        f.write(html)
    print(f"Written → {out}")
