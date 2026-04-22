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
    "login_customer_id":os.environ["GADS_LOGIN_CID"].replace("-", ""),
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
POSTHOG_STUB = {
    "conversions": [], "pages": [], "sessions": 0, "avg_pages": 0, "bounce_pct": 0,
    "_stub": True,
}

def pull_posthog():
    try:
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
    except Exception as e:
        print(f"  ⚠️  PostHog unavailable ({e}) — using stub. Update POSTHOG_KEY in ACCESS.md.")
        return POSTHOG_STUB

# ── RLSA / Extensions / Shared Negatives constants ───────────────────────────
SHARED_NEG_LIST_ID = "12011903840"   # "Competitors + Irrelevant" — linked to all 9 campaigns

RLSA_RECS = [
    {"id": "7565330762", "name": "all - 540",                      "adj": +20,  "action": "observe",  "note": "All visitors 540-day window"},
    {"id": "7566114910", "name": "all - 7",                        "adj": +40,  "action": "observe",  "note": "Recent 7-day visitors — high intent"},
    {"id": "7277020330", "name": "AdWords optimized list",         "adj": +15,  "action": "observe",  "note": "Google smart remarketing list"},
    {"id": "8144411558", "name": "All Users of Boost.space Master","adj": -100, "action": "exclude",  "note": "Existing platform users — exclude from prospecting"},
    {"id": "7278536549", "name": "All Converters",                 "adj": -100, "action": "exclude",  "note": "Already converted — exclude to avoid waste"},
]

CAMP_TARGET_GROUP = {
    ("CDP",    "Marketing"):  "Marketing Ops / CMO / CDP Evaluator",
    ("PIM",    "Commerce"):   "E-commerce Manager / Marketplace Seller / Product Ops",
    ("Search", "Technology"): "Developer / IT Director / Data Ops",
}

LANDING_PAGES = {
    "CDP | Marketing - AI Personalized Activation": "https://boost.space/solutions/marketing/ai-personalized-activation",
    "CDP | Marketing - Dynamic Segmentation":       "https://boost.space/solutions/marketing/dynamic-segmentation",
    "CDP | Marketing - Revenue Attribution":        "https://boost.space/solutions/marketing/real-time-revenue-attribution",
    "CDP | Marketing - Unified Customer Profiles":  "https://boost.space/solutions/marketing/unified-customer-profiles",
    "PIM | Commerce - Dynamic Pricing":             "https://boost.space/solutions/commerce/dynamic-pricing",
    "PIM | Commerce - GEO Catalog":                 "https://boost.space/solutions/commerce/geo-ai-catalog-optimization",
    "PIM | Commerce - Marketplace Expansion":       "https://boost.space/solutions/commerce/marketplace-expansion",
    "PIM | Commerce - Supplier Catalog":            "https://boost.space/solutions/commerce/supplier-catalog-automation",
    "Search | Technology | EN":                     "https://boost.space/",
}

_BSU = "https://boost.space"
CAMP_SITELINKS = {
    "CDP | Marketing - AI Personalized Activation": [
        ("Book a Demo",     "See it live in 30 minutes",        "No setup required",           f"{_BSU}/book-demo"),
        ("See Pricing",     "Transparent, modular pricing",     "Scale as you grow",           f"{_BSU}/pricing"),
        ("Customer Stories","How teams automate personalization","Real results, real use cases",f"{_BSU}/case-studies"),
        ("Compare Plans",   "Find the right plan for your team","Free trial included",          f"{_BSU}/pricing"),
    ],
    "CDP | Marketing - Dynamic Segmentation": [
        ("Book a Demo",      "See dynamic segments in action", "Live in 30 minutes",           f"{_BSU}/book-demo"),
        ("See Pricing",      "Transparent, modular pricing",   "Scale as you grow",            f"{_BSU}/pricing"),
        ("Segmentation Guide","Build smarter audience segments","With AI and real-time data",  f"{_BSU}/blog"),
        ("Compare Plans",    "Find the right plan for your team","Free trial included",         f"{_BSU}/pricing"),
    ],
    "CDP | Marketing - Revenue Attribution": [
        ("Book a Demo",      "See attribution in action",      "30-minute live walkthrough",   f"{_BSU}/book-demo"),
        ("See Pricing",      "Transparent, modular pricing",   "Scale as you grow",            f"{_BSU}/pricing"),
        ("Attribution Guide","Multi-touch attribution explained","Connect spend to revenue",   f"{_BSU}/blog"),
        ("Compare Plans",    "Find the right plan for your team","Free trial included",         f"{_BSU}/pricing"),
    ],
    "CDP | Marketing - Unified Customer Profiles": [
        ("Book a Demo",      "See unified profiles live",      "30-minute walkthrough",        f"{_BSU}/book-demo"),
        ("See Pricing",      "Transparent, modular pricing",   "Scale as you grow",            f"{_BSU}/pricing"),
        ("CDP Buyer's Guide","What to look for in a CDP",      "Free guide for evaluators",    f"{_BSU}/blog"),
        ("Compare Plans",    "Find the right plan for your team","Free trial included",         f"{_BSU}/pricing"),
    ],
    "PIM | Commerce - Dynamic Pricing": [
        ("Book a Demo",     "See repricing in action",         "Live in 30 minutes",           f"{_BSU}/book-demo"),
        ("See Pricing",     "Transparent, modular pricing",    "Scale as you grow",            f"{_BSU}/pricing"),
        ("Repricing Guide", "Automate competitive pricing",    "Across all channels",          f"{_BSU}/blog"),
        ("Compare Plans",   "Find the right plan for your team","Free trial included",          f"{_BSU}/pricing"),
    ],
    "PIM | Commerce - GEO Catalog": [
        ("Book a Demo",     "See GEO catalog optimization",    "Live in 30 minutes",           f"{_BSU}/book-demo"),
        ("See Pricing",     "Transparent, modular pricing",    "Scale as you grow",            f"{_BSU}/pricing"),
        ("GEO AI Guide",    "Optimize for AI-powered search",  "Stay visible in LLMs",         f"{_BSU}/blog"),
        ("Compare Plans",   "Find the right plan for your team","Free trial included",          f"{_BSU}/pricing"),
    ],
    "PIM | Commerce - Marketplace Expansion": [
        ("Book a Demo",       "See marketplace automation",    "Live in 30 minutes",           f"{_BSU}/book-demo"),
        ("See Pricing",       "Transparent, modular pricing",  "Scale as you grow",            f"{_BSU}/pricing"),
        ("Marketplace Guide", "Expand to new channels faster", "Automate product feeds",       f"{_BSU}/blog"),
        ("Compare Plans",     "Find the right plan for your team","Free trial included",        f"{_BSU}/pricing"),
    ],
    "PIM | Commerce - Supplier Catalog": [
        ("Book a Demo",       "See supplier sync live",        "30-minute walkthrough",        f"{_BSU}/book-demo"),
        ("See Pricing",       "Transparent, modular pricing",  "Scale as you grow",            f"{_BSU}/pricing"),
        ("Integration Guide", "Connect any supplier feed",     "No-code data mapping",         f"{_BSU}/blog"),
        ("Compare Plans",     "Find the right plan for your team","Free trial included",        f"{_BSU}/pricing"),
    ],
    "Search | Technology | EN": [
        ("Book a Demo",    "See Boost.space in action",        "30-minute live demo",          f"{_BSU}/book-demo"),
        ("See Pricing",    "Transparent, modular pricing",     "Scale as you grow",            f"{_BSU}/pricing"),
        ("Documentation",  "Full API and integration docs",    "For developers and admins",    f"{_BSU}/docs"),
        ("Case Studies",   "How tech teams use Boost.space",   "Real results from real teams", f"{_BSU}/case-studies"),
    ],
}

UNIVERSAL_CALLOUTS = [
    "No-code setup", "14-day free trial", "GDPR compliant",
    "EU data residency", "SOC 2 certified", "Live support",
    "API access included", "Free onboarding",
]

CAMP_SNIPPETS = {
    "CDP | Marketing - AI Personalized Activation": ("Features", "AI Personalization;Audience Segments;Predictive Analytics;Real-time Data;Multi-channel Activation"),
    "CDP | Marketing - Dynamic Segmentation":       ("Features", "Dynamic Segments;Behavioral Targeting;Lookalike Audiences;Real-time Sync;Cross-channel Segments"),
    "CDP | Marketing - Revenue Attribution":        ("Features", "Multi-touch Attribution;Revenue Tracking;Campaign ROI;Conversion Paths;Data-driven Attribution"),
    "CDP | Marketing - Unified Customer Profiles":  ("Features", "Unified Profiles;First-party Data;Identity Resolution;360-degree View;Real-time Updates"),
    "PIM | Commerce - Dynamic Pricing":             ("Features", "Real-time Repricing;Competitor Monitoring;AI Price Optimization;Multi-channel Sync;Price Rules Engine"),
    "PIM | Commerce - GEO Catalog":                 ("Features", "AI Catalog Optimization;GEO Visibility;Schema Markup;Product Discovery;LLM Optimization"),
    "PIM | Commerce - Marketplace Expansion":       ("Features", "Multi-marketplace Sync;Product Feed Automation;Channel Management;Listing Optimization;Profitability Tracking"),
    "PIM | Commerce - Supplier Catalog":            ("Features", "Supplier Feed Sync;Data Normalization;Deduplication;Catalog Automation;API Integrations"),
    "Search | Technology | EN":                     ("Services", "CDP Marketing;PIM Commerce;Data Unification;AI Automation;API Integrations"),
}


def pull_user_lists():
    """Pull available remarketing/audience lists with search sizes."""
    from google.ads.googleads.client import GoogleAdsClient
    client = GoogleAdsClient.load_from_dict(GADS_CONFIG)
    svc = client.get_service("GoogleAdsService")
    result = []
    try:
        for row in svc.search(customer_id=CUSTOMER_ID, query="""
            SELECT user_list.id, user_list.name, user_list.type, user_list.size_for_search
            FROM user_list WHERE user_list.membership_status = 'OPEN'
            ORDER BY user_list.size_for_search DESC
        """):
            ul = row.user_list
            result.append({"id": str(ul.id), "name": ul.name,
                           "type": ul.type.name, "search_size": ul.size_for_search})
    except Exception as e:
        print(f"  ⚠️  pull_user_lists: {e}")
    return result


def pull_rlsa_assignments():
    """Pull existing RLSA/audience targeting per ad group."""
    from google.ads.googleads.client import GoogleAdsClient
    client = GoogleAdsClient.load_from_dict(GADS_CONFIG)
    svc = client.get_service("GoogleAdsService")
    result = []
    try:
        for row in svc.search(customer_id=CUSTOMER_ID, query="""
            SELECT campaign.name, ad_group.name,
                   ad_group_criterion.user_list.user_list,
                   ad_group_criterion.bid_modifier
            FROM ad_group_criterion
            WHERE campaign.status = 'ENABLED' AND ad_group_criterion.type = 'USER_LIST'
        """):
            result.append({
                "campaign": row.campaign.name,
                "adgroup":  row.ad_group.name,
                "list_res": row.ad_group_criterion.user_list.user_list,
                "bid_mod":  row.ad_group_criterion.bid_modifier,
            })
    except Exception as e:
        print(f"  ⚠️  pull_rlsa_assignments: {e}")
    return result


def pull_assets():
    """Pull current sitelinks, callouts, structured snippets and campaign assignments."""
    from google.ads.googleads.client import GoogleAdsClient
    client = GoogleAdsClient.load_from_dict(GADS_CONFIG)
    svc = client.get_service("GoogleAdsService")
    sitelinks, callouts, snippets, assignments = [], [], [], {}
    try:
        for row in svc.search(customer_id=CUSTOMER_ID, query="""
            SELECT asset.id, asset.sitelink_asset.link_text,
                   asset.sitelink_asset.description1, asset.sitelink_asset.description2
            FROM asset WHERE asset.type = 'SITELINK' LIMIT 200
        """):
            a = row.asset.sitelink_asset
            sitelinks.append({"link_text": a.link_text, "desc1": a.description1, "desc2": a.description2})
    except Exception as e:
        print(f"  ⚠️  pull_assets/sitelinks: {e}")
    try:
        for row in svc.search(customer_id=CUSTOMER_ID, query="""
            SELECT asset.callout_asset.callout_text FROM asset WHERE asset.type = 'CALLOUT' LIMIT 200
        """):
            callouts.append(row.asset.callout_asset.callout_text)
    except Exception as e:
        print(f"  ⚠️  pull_assets/callouts: {e}")
    try:
        for row in svc.search(customer_id=CUSTOMER_ID, query="""
            SELECT asset.structured_snippet_asset.header, asset.structured_snippet_asset.values
            FROM asset WHERE asset.type = 'STRUCTURED_SNIPPET' LIMIT 100
        """):
            s = row.asset.structured_snippet_asset
            snippets.append({"header": s.header, "values": list(s.values)})
    except Exception as e:
        print(f"  ⚠️  pull_assets/snippets: {e}")
    try:
        for row in svc.search(customer_id=CUSTOMER_ID, query="""
            SELECT campaign.name, campaign_asset.field_type, campaign.status
            FROM campaign_asset
            WHERE campaign.status = 'ENABLED' AND campaign_asset.status = 'ENABLED'
            ORDER BY campaign.name
        """):
            cn = row.campaign.name
            ft = row.campaign_asset.field_type.name
            assignments.setdefault(cn, [])
            if ft not in assignments[cn]:
                assignments[cn].append(ft)
    except Exception as e:
        print(f"  ⚠️  pull_assets/assignments: {e}")
    return {"sitelinks": sitelinks, "callouts": callouts, "snippets": snippets, "assignments": assignments}


def pull_shared_neg_list():
    """Pull contents of the shared 'Competitors + Irrelevant' negative keyword list."""
    from google.ads.googleads.client import GoogleAdsClient
    client = GoogleAdsClient.load_from_dict(GADS_CONFIG)
    svc = client.get_service("GoogleAdsService")
    terms = []
    try:
        for row in svc.search(customer_id=CUSTOMER_ID, query=f"""
            SELECT shared_criterion.keyword.text, shared_criterion.keyword.match_type
            FROM shared_criterion WHERE shared_set.id = {SHARED_NEG_LIST_ID}
            ORDER BY shared_criterion.keyword.text
        """):
            kw = row.shared_criterion.keyword
            terms.append({"text": kw.text, "match_type": kw.match_type.name})
    except Exception as e:
        print(f"  ⚠️  pull_shared_neg_list: {e}")
    return terms


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

# ── Campaign name parser ──────────────────────────────────────────────────────
def parse_camp_name(name):
    """
    Parses naming convention: '[Product] | [Vertical] - [Use Case]'
    or '[Product] | [Vertical] | [Region]'
    Returns dict with product, vertical, usecase, badge_cls, short
    """
    if " | " in name:
        parts = name.split(" | ")
        product = parts[0].strip()
        if " - " in name:
            # CDP | Marketing - AI Personalized Activation
            rest = parts[1] if len(parts) > 1 else ""
            vertical, usecase = rest.split(" - ", 1) if " - " in rest else (rest, "")
            vertical = vertical.strip(); usecase = usecase.strip()
        else:
            # Search | Technology | EN
            vertical = parts[1].strip() if len(parts) > 1 else ""
            usecase  = parts[2].strip() if len(parts) > 2 else ""
        badge = ("badge-purple" if product == "CDP"
                 else "badge-blue" if product == "PIM"
                 else "badge-green" if product == "Search"
                 else "badge-gray")
        label = usecase if usecase else vertical
        return {"product": product, "vertical": vertical, "usecase": usecase,
                "badge": badge, "label": label, "full": name}
    return {"product": "", "vertical": "", "usecase": name,
            "badge": "badge-gray", "label": name, "full": name}

def adgroup_funnel(ag_name):
    """Classify ad group by name pattern → BOFU / MOFU / TOFU"""
    n = ag_name.lower()
    if any(x in n for x in ["alternative","comparison","pricing","price","demo","trial","switch","migration"]):
        return "BOFU", "badge-green bofu"
    if any(x in n for x in ["software","platform","management","analytics","solution","optimization","tool","system"]):
        return "MOFU", "badge-yellow mofu"
    return "TOFU", "badge-blue tofu"

# ── CSV export helpers ────────────────────────────────────────────────────────
def to_csv_js(rows):
    """Convert list of lists to a CSV string, JSON-encoded for safe JS embedding."""
    lines = []
    for row in rows:
        cells = ['"' + str(v).replace('"', '""') + '"' for v in row]
        lines.append(",".join(cells))
    return json.dumps("\n".join(lines))

# ── Score calculation ─────────────────────────────────────────────────────────
def calc_score(camps, terms, ph):
    total_cost = sum(c["cost_czk"] for c in camps)
    zero_conv  = sum(1 for c in camps if c["cost_czk"] > 500 and c["conv"] == 0)
    waste_czk  = sum(t["cost_czk"] for t in terms
                     if classify_term(t["term"])[0] in ("COMPETITOR","IRRELEVANT"))
    waste_pct  = (waste_czk / total_cost * 100) if total_cost else 0
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
def build_html(camps, terms, keywords, ph, score, cats, generated_at,
               user_lists=None, rlsa_assignments=None, assets=None, shared_neg_list=None):
    total_spend_czk = sum(c["cost_czk"] for c in camps)
    total_spend_eur = eur(total_spend_czk)
    total_conv      = sum(c["conv"] for c in camps)
    waste_czk       = sum(t["cost_czk"] for t in terms
                          if classify_term(t["term"])[0] in ("COMPETITOR","IRRELEVANT"))
    waste_eur       = eur(waste_czk)
    ph_paid_total   = sum(c["paid"] for c in ph["conversions"])
    ph_total_hv     = sum(c["total"] for c in ph["conversions"]
                          if c["event"] in ("demo_meeting_booked","leady_new_lead","be_form_submit_demo_lead"))

    # ── Campaign rows (grouped by product) ───────────────────────────────────
    def camp_row(c):
        pn = parse_camp_name(c['name'])
        prod_badge = f'<span class="badge {pn["badge"]}" style="margin-right:6px;font-size:10px">{pn["product"]}</span>' if pn["product"] else ""
        name_cell  = f'{prod_badge}<strong>{pn["label"]}</strong>'
        verdict    = "badge-red" if c["conv"] == 0 and c["cost_czk"] > 200 else "badge-yellow"
        vtext      = "Pause" if c["conv"] == 0 and c["cost_czk"] > 300 else ("Rebuild" if c["conv"] < 5 else "Scale")
        is_bar  = f'<div class="bar-wrap"><div class="bar-fill bar-{"red" if (c["is"] or 0)<20 else "yellow"}" style="width:{c["is"] or 0}%"></div></div>' if c["is"] else ""
        rl_bar  = f'<div class="bar-wrap"><div class="bar-fill bar-red" style="width:{c["rank_lost"] or 0}%"></div></div>' if c["rank_lost"] else ""
        return f"""<tr>
          <td>{name_cell}<div style="font-size:10px;color:var(--muted);margin-top:2px">{pn["vertical"]}</div></td>
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

    # Group campaigns by product
    camps_by_product = {}
    for c in camps:
        pn = parse_camp_name(c["name"])
        camps_by_product.setdefault(pn["product"] or "Other", []).append(c)

    camp_rows_html = ""
    for product in sorted(camps_by_product.keys()):
        badge_cls = ("badge-purple" if product == "CDP" else "badge-blue" if product == "PIM"
                     else "badge-green" if product == "Search" else "badge-gray")
        camp_rows_html += f'<tr><td colspan="12" style="background:var(--bg3);padding:8px 12px;font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.8px"><span class="badge {badge_cls}" style="margin-right:8px">{product}</span>{camps_by_product[product][0]["name"].split(" | ")[1].split(" - ")[0].strip() if " | " in camps_by_product[product][0]["name"] else ""}</td></tr>\n'
        for c in camps_by_product[product]:
            camp_rows_html += camp_row(c) + "\n"

    # ── Search term rows ──────────────────────────────────────────────────────
    def term_row(t):
        intent, cls = classify_term(t["term"])
        pn   = parse_camp_name(t["campaign"])
        ag_f, ag_cls = adgroup_funnel(t["adgroup"])
        prod_badge = f'<span class="badge {pn["badge"]}" style="margin-right:4px;font-size:10px">{pn["product"]}</span>' if pn["product"] else ""
        camp_cell  = f'{prod_badge}<span style="font-size:11px">{pn["label"]}</span>'
        ag_cell    = f'<span class="badge {ag_cls}" style="font-size:10px">{ag_f}</span> <span style="font-size:11px;color:var(--muted)">{t["adgroup"]}</span>'
        action = "→ Negative" if intent in ("COMPETITOR","IRRELEVANT") else ("Fix QS" if t["conv"]==0 and t["cost_czk"]>500 else "Keep")
        ab = "badge-red" if intent in ("COMPETITOR","IRRELEVANT") else ("badge-green" if t["conv"]>0 else "badge-yellow")
        return f"""<tr>
          <td><strong>{t['term']}</strong></td>
          <td>{camp_cell}</td>
          <td>{ag_cell}</td>
          <td class="num">{t['cost_eur']}</td>
          <td class="num">{t['clicks']}</td>
          <td class="num">{t['cpc_eur']}</td>
          <td class="num">{t['ctr']}%</td>
          <td class="num">{t['conv']}</td>
          <td><span class="badge {cls.split()[0]}">{intent}</span></td>
          <td><span class="badge {ab}">{action}</span></td>
        </tr>"""

    term_rows = "\n".join(term_row(t) for t in terms[:50])

    # ── Keyword funnel rows grouped by Product → Use Case → Adgroup ──────────
    # Build structure: product → campaign → adgroup → [keywords]
    kw_tree = {}
    for k in keywords:
        pn = parse_camp_name(k["campaign"])
        prod = pn["product"] or "Other"
        camp = k["campaign"]
        ag   = k["adgroup"]
        kw_tree.setdefault(prod, {}).setdefault(camp, {}).setdefault(ag, []).append(k)

    kw_sections = ""
    for product in sorted(kw_tree.keys()):
        prod_badge_cls = ("badge-purple" if product == "CDP" else "badge-blue" if product == "PIM"
                          else "badge-green" if product == "Search" else "badge-gray")
        kw_sections += f'<div style="margin:24px 0 12px"><span class="badge {prod_badge_cls}" style="font-size:14px;padding:5px 14px;margin-right:8px">{product}</span></div>\n'

        for camp_name in sorted(kw_tree[product].keys()):
            pn2  = parse_camp_name(camp_name)
            camp_kws_all = [k for ags in kw_tree[product][camp_name].values() for k in ags]
            total_c = sum(k["cost_czk"] for k in camp_kws_all)
            total_v = sum(k["conv"] for k in camp_kws_all)

            ag_tables = ""
            for ag_name in sorted(kw_tree[product][camp_name].keys()):
                ag_kws = sorted(kw_tree[product][camp_name][ag_name], key=lambda x: -x["cost_czk"])[:20]
                ag_f, ag_cls = adgroup_funnel(ag_name)
                ag_cost = sum(k["cost_czk"] for k in ag_kws)
                ag_conv = sum(k["conv"] for k in ag_kws)
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
                kw_rows_str = "\n".join(kw_row(k) for k in ag_kws)
                ag_tables += f"""
                <div style="margin-bottom:12px">
                  <div style="padding:8px 12px;background:var(--bg3);display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--border)">
                    <span class="badge {ag_cls}" style="font-size:10px">{ag_f}</span>
                    <span style="font-size:12px;font-weight:600">{ag_name}</span>
                    <span style="color:var(--muted);font-size:11px;margin-left:auto">{len(ag_kws)} kws · {eur(ag_cost)} · {ag_conv:.0f} conv</span>
                  </div>
                  <table>
                    <thead><tr><th>Keyword</th><th>Match</th><th>QS</th><th class="num">Cost</th><th class="num">Clicks</th><th class="num">Conv.</th><th>Funnel</th></tr></thead>
                    <tbody>{kw_rows_str}</tbody>
                  </table>
                </div>"""

            kw_sections += f"""
            <div class="table-wrap" style="margin-bottom:20px">
              <div class="table-header">
                <h3><span class="badge {prod_badge_cls}" style="margin-right:6px;font-size:11px">{product}</span>{pn2["label"]}</h3>
                <span style="color:var(--muted);font-size:12px">{len(camp_kws_all)} keywords · {eur(total_c)} spend · {total_v:.0f} conv</span>
              </div>
              {ag_tables}
            </div>"""

    # ── PostHog conversion table ──────────────────────────────────────────────
    conv_rows_html = ""
    for c in ph["conversions"]:
        pct = round(c["paid"]/c["total"]*100,1) if c["total"] else 0
        ok  = c["paid"] > 0
        conv_rows_html += f"""<div class="budget-row">
          <div class="budget-label {'text-red' if not ok else ''}">{c['event']} {'❌' if not ok else '✅'}</div>
          <div class="budget-val"><strong {'class="text-red"' if not ok else ''}>{c['paid']}</strong>
            <span class="text-muted"> / {c['total']} total ({pct}%)</span></div>
        </div>"""

    pages_html = ""
    max_pv = ph["pages"][0]["paid_views"] if ph["pages"] else 1
    for p in ph["pages"]:
        w = int(p["paid_views"] / max_pv * 100)
        pages_html += f"""<div class="funnel-row">
          <div class="funnel-page">{p['path']}</div>
          <div class="funnel-bar-wrap"><div class="bar-wrap"><div class="bar-fill bar-purple" style="width:{w}%"></div></div></div>
          <div class="funnel-visits">{p['paid_views']}</div>
        </div>"""

    # ── Score bars ────────────────────────────────────────────────────────────
    score_color = "#ef4444" if score < 50 else "#f59e0b" if score < 70 else "#10b981"
    grade = "F" if score < 40 else "D" if score < 60 else "C" if score < 75 else "B" if score < 90 else "A"

    def sbar(label, val, max_val, color="red"):
        pct = int(val/max_val*100)
        c = "bar-green" if pct>66 else "bar-yellow" if pct>33 else "bar-red"
        tc = "text-red" if pct<40 else "text-yellow" if pct<66 else "text-green"
        return f"""<div style="margin-bottom:16px">
          <div class="hbar-label"><span>{label}</span><span class="{tc}">{val}/{max_val}</span></div>
          <div class="bar-wrap"><div class="bar-fill {c}" style="width:{pct}%"></div></div>
        </div>"""

    # ── Playbook data ─────────────────────────────────────────────────────────
    # Negative keywords grouped by campaign
    neg_by_camp = {}
    for t in terms:
        intent, _ = classify_term(t["term"])
        if intent in ("COMPETITOR", "IRRELEVANT"):
            neg_by_camp.setdefault(t["campaign"], {"terms": [], "intent": {}})
            if t["term"] not in neg_by_camp[t["campaign"]]["terms"]:
                neg_by_camp[t["campaign"]]["terms"].append(t["term"])
                neg_by_camp[t["campaign"]]["intent"][t["term"]] = intent

    all_neg_terms = sorted(set(
        t["term"] for t in terms
        if classify_term(t["term"])[0] in ("COMPETITOR", "IRRELEVANT")
    ))
    all_neg_text = "\n".join(f"[{term}]" for term in all_neg_terms)

    # ── Google Ads CSV exports ────────────────────────────────────────────────
    NEG_HEADERS = ["Campaign", "Ad Group", "Account keyword type", "Keyword", "Criterion Type"]

    # Campaign-level negatives (one row per campaign × term, deduped)
    seen_neg = set()
    neg_camp_csv_rows = [NEG_HEADERS]
    for t in terms:
        intent, _ = classify_term(t["term"])
        if intent in ("COMPETITOR", "IRRELEVANT"):
            key = (t["campaign"], t["term"])
            if key not in seen_neg:
                seen_neg.add(key)
                neg_camp_csv_rows.append([t["campaign"], "", "Excluded negative keywords", t["term"], "Negative Exact"])
    neg_camp_csv_js = to_csv_js(neg_camp_csv_rows)

    # Account-level negatives (all unique terms, applied account-wide)
    neg_acct_csv_rows = [NEG_HEADERS]
    for term in all_neg_terms:
        neg_acct_csv_rows.append(["<account>", "", "Excluded negative keywords", term, "Negative Exact"])
    neg_acct_csv_js = to_csv_js(neg_acct_csv_rows)

    # Converting search terms not yet added as keywords → suggest adding
    existing_kw_texts = {k["keyword"].lower() for k in keywords}
    add_kw_rows = [["Campaign", "Ad Group", "Keyword", "Criterion Type"]]
    seen_add = set()
    for t in sorted(terms, key=lambda x: -x["conv"]):
        intent, _ = classify_term(t["term"])
        key = (t["campaign"], t["adgroup"], t["term"].lower())
        if (t["conv"] > 0 and intent not in ("COMPETITOR", "IRRELEVANT")
                and t["term"].lower() not in existing_kw_texts
                and key not in seen_add):
            seen_add.add(key)
            match = "Exact" if t["conv"] >= 2 else "Phrase"
            add_kw_rows.append([t["campaign"], t["adgroup"], t["term"], match])
    add_kw_csv_js  = to_csv_js(add_kw_rows)
    has_add_kws    = len(add_kw_rows) > 1
    add_kw_count   = len(add_kw_rows) - 1

    # Campaign actions CSV (for reference / tracking)
    camp_act_rows = [["Campaign", "Vertical", "Use Case", "Action", "Reason", "30d Spend", "30d Conv"]]
    for c in sorted(camps, key=lambda x: (0 if x["conv"] == 0 and x["cost_czk"] > 300 else
                                           1 if x["conv"] == 0 else
                                           2 if x["conv"] < 5 else 3)):
        pn = parse_camp_name(c["name"])
        if c["conv"] == 0 and c["cost_czk"] > 300:
            act, reason = "Pause", "0 conversions, significant spend"
        elif c["conv"] == 0:
            act, reason = "Review", "0 conversions, review targeting"
        elif c["conv"] < 5 and c["cost_czk"] > 500:
            act, reason = "Rebuild", f"Low conv rate at {c['cpa_eur']} CPA"
        else:
            act, reason = "Scale", f"{int(c['conv'])} conv at {c['cpa_eur']} CPA"
        camp_act_rows.append([c["name"], pn["vertical"], pn["label"], act, reason, c["cost_eur"], int(c["conv"])])
    camp_act_csv_js = to_csv_js(camp_act_rows)

    neg_camp_blocks = ""
    for ni, (camp_name, data) in enumerate(sorted(neg_by_camp.items(), key=lambda x: -len(x[1]["terms"]))):
        pn = parse_camp_name(camp_name)
        prod_badge = f'<span class="badge {pn["badge"]}" style="margin-right:4px;font-size:10px">{pn["product"]}</span>' if pn["product"] else ""
        feature = pn["label"] or camp_name
        terms_text = "\n".join(f"[{t}]" for t in sorted(data["terms"]))
        intent_pills = ""
        comp_count = sum(1 for t, i in data["intent"].items() if i == "COMPETITOR")
        irr_count  = sum(1 for t, i in data["intent"].items() if i == "IRRELEVANT")
        if comp_count: intent_pills += f'<span class="badge badge-purple" style="font-size:10px;margin-right:4px">{comp_count} competitor</span>'
        if irr_count:  intent_pills += f'<span class="badge badge-red" style="font-size:10px">{irr_count} irrelevant</span>'
        neg_camp_blocks += f"""<div class="pb-neg-block">
          <div class="pb-neg-header">
            <span>{prod_badge}<strong>{feature}</strong> &nbsp;{intent_pills}</span>
            <button class="copy-btn" onclick="copyEl('neg-{ni}', this)">📋 Copy {len(data["terms"])} terms</button>
          </div>
          <textarea id="neg-{ni}" class="neg-textarea" readonly>{terms_text}</textarea>
        </div>"""

    # Campaign action rows
    action_rows = ""
    sorted_camps = sorted(camps, key=lambda x: (0 if x["conv"] == 0 and x["cost_czk"] > 300 else
                                                  1 if x["conv"] == 0 else
                                                  2 if x["conv"] < 5 else 3))
    for ai, c in enumerate(sorted_camps):
        pn = parse_camp_name(c["name"])
        prod_badge = f'<span class="badge {pn["badge"]}" style="margin-right:4px;font-size:10px">{pn["product"]}</span>' if pn["product"] else ""
        feature = pn["label"] or c["name"]
        if c["conv"] == 0 and c["cost_czk"] > 300:
            act, act_cls = "Pause", "badge-red"
            reason = f"0 conversions on {c['cost_eur']} spend — stop budget bleed"
        elif c["conv"] == 0:
            act, act_cls = "Review", "badge-yellow"
            reason = f"0 conversions on {c['cost_eur']} — audit targeting & landing page"
        elif c["conv"] < 5 and c["cost_czk"] > 500:
            act, act_cls = "Rebuild", "badge-yellow"
            reason = f"Only {int(c['conv'])} conv at {c['cpa_eur']} CPA — restructure ad groups"
        else:
            act, act_cls = "Scale", "badge-green"
            reason = f"{int(c['conv'])} conversions at {c['cpa_eur']} CPA — increase daily budget"
        action_rows += f"""<tr id="pb-row-{ai}" class="pb-action-row">
          <td style="width:36px;text-align:center">
            <input type="checkbox" class="done-cb" data-id="pb-act-{ai}" onchange="toggleDone(this)" style="width:16px;height:16px;cursor:pointer">
          </td>
          <td>{prod_badge}<strong>{feature}</strong>
            <div style="font-size:10px;color:var(--muted);margin-top:2px">{pn["vertical"]}</div></td>
          <td><span class="badge {act_cls}">{act}</span></td>
          <td style="color:var(--muted);font-size:12px">{reason}</td>
          <td class="num" style="white-space:nowrap">{c['cost_eur']}</td>
          <td class="num">{int(c['conv'])}</td>
        </tr>"""

    # Structural fix checklist
    struct_items = [
        ("fix-gclid",  "Fix GCLID 90-day cookie",
         "Store gclid URL param in a 90-day cookie on first landing. Read cookie value on all pageviews to recover cross-session attribution. Without this, demos & leads on return visits show 0 paid attribution.",
         "badge-red", "Critical"),
        ("fix-utm",    "Fix UTM ValueTrack: {campaign} → {campaignname}",
         "All campaign Final URLs use the invalid {campaign} parameter — replace with {campaignname}. Current state: all Google Ads sessions land with utm_campaign=(none) in PostHog.",
         "badge-red", "Critical"),
        ("fix-conv",   "Add demo_meeting_booked as Google Ads conversion action",
         "Import the PostHog demo_meeting_booked event (via GCLID matching) as a conversion action. Currently Google Ads optimises on micro-conversions only, not actual demos.",
         "badge-yellow", "High"),
        ("fix-tcpa",   "Switch converting ad groups to Target CPA bidding",
         "Ad groups with 5+ conversions in 90 days are eligible for Smart Bidding. Start with tCPA = current average × 1.2 to give the algorithm room to learn.",
         "badge-yellow", "High"),
        ("fix-neglist","Create shared negative keyword list for all campaigns",
         "Apply the negatives from the P1 list above as a shared list across all campaigns. This prevents the same irrelevant terms from appearing again after Google expands match types.",
         "badge-yellow", "High"),
        ("fix-rlsa",   "Add RLSA audiences to all Search campaigns",
         "Layer in website visitors (30-day) and demo-form visitors (90-day) as Observation audiences. Use bid adjustments: +30% for demo visitors, +15% for general website visitors.",
         "badge-blue", "Medium"),
        ("fix-assets", "Audit ad assets: add sitelinks, callouts, structured snippets",
         "All campaigns are missing key ad extensions. Add at minimum: 4 sitelinks per campaign (Pricing, Demo, Case Studies, [Use Case]), 4 callouts, structured snippets for product features.",
         "badge-blue", "Medium"),
    ]

    struct_rows = ""
    for si, (sid, title, desc, pri_cls, pri_label) in enumerate(struct_items):
        struct_rows += f"""<div id="pb-struct-{si}" class="pb-struct-item">
          <input type="checkbox" class="done-cb" data-id="pb-struct-{si}" onchange="toggleDone(this)" style="width:16px;height:16px;cursor:pointer;flex-shrink:0;margin-top:2px">
          <div style="flex:1">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
              <strong style="font-size:13px">{title}</strong>
              <span class="badge {pri_cls}" style="font-size:10px">{pri_label}</span>
            </div>
            <p style="color:var(--muted);font-size:12px;line-height:1.6">{desc}</p>
          </div>
        </div>"""

    # ── Shared negative list delta ────────────────────────────────────────────
    if shared_neg_list is None:  shared_neg_list  = []
    if user_lists      is None:  user_lists       = []
    if rlsa_assignments is None: rlsa_assignments = []
    if assets          is None:  assets           = {"sitelinks": [], "callouts": [], "snippets": [], "assignments": {}}

    shared_neg_texts      = {t["text"].lower() for t in shared_neg_list}
    shared_neg_delta      = [t for t in all_neg_terms if t.lower() not in shared_neg_texts]
    shared_in_list        = {t for t in all_neg_terms if t.lower() in shared_neg_texts}
    shared_neg_delta_count = len(shared_neg_delta)
    shared_in_list_count   = len(shared_in_list)

    shared_neg_delta_rows = [["Shared Set Name", "Keyword", "Match Type", "Action"]]
    for _t in shared_neg_delta:
        shared_neg_delta_rows.append(["Competitors + Irrelevant", _t, "Broad", "Add"])
    shared_neg_delta_csv_js  = to_csv_js(shared_neg_delta_rows)
    shared_neg_delta_text    = "\n".join(shared_neg_delta)

    shared_term_pills = ""
    for _t in sorted(all_neg_terms):
        if _t in shared_in_list:
            shared_term_pills += f'<span class="badge badge-green" style="font-size:10px;margin-right:4px;margin-bottom:4px">✓ {_t}</span>'
        else:
            shared_term_pills += f'<span class="badge badge-yellow" style="font-size:10px;margin-right:4px;margin-bottom:4px">+ {_t}</span>'

    shared_delta_btn = ""
    if shared_neg_delta:
        shared_delta_btn = (f'<button class="copy-btn" style="margin-bottom:10px;background:linear-gradient(135deg,#f59e0b,#d97706)" '
                            f"""onclick="downloadCSV('shared_neg_delta.csv', SHARED_NEG_DELTA)">"""
                            f'⬇ Export {shared_neg_delta_count} new terms CSV</button>')

    shared_delta_textarea = ""
    if shared_neg_delta:
        shared_delta_textarea = f'<textarea class="neg-textarea" style="min-height:80px;margin-top:8px" readonly>{shared_neg_delta_text}</textarea>'

    # ── RLSA tab HTML ─────────────────────────────────────────────────────────
    user_list_map  = {ul["id"]: ul for ul in user_lists}
    assigned_rlsa  = {}
    for _a in rlsa_assignments:
        _key = (_a["campaign"], _a["adgroup"])
        assigned_rlsa.setdefault(_key, []).append(_a["list_res"])

    camp_adgroups = {}
    for _k in keywords:
        camp_adgroups.setdefault(_k["campaign"], set()).add(_k["adgroup"])

    rlsa_csv_rows = [["Campaign", "Ad Group", "Audience", "Bid Adjustment", "Target Method"]]
    rlsa_tab_html = ""
    camp_order = sorted(camps, key=lambda c: (parse_camp_name(c["name"])["product"], c["name"]))

    for _c in camp_order:
        _pn  = parse_camp_name(_c["name"])
        _tg  = CAMP_TARGET_GROUP.get((_pn["product"], _pn["vertical"]), "General")
        _lp  = LANDING_PAGES.get(_c["name"], "https://boost.space/")
        _lps = _lp.replace("https://boost.space", "") or "/"
        _ags = sorted(camp_adgroups.get(_c["name"], {"(default)"}))
        _pb  = f'<span class="badge {_pn["badge"]}" style="font-size:11px">{_pn["product"]}</span>'
        _cur = sum(1 for ag in _ags if (_c["name"], ag) in assigned_rlsa)
        _rlsa_status = (f'<span class="badge badge-red" style="font-size:11px">❌ None configured</span>'
                        if _cur == 0 else
                        f'<span class="badge badge-green" style="font-size:11px">✓ {_cur} active</span>')

        _rec_rows = ""
        for _ag in _ags:
            for _rec in RLSA_RECS:
                _ul  = user_list_map.get(_rec["id"], {})
                _sz  = _ul.get("search_size", 0)
                _szs = f"{_sz/1000:.0f}K" if _sz >= 1000 else (str(_sz) if _sz else "—")
                _adj = _rec["adj"]
                _adjs = f"+{_adj}%" if _adj > 0 else f"{_adj}%"
                _adjc = "badge-green" if _adj > 0 else "badge-red"
                _rec_rows += f"""<tr>
                  <td style="font-size:11px;color:var(--muted)">{_ag}</td>
                  <td><strong style="font-size:12px">{_rec['name']}</strong><div style="font-size:10px;color:var(--muted)">{_rec['note']}</div></td>
                  <td class="num" style="font-size:12px">{_szs}</td>
                  <td><span class="badge {_adjc}" style="font-size:11px">{_adjs}</span></td>
                  <td><span class="badge badge-blue" style="font-size:10px">{_rec['action'].title()}</span></td>
                </tr>"""
                rlsa_csv_rows.append([_c["name"], _ag, _rec["name"], _adjs, "Observation"])

        rlsa_tab_html += f"""
        <div class="pb-section" style="margin-bottom:20px">
          <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;flex-wrap:wrap">
            {_pb}
            <strong style="font-size:14px">{_pn['label']}</strong>
            {_rlsa_status}
            <span style="margin-left:auto;color:var(--muted);font-size:11px">Target: <strong style="color:var(--text)">{_tg}</strong></span>
          </div>
          <div style="background:var(--bg3);border-radius:8px;padding:8px 14px;margin-bottom:12px;font-size:12px">
            Landing page: <a href="{_lp}" target="_blank" style="color:var(--accent)">{_lps}</a>
          </div>
          <div class="table-wrap" style="margin:0">
            <table>
              <thead><tr><th>Ad Group</th><th>Audience</th><th class="num">Search Size</th><th class="num">Bid Adj</th><th>Method</th></tr></thead>
              <tbody>{_rec_rows}</tbody>
            </table>
          </div>
        </div>"""

    rlsa_csv_js    = to_csv_js(rlsa_csv_rows)
    rlsa_row_count = len(rlsa_csv_rows) - 1

    # ── Extensions tab HTML ────────────────────────────────────────────────────
    camp_ext_asn    = assets.get("assignments", {})
    existing_slinks = assets.get("sitelinks", [])

    coverage_rows_html = ""
    for _c in camp_order:
        _pn  = parse_camp_name(_c["name"])
        _pb  = f'<span class="badge {_pn["badge"]}" style="font-size:10px;margin-right:4px">{_pn["product"]}</span>'
        _asn = camp_ext_asn.get(_c["name"], [])
        _cells = ""
        for _et in ("SITELINK", "CALLOUT", "STRUCTURED_SNIPPET"):
            _ok = _et in _asn
            _cells += f'<td style="text-align:center;font-size:16px">{"✅" if _ok else "❌"}</td>'
        coverage_rows_html += f"<tr><td>{_pb}<strong>{_pn['label']}</strong></td>{_cells}</tr>\n"

    stale_sl_html = ""
    for _sl in existing_slinks[:20]:
        _txt = _sl.get("link_text", "") or ""
        _d1  = _sl.get("desc1", "")    or ""
        _d2  = _sl.get("desc2", "")    or ""
        _stale = any(ord(ch) > 127 for ch in (_txt + _d1 + _d2))
        _nodescs = not (_d1 and _d2)
        _tags = ""
        if _stale:   _tags += '<span class="badge badge-red" style="font-size:10px;margin-right:4px">Non-EN</span>'
        if _nodescs: _tags += '<span class="badge badge-yellow" style="font-size:10px">No descriptions</span>'
        _status = _tags or '<span class="badge badge-green" style="font-size:10px">OK</span>'
        stale_sl_html += (f"<tr><td><strong>{_txt}</strong></td>"
                          f"<td style='font-size:11px;color:var(--muted)'>{_d1 or '—'}</td>"
                          f"<td style='font-size:11px;color:var(--muted)'>{_d2 or '—'}</td>"
                          f"<td>{_status}</td></tr>\n")

    sl_csv_rows = [["Campaign", "Sitelink Text", "Description Line 1", "Description Line 2", "Final URL"]]
    ca_csv_rows = [["Campaign", "Callout Text"]]
    sn_csv_rows = [["Campaign", "Structured Snippet Header", "Structured Snippet Values"]]
    ext_camp_html = ""

    for _c in camp_order:
        _pn   = parse_camp_name(_c["name"])
        _pb   = f'<span class="badge {_pn["badge"]}" style="font-size:11px">{_pn["product"]}</span>'
        _sls  = CAMP_SITELINKS.get(_c["name"], [])
        _snip = CAMP_SNIPPETS.get(_c["name"])

        _sl_rows = ""
        for _sl_text, _sl_d1, _sl_d2, _sl_url in _sls:
            _sl_short = _sl_url.replace("https://boost.space", "")
            _sl_rows += (f"<tr><td><strong>{_sl_text}</strong></td>"
                         f"<td style='font-size:11px;color:var(--muted)'>{_sl_d1}</td>"
                         f"<td style='font-size:11px;color:var(--muted)'>{_sl_d2}</td>"
                         f"<td style='font-size:11px'><a href='{_sl_url}' target='_blank' style='color:var(--accent)'>{_sl_short}</a></td></tr>\n")
            sl_csv_rows.append([_c["name"], _sl_text, _sl_d1, _sl_d2, _sl_url])

        for _ca in UNIVERSAL_CALLOUTS:
            ca_csv_rows.append([_c["name"], _ca])
        _ca_pills = " ".join(f'<span class="badge badge-blue" style="font-size:10px">{_ca}</span>' for _ca in UNIVERSAL_CALLOUTS)

        _snip_html = ""
        if _snip:
            _sh, _sv = _snip
            _v_pills = " ".join(f'<span class="badge badge-gray" style="font-size:10px">{_v}</span>' for _v in _sv.split(";"))
            _snip_html = f'<div style="margin-top:8px"><span style="font-size:11px;color:var(--muted);margin-right:8px">📋 {_sh}:</span>{_v_pills}</div>'
            sn_csv_rows.append([_c["name"], _sh, _sv.replace(";", ",")])

        _sl_section = ""
        if _sl_rows:
            _sl_section = (f'<div style="margin-bottom:12px">'
                           f'<div style="font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.7px;margin-bottom:8px">🔗 Sitelinks</div>'
                           f'<div class="table-wrap" style="margin:0"><table><thead><tr><th>Link Text</th><th>Desc 1</th><th>Desc 2</th><th>URL</th></tr></thead>'
                           f'<tbody>{_sl_rows}</tbody></table></div></div>')

        ext_camp_html += f"""
        <div class="pb-section" style="margin-bottom:16px">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px">{_pb}<strong style="font-size:13px">{_pn['label']}</strong></div>
          {_sl_section}
          <div style="margin-bottom:12px">
            <div style="font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.7px;margin-bottom:8px">📢 Callouts</div>
            <div style="display:flex;flex-wrap:wrap;gap:6px">{_ca_pills}</div>
          </div>
          {_snip_html}
        </div>"""

    sl_csv_js = to_csv_js(sl_csv_rows)
    ca_csv_js = to_csv_js(ca_csv_rows)
    sn_csv_js = to_csv_js(sn_csv_rows)
    sl_row_count = len(sl_csv_rows) - 1
    ca_row_count = len(ca_csv_rows) - 1
    sn_row_count = len(sn_csv_rows) - 1

    stale_sl_section = ""
    if stale_sl_html:
        stale_sl_section = (f'<div class="table-wrap" style="margin-bottom:24px">'
                            f'<div class="table-header"><h3>Existing Sitelinks Audit</h3>'
                            f'<span style="color:var(--muted);font-size:12px">{len(existing_slinks)} in account</span></div>'
                            f'<table><thead><tr><th>Link Text</th><th>Desc 1</th><th>Desc 2</th><th>Status</th></tr></thead>'
                            f'<tbody>{stale_sl_html}</tbody></table></div>')

    # ── Build final HTML ──────────────────────────────────────────────────────
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
    if(uh===UH&&ph===PH){{sessionStorage.setItem('_auth','1');document.getElementById('_gate').style.display='none';document.getElementById('_app').style.display='block';restoreDoneStates();}}
    else{{const err=document.getElementById('_err');err.textContent='Invalid credentials.';err.style.display='block';document.getElementById('_p').value='';}}
  }}
  window.addEventListener('DOMContentLoaded',function(){{
    if(sessionStorage.getItem('_auth')==='1'){{document.getElementById('_gate').style.display='none';document.getElementById('_app').style.display='block';restoreDoneStates();}}
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
      headers:{{'Authorization':'Bearer '+atob('{{__GHTOKEN__}}'),'Accept':'application/vnd.github+json','Content-Type':'application/json'}},
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

function copyEl(id, btn){{
  const el=document.getElementById(id);
  navigator.clipboard.writeText(el.value).then(()=>{{
    const orig=btn.textContent;
    btn.textContent='✅ Copied!';
    btn.style.background='linear-gradient(135deg,#10b981,#34d399)';
    setTimeout(()=>{{btn.textContent=orig;btn.style.background='';}},2000);
  }}).catch(()=>{{el.select();document.execCommand('copy');}});
}}

function toggleDone(cb){{
  const id=cb.dataset.id;
  const row=document.getElementById(id)||cb.closest('tr,div.pb-struct-item');
  if(cb.checked){{
    localStorage.setItem('done-'+id,'1');
    if(row){{row.style.opacity='0.4';}}
  }}else{{
    localStorage.removeItem('done-'+id);
    if(row){{row.style.opacity='1';}}
  }}
}}

function restoreDoneStates(){{
  document.querySelectorAll('.done-cb').forEach(cb=>{{
    const id=cb.dataset.id;
    if(localStorage.getItem('done-'+id)==='1'){{
      cb.checked=true;
      const row=document.getElementById(id)||cb.closest('tr,div.pb-struct-item');
      if(row) row.style.opacity='0.4';
    }}
  }});
}}

/* ── Google Ads CSV download data ───────────────────────────────────────── */
var NEG_CAMP_CSV      = {neg_camp_csv_js};
var NEG_ACCT_CSV      = {neg_acct_csv_js};
var ADD_KW_CSV        = {add_kw_csv_js};
var CAMP_ACT_CSV      = {camp_act_csv_js};
var RLSA_CSV          = {rlsa_csv_js};
var SITELINKS_CSV     = {sl_csv_js};
var CALLOUTS_CSV      = {ca_csv_js};
var SNIPPETS_CSV      = {sn_csv_js};
var SHARED_NEG_DELTA  = {shared_neg_delta_csv_js};

function downloadCSV(filename, data){{
  const BOM='\uFEFF';
  const blob=new Blob([BOM+data],{{type:'text/csv;charset=utf-8;'}});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url; a.download=filename;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a); URL.revokeObjectURL(url);
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
  /* Playbook styles */
  .pb-section{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:24px;}}
  .pb-section-header{{display:flex;align-items:flex-start;gap:16px;margin-bottom:20px;}}
  .pb-priority{{width:36px;height:36px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:800;flex-shrink:0;}}
  .pb-p1{{background:rgba(239,68,68,0.2);color:#f87171;border:1px solid rgba(239,68,68,0.3);}}
  .pb-p2{{background:rgba(245,158,11,0.2);color:#fbbf24;border:1px solid rgba(245,158,11,0.3);}}
  .pb-p3{{background:rgba(59,130,246,0.2);color:#60a5fa;border:1px solid rgba(59,130,246,0.3);}}
  .pb-section-header h3{{font-size:15px;font-weight:700;margin-bottom:3px;}}
  .pb-section-header p{{font-size:12px;color:var(--muted);}}
  .pb-neg-block{{background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:14px 16px;margin-bottom:10px;}}
  .pb-neg-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;}}
  .neg-textarea{{width:100%;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px 12px;color:#a78bfa;font-family:monospace;font-size:12px;line-height:1.7;resize:vertical;min-height:80px;max-height:200px;}}
  .copy-btn{{background:linear-gradient(135deg,#6366f1,#8b5cf6);border:none;border-radius:6px;padding:7px 14px;color:#fff;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap;transition:all .2s;}}
  .copy-btn:hover{{opacity:0.9;}}
  .neg-master-block{{background:linear-gradient(135deg,rgba(99,102,241,0.08),rgba(139,92,246,0.05));border:1px solid rgba(99,102,241,0.3);border-radius:8px;padding:16px;margin-top:16px;}}
  .pb-action-row td{{vertical-align:middle;}}
  .pb-struct-item{{display:flex;gap:14px;align-items:flex-start;padding:14px 0;border-bottom:1px solid var(--border);}}
  .pb-struct-item:last-child{{border-bottom:none;padding-bottom:0;}}
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
  <div class="tab active" onclick="showTab('dashboard',this)">📊 Dashboard</div>
  <div class="tab" onclick="showTab('campaigns',this)">🎯 Campaigns</div>
  <div class="tab" onclick="showTab('searchterms',this)">🔍 Search Terms</div>
  <div class="tab" onclick="showTab('funnel',this)">🏷️ Keyword Funnel</div>
  <div class="tab" onclick="showTab('posthog',this)">📈 PostHog</div>
  <div class="tab" onclick="showTab('budget',this)">💰 Budget Model</div>
  <div class="tab" onclick="showTab('playbook',this)" style="color:#f87171;font-weight:700">⚡ Playbook</div>
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
      <thead><tr><th>Campaign / Use Case</th><th>Type</th><th class="num">Impr.</th><th class="num">Clicks</th><th class="num">CTR</th><th class="num">Cost</th><th class="num">CPC</th><th class="num">Conv.</th><th class="num">CPA</th><th class="num">Imp Share</th><th class="num">Rank Lost</th><th>Verdict</th></tr></thead>
      <tbody>{camp_rows_html}</tbody>
    </table>
  </div>
</div>

<!-- SEARCH TERMS -->
<div id="tab-searchterms" class="tab-content">
  <div class="section-title">Search Terms — Top Spend → Low Spend</div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Search Term</th><th>Campaign</th><th>Ad Group</th><th class="num">Cost</th><th class="num">Clicks</th><th class="num">CPC</th><th class="num">CTR</th><th class="num">Conv.</th><th>Intent</th><th>Action</th></tr></thead>
      <tbody>{term_rows}</tbody>
    </table>
  </div>
</div>

<!-- KEYWORD FUNNEL -->
<div id="tab-funnel" class="tab-content">
  <div class="section-title">Keyword Funnel — by Product · Use Case · Ad Group</div>
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

<!-- PLAYBOOK -->
<div id="tab-playbook" class="tab-content">
  <div class="section-title">⚡ Execution Playbook</div>
  <p style="color:var(--muted);font-size:13px;margin-bottom:24px">Prioritised actions with copy-paste execution. Check off items as you complete them — progress is saved in your browser.</p>

  <!-- P1: Negative Keywords -->
  <div class="pb-section">
    <div class="pb-section-header">
      <div class="pb-priority pb-p1">P1</div>
      <div style="flex:1">
        <h3>Exclude Negative Keywords</h3>
        <p>Competitor &amp; irrelevant search terms wasting budget. Export CSV → Google Ads → Tools → Bulk Actions → Upload.</p>
      </div>
      <div style="display:flex;flex-direction:column;gap:6px;align-self:flex-start;min-width:200px">
        <button class="copy-btn" style="background:linear-gradient(135deg,#10b981,#059669)" onclick="downloadCSV('negatives_campaign_level.csv', NEG_CAMP_CSV)">⬇ Campaign-Level CSV ({len(neg_camp_csv_rows)-1} rows)</button>
        <button class="copy-btn" style="background:linear-gradient(135deg,#6366f1,#4f46e5)" onclick="downloadCSV('negatives_account_level.csv', NEG_ACCT_CSV)">⬇ Account-Level CSV ({len(all_neg_terms)} rows)</button>
        <div style="font-size:10px;color:var(--muted);text-align:center;line-height:1.4">Campaign-level = per campaign<br>Account-level = blocks all campaigns</div>
      </div>
    </div>
    <div style="background:var(--bg3);border-radius:8px;padding:10px 14px;margin-bottom:16px;font-size:11px;color:var(--muted);font-family:monospace;line-height:1.8">
      <span style="color:var(--text);font-weight:600">CSV format:</span> &nbsp;
      Campaign &nbsp;|&nbsp; Ad Group &nbsp;|&nbsp; Account keyword type &nbsp;|&nbsp; Keyword &nbsp;|&nbsp; Criterion Type
    </div>
    {neg_camp_blocks}
    <div class="neg-master-block">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
        <strong style="font-size:13px">Copy list (text)</strong>
        <span style="color:var(--muted);font-size:12px">{len(all_neg_terms)} unique terms · exact match</span>
      </div>
      <textarea id="neg-master" class="neg-textarea" style="min-height:140px" readonly>{all_neg_text}</textarea>
    </div>
  </div>

  <!-- P2: Campaign Actions -->
  <div class="pb-section">
    <div class="pb-section-header">
      <div class="pb-priority pb-p2">P2</div>
      <div style="flex:1">
        <h3>Campaign Actions</h3>
        <p>Sorted by urgency — pause zero-conversion campaigns first to stop budget bleed.</p>
      </div>
      <button class="copy-btn" style="background:linear-gradient(135deg,#f59e0b,#d97706);align-self:flex-start" onclick="downloadCSV('campaign_actions.csv', CAMP_ACT_CSV)">⬇ Export CSV</button>
    </div>
    <div class="table-wrap" style="margin:0">
      <table>
        <thead><tr><th style="width:36px"></th><th>Campaign / Use Case</th><th>Action</th><th>Reason</th><th class="num">Spend</th><th class="num">Conv.</th></tr></thead>
        <tbody>{action_rows}</tbody>
      </table>
    </div>
  </div>

  <!-- P3: Structural Fixes -->
  <div class="pb-section">
    <div class="pb-section-header">
      <div class="pb-priority pb-p3">P3</div>
      <div>
        <h3>Structural &amp; Tracking Fixes</h3>
        <p>One-time fixes that unlock compounding improvements — start with Critical items.</p>
      </div>
    </div>
    {struct_rows}
  </div>

  <!-- P4: Add Targeted Keywords (converting search terms not yet as keywords) -->
  {'<div class="pb-section">' if has_add_kws else '<!-- no new keyword suggestions -->'}
  {f"""
  <div class="pb-section-header">
    <div class="pb-priority" style="background:rgba(99,102,241,0.2);color:#a78bfa;border:1px solid rgba(99,102,241,0.3);font-size:14px;font-weight:800;width:36px;height:36px;border-radius:8px;display:flex;align-items:center;justify-content:center;flex-shrink:0">P4</div>
    <div style="flex:1">
      <h3>Add Targeted Keywords</h3>
      <p>Converting search terms ({add_kw_count}) not yet in your keyword lists — add as Exact or Phrase match to capture them explicitly.</p>
    </div>
    <div style="display:flex;flex-direction:column;gap:6px;align-self:flex-start;min-width:200px">
      <button class="copy-btn" style="background:linear-gradient(135deg,#a78bfa,#6366f1)" onclick="downloadCSV('add_keywords.csv', ADD_KW_CSV)">⬇ Keywords CSV ({add_kw_count} rows)</button>
      <div style="font-size:10px;color:var(--muted);text-align:center;line-height:1.4">Exact = 2+ conversions<br>Phrase = 1 conversion</div>
    </div>
  </div>
  <div style="background:var(--bg3);border-radius:8px;padding:10px 14px;margin-bottom:16px;font-size:11px;color:var(--muted);font-family:monospace;line-height:1.8">
    <span style="color:var(--text);font-weight:600">CSV format:</span> &nbsp;
    Campaign &nbsp;|&nbsp; Ad Group &nbsp;|&nbsp; Keyword &nbsp;|&nbsp; Criterion Type
  </div>
  """ if has_add_kws else ""}
  {'</div>' if has_add_kws else ''}

</div>

<script>
function showTab(id, el){{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));
  document.getElementById('tab-'+id).classList.add('active');
  if(el) el.classList.add('active');
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

    generated_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    gh_token_b64 = os.environ.get("GH_TOKEN_B64", "")
    html = build_html(camps, terms, keywords, ph, score, cats, generated_at)
    html = html.replace("{__GHTOKEN__}", gh_token_b64)

    out = os.path.join(os.path.dirname(__file__), "..", "index.html")
    with open(out, "w") as f:
        f.write(html)
    print(f"Written → {out}")
