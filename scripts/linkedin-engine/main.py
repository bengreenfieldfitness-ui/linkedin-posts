"""
LinkedIn Content Engine
=======================
Discovers viral LinkedIn posts in your niche, rewrites them in your brand voice
using Claude, and publishes to LinkedIn automatically. Runs on GitHub Actions.

Usage:
    python main.py                  # Normal run (discover + rewrite + publish)
    python main.py --dry-run        # Preview without publishing
    python main.py --discover-only  # Only discover, print results
    python main.py --publish-queue  # Skip discovery, publish next queued post
    python main.py --backfill 14    # Look back N days for content (default: 7)
    python main.py --collect-analytics  # Collect engagement stats for published posts
    python main.py --show-learnings     # View performance report with insights
    python main.py --backfill-published # Import published posts from archive files
    python main.py --skip-check     # Force run, bypass schedule variance
"""

import argparse
import hashlib
import json
import logging
import math
import os
import random
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("linkedin-engine")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.json"
BRAND_PATH = SCRIPT_DIR / "brand.json"
STATE_PATH = SCRIPT_DIR / "state.json"
ARCHIVE_DIR = SCRIPT_DIR / "outputs"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_brand() -> dict:
    if not BRAND_PATH.exists():
        log.error("brand.json not found at %s — run the installer first", BRAND_PATH)
        sys.exit(1)
    with open(BRAND_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_state() -> dict:
    if STATE_PATH.exists():
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed_ids": [], "content_queue": [], "published_posts": [], "last_run": None, "cta_counter": 0, "client_last_used": {}}


def save_state(state: dict) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Slack Notifications (optional)
# ---------------------------------------------------------------------------

def slack_send(text: str) -> bool:
    """Send a Slack message. Returns True on success. Silently skips if not configured."""
    token = os.getenv("SLACK_BOT_TOKEN")
    channel = os.getenv("SLACK_CHANNEL_ID")
    if not token or not channel:
        return False

    import urllib.request
    import urllib.error

    payload = json.dumps({"channel": channel, "text": text, "unfurl_links": False}).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("ok", False)
    except Exception as e:
        log.warning("Slack notification failed (non-fatal): %s", e)
        return False


def slack_failure(system: str, error: str) -> bool:
    return slack_send(f":x: *{system}* — Failed\n```{error}```")


# ---------------------------------------------------------------------------
# Airtable Testimonials
# ---------------------------------------------------------------------------

def fetch_airtable_testimonials(config: dict) -> list[str]:
    """Fetch client testimonials from Airtable. Returns list of client_wins strings.
    Silently skips if AIRTABLE_API_KEY is not set or fetch fails."""
    api_key = os.getenv("AIRTABLE_API_KEY")
    if not api_key:
        log.info("AIRTABLE_API_KEY not set, skipping testimonial fetch")
        return []

    at = config.get("airtable", {})
    base_id = at.get("base_id", "")
    table_id = at.get("table_id", "")
    name_field = at.get("name_field", "")
    quote_field = at.get("quote_field", "")

    if not all([base_id, table_id, name_field, quote_field]):
        log.warning("Airtable config incomplete, skipping testimonial fetch")
        return []

    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    wins = []
    offset = None

    try:
        while True:
            params = {"fields[]": [name_field, quote_field], "pageSize": 100}
            if offset:
                params["offset"] = offset
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for record in data.get("records", []):
                fields = record.get("fields", {})
                name = (fields.get(name_field) or "").strip()
                quote = (fields.get(quote_field) or "").strip()
                if name and quote:
                    wins.append(f"{name}: '{quote}'")

            offset = data.get("offset")
            if not offset:
                break

        log.info("Fetched %d testimonials from Airtable", len(wins))
        return wins
    except Exception as e:
        log.warning("Airtable testimonial fetch failed (non-fatal): %s", e)
        return []


# ---------------------------------------------------------------------------
# Content Discovery via Apify
# ---------------------------------------------------------------------------

def select_queries(config: dict, brand: dict) -> list[str]:
    """Rotate through search queries using date-based hash for deterministic
    daily rotation."""
    queries = brand.get("discovery_queries", [])
    if not queries:
        log.error("No discovery_queries defined in brand.json")
        return []
    per_run = config["discovery"]["queries_per_run"]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
    seed = int(hashlib.md5(date_str.encode()).hexdigest(), 16)
    rng = random.Random(seed)
    selected = rng.sample(queries, min(per_run, len(queries)))
    log.info("Selected queries for this run: %s", selected)
    return selected


def discover_viral_posts(config: dict, brand: dict, state: dict, backfill_days: int = 7) -> list[dict]:
    """Discover viral LinkedIn posts using Apify LinkedIn Posts Scraper."""
    apify_key = os.getenv("APIFY_API_KEY") or os.getenv("APIFY_API_TOKEN")
    if not apify_key:
        log.error("APIFY_API_KEY / APIFY_API_TOKEN not set, skipping discovery")
        return []

    disc = config["discovery"]
    queries = select_queries(config, brand)
    processed = set(state.get("processed_ids", []))
    all_posts = []

    for query in queries:
        log.info("Searching LinkedIn for: '%s'", query)
        try:
            posts = _apify_search(apify_key, query, disc, backfill_days)
            log.info("  Found %d raw posts", len(posts))

            for post in posts:
                post_id = post.get("url") or post.get("urn") or post.get("id", "")
                if not post_id or post_id in processed:
                    continue

                reactions = post.get("reactionCount", 0) or post.get("numLikes", 0) or 0
                comments = post.get("commentCount", 0) or post.get("numComments", 0) or 0

                if reactions < disc["min_reactions"]:
                    continue
                if comments < disc["min_comments"]:
                    continue

                text = post.get("text", "") or post.get("commentary", "")
                if not text or len(text) < 50:
                    continue

                author = post.get("authorName", "") or post.get("author", {}).get("name", "Unknown")
                reposts = post.get("repostCount", 0) or post.get("numShares", 0) or 0
                published = post.get("postedAt", "") or post.get("publishedAt", "")

                scored_post = {
                    "id": post_id,
                    "text": text,
                    "author": author,
                    "reactions": reactions,
                    "comments": comments,
                    "reposts": reposts,
                    "published": published,
                    "url": post.get("url", ""),
                    "query": query,
                }

                scored_post["viral_score"] = _calculate_viral_score(scored_post, disc)
                all_posts.append(scored_post)

        except Exception as e:
            log.warning("Error searching for '%s': %s", query, e)
            continue

    # Deduplicate by post ID
    seen = set()
    unique = []
    for p in all_posts:
        if p["id"] not in seen:
            seen.add(p["id"])
            unique.append(p)

    # Sort by viral score, return top posts
    unique.sort(key=lambda x: x["viral_score"], reverse=True)
    max_posts = disc.get("max_posts_per_query", 10)
    result = unique[:max_posts]
    log.info("Discovery complete: %d unique viral posts found (top %d returned)", len(unique), len(result))
    return result


def _apify_search(api_key: str, query: str, disc: dict, backfill_days: int) -> list[dict]:
    """Call Apify harvestapi LinkedIn Post Search actor and normalize results."""
    url = "https://api.apify.com/v2/acts/harvestapi~linkedin-post-search/run-sync-get-dataset-items"

    params = {"token": api_key}
    body = {
        "searchQueries": [query],
        "limit": disc.get("max_posts_per_query", 10),
    }

    resp = requests.post(url, params=params, json=body, timeout=180)
    resp.raise_for_status()
    raw = resp.json()

    # Normalize harvestapi format
    normalized = []
    for item in raw:
        engagement = item.get("engagement", {})
        author_data = item.get("author", {})
        posted_at = item.get("postedAt", {})

        normalized.append({
            "url": item.get("linkedinUrl", ""),
            "urn": item.get("id", ""),
            "id": item.get("linkedinUrl", "") or item.get("id", ""),
            "text": item.get("content", ""),
            "authorName": author_data.get("name", "Unknown"),
            "reactionCount": engagement.get("likes", 0),
            "commentCount": engagement.get("comments", 0),
            "repostCount": engagement.get("shares", 0),
            "postedAt": posted_at.get("date", "") if isinstance(posted_at, dict) else str(posted_at),
        })

    return normalized


def _calculate_viral_score(post: dict, disc: dict) -> float:
    """Calculate 0-100 viral score using weighted algorithm."""
    weights = disc["scoring"]

    # Reaction velocity: reactions per hour, log-scaled
    hours = _hours_since(post.get("published", ""))
    if hours < 1:
        hours = 1
    velocity_raw = math.log10(max(post["reactions"] / hours, 1)) / 4 * 100
    velocity = min(velocity_raw, 100)

    # Comment rate: comments / reactions
    comment_rate = (post["comments"] / max(post["reactions"], 1)) / 0.05 * 100
    comment_rate = min(comment_rate, 100)

    # Repost rate: reposts / reactions
    repost_rate = (post["reposts"] / max(post["reactions"], 1)) / 0.03 * 100
    repost_rate = min(repost_rate, 100)

    # Recency: exponential decay with 48-hour half-life
    recency = 100 * (0.5 ** (hours / 48))

    score = (
        weights["reaction_velocity"] * velocity
        + weights["comment_rate"] * comment_rate
        + weights["repost_rate"] * repost_rate
        + weights["recency"] * recency
    )
    return round(min(score, 100), 1)


def _hours_since(date_str: str) -> float:
    """Calculate hours since a date string."""
    if not date_str:
        return 168  # Default 7 days if no date
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        try:
            for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%B %d, %Y"]:
                try:
                    dt = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
            else:
                return 168
        except Exception:
            return 168

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    return max(delta.total_seconds() / 3600, 0.1)


# ---------------------------------------------------------------------------
# Content Rewriting via Claude
# ---------------------------------------------------------------------------

def get_client_first_name(win: str) -> str:
    """Extract the first name from a client win entry."""
    # Win format: "FirstName (context): ..." or "FirstName: ..."
    return win.split("(")[0].split(":")[0].strip()


def filter_eligible_wins(brand: dict, state: dict) -> list:
    """Return client wins excluding any client used in the last 10 days."""
    all_wins = brand.get("client_wins", [])
    client_last_used = state.get("client_last_used", {})
    cutoff = datetime.now(timezone.utc) - timedelta(days=10)

    eligible = []
    for win in all_wins:
        name = get_client_first_name(win)
        last_used_str = client_last_used.get(name)
        if last_used_str:
            last_used = datetime.fromisoformat(last_used_str)
            if last_used > cutoff:
                continue
        eligible.append(win)

    # Fall back to all wins if every client was used recently
    return eligible if eligible else all_wins


def record_client_used(post_text: str, brand: dict, state: dict) -> None:
    """Detect which client was mentioned in a post and record the date."""
    all_wins = brand.get("client_wins", [])
    client_last_used = state.setdefault("client_last_used", {})
    now = datetime.now(timezone.utc).isoformat()

    for win in all_wins:
        name = get_client_first_name(win)
        if name and name in post_text:
            client_last_used[name] = now


def build_system_prompt(brand: dict, state: dict = None) -> str:
    """Build the system prompt dynamically from brand.json."""

    # Format client wins — filtered by 10-day rotation rule
    eligible_wins = filter_eligible_wins(brand, state or {})
    wins_section = ""
    if eligible_wins:
        wins_lines = []
        for win in eligible_wins:
            wins_lines.append(f"- {win}")
        wins_section = "\n".join(wins_lines)

    # Format aggregate stats
    stats_section = ""
    if brand.get("aggregate_stats"):
        stats_lines = []
        for stat in brand["aggregate_stats"]:
            stats_lines.append(f"- {stat}")
        stats_section = "\n".join(stats_lines)

    # Format themes
    themes_section = ""
    if brand.get("themes"):
        themes_lines = []
        for theme in brand["themes"]:
            themes_lines.append(f"- {theme}")
        themes_section = "\n".join(themes_lines)

    # Build the prompt
    prompt = f"""You are the content voice of {brand['brand_name']}, {brand['persona']}.

You write LinkedIn posts that go viral. You understand the LinkedIn algorithm deeply:

=== LINKEDIN ALGORITHM RULES (2026) ===

1. DWELL TIME is the #1 ranking signal. Not likes. Not shares. Time spent reading your post.
2. COMMENTS are 15x more valuable than reactions. Every comment extends distribution.
3. The "See More" fold hits at ~210 characters. 60-70% of readers NEVER click it. Your first 210 characters decide everything.
4. First 60 minutes is the "golden hour." Only 5% of underperforming posts recover.
5. External links kill ~60% of reach. NEVER include URLs in the post body.
6. LinkedIn NLP-detects engagement bait ("Comment YES", "Like if you agree"). This gets suppressed.
7. The algorithm now uses interest-graph matching, posts are served beyond your network based on topic authority.

=== POST STRUCTURE FORMULA ===

Use this proven structure for maximum virality:

**LINE 1 (THE HOOK, most important line in the entire post):**
Must be a pattern interrupt that stops the scroll. Use one of these hook types:
- CONTRARIAN TAKE: Challenge conventional wisdom in your space
- BOLD CLAIM: Lead with a specific, credible data point
- VISCERAL FRUSTRATION: Voice the frustration your audience feels
- SHOCKING STAT: Open with a number that makes them stop
- PERSONAL CONFESSION: Share something vulnerable or unexpected
- STORY OPENER: Start mid-story with a specific moment
- COUNTER-NARRATIVE: Flip a popular narrative on its head

**LINES 2-3 (ABOVE THE FOLD, must appear before "See More"):**
Expand the hook with ONE compelling detail that creates an open loop.
The reader must NEED to click "See More" to get the resolution.

**BODY (BELOW THE FOLD):**
- Short paragraphs: 1-2 sentences MAX per paragraph
- Blank line between EVERY paragraph
- Use numbered lists or frameworks when teaching (people save these)
- MANDATORY: Include at least one specific client win or proof point from the PROOF BANK below. Use a real name/detail and real number. This is what separates your posts from generic advice.
- Build toward an insight that reframes how the reader thinks about the topic
- Optimal total length: 1,300-1,900 characters (47% higher engagement than shorter posts)

**CLOSING (CTA, last 2-3 lines):**
CRITICAL: Comments are 15x more valuable than reactions for LinkedIn's algorithm.
80% of posts must end with a COMMENT-PROVOKING question or prompt.
Only 20% of posts should use DM-based lead capture.
The CTA type will be specified in the user prompt. Follow it exactly.
Then add 3-5 hashtags on the final line.

=== PROOF BANK (USE THESE — THEY ARE REAL) ===

Every post MUST include at least one specific client story or data point from this bank.
Do NOT use generic phrases. Use a REAL example.
Rotate through these so no two consecutive posts use the same win.

INDIVIDUAL WINS:
{wins_section}

AGGREGATE STATS (use sparingly, always pair with an individual story):
{stats_section}

HOW TO USE THESE:
- Open with or build toward a specific outcome (not a generic stat)
- Weave the win into the narrative naturally, not as a testimonial dump
- Use first names only. Never fabricate details not listed here.

=== BRAND VOICE ===

{brand['brand_voice']}

=== THEMES TO REINFORCE ===

{themes_section}

=== ANTI-AI GUARDRAILS (CRITICAL) ===

NEVER do any of the following:
- The "X isn't Y. It's Z." mirroring pattern (dead giveaway)
- "Here's the thing:" or "Here's what nobody tells you:" (overused AI opener)
- Overly polished, rhythmic sentence pairs
- Grandiose declarations or motivational-poster language
- "Let me be clear" / "The truth is" / "Here's the reality" as transitions
- Lists of exactly 5 items (vary: 3, 4, 6, 7)
- Starting multiple paragraphs with the same word
- Em dashes (use commas, periods, or "and" instead)
- Emojis in the body text (hashtags only at end)
- Perfect parallel structure across all paragraphs
Write ROUGHER, not smoother. Real LinkedIn posts have texture, not symmetry."""

    return prompt


HOOK_TYPES = [
    "CONTRARIAN TAKE",
    "BOLD CLAIM",
    "VISCERAL FRUSTRATION",
    "SHOCKING STAT",
    "PERSONAL CONFESSION",
    "STORY OPENER",
    "COUNTER-NARRATIVE",
]


def select_cta(brand: dict, state: dict) -> tuple[str, str]:
    """Select CTA type and text based on 80/20 comment/DM ratio."""
    counter = state.get("cta_counter", 0)

    cta_comment_options = brand.get("cta_comment_options", ["What do you think? Drop your take below."])
    cta_dm_options = brand.get("cta_dm_options", [])

    # Every 5th post uses DM CTA (if DM options exist), rest use comment CTA
    if counter % 5 == 4 and cta_dm_options:
        cta_type = "DM"
        options = cta_dm_options
    else:
        cta_type = "COMMENT"
        options = cta_comment_options

    selected = options[counter % len(options)]
    state["cta_counter"] = counter + 1
    return cta_type, selected


def select_hook_type(state: dict) -> str:
    """Rotate through hook types to maintain variety."""
    counter = state.get("cta_counter", 0)
    return HOOK_TYPES[counter % len(HOOK_TYPES)]


def rewrite_post(client, original_post: dict, config: dict, brand: dict, state: dict) -> tuple[str | None, str, str]:
    """Rewrite a viral LinkedIn post in brand voice using Claude.
    Returns (post_text, hook_type, cta_type)."""
    cta_type, cta_text = select_cta(brand, state)
    hook_type = select_hook_type(state)

    if cta_type == "COMMENT":
        cta_instruction = f'End with this comment-provoking question: "{cta_text}"'
    else:
        cta_instruction = f'End with this DM value exchange: "{cta_text}"'

    target_audience = brand.get("target_audience", "professionals")
    topic_lens = brand.get("topic_lens", "your expertise")

    user_prompt = f"""Here is a LinkedIn post that went viral with professionals (viral score: {original_post.get('viral_score', 0)}/100, {original_post.get('reactions', 0)} reactions, {original_post.get('comments', 0)} comments):

---
{original_post['text'][:3000]}
---

Rewrite this as an ORIGINAL LinkedIn post for {brand['brand_name']}.

Rules:
1. Extract the CORE INSIGHT, the reason this resonated, and reframe it through our lens ({topic_lens})
2. Your first line must use a {hook_type} hook. Make it completely different from the original opening.
3. The first 210 characters (before LinkedIn's "See More" fold) must create an irresistible open loop
4. MANDATORY: Include at least one SPECIFIC client win from the PROOF BANK in the system prompt. Use a real name and real number. This is the #1 thing that makes our posts perform.
5. Total post: 1,300-1,900 characters (the engagement sweet spot)
6. CTA TYPE FOR THIS POST: {cta_type}
   {cta_instruction}
7. Add 3-5 hashtags on the final line (mix broad + niche)
8. NEVER copy the original post's structure or phrasing, take the idea, make it ours
9. No external links anywhere in the post
10. Output ONLY the post text, nothing else, no labels, no explanations"""

    system_prompt = build_system_prompt(brand, state)

    try:
        response = client.messages.create(
            model=config.get("model", "claude-sonnet-4-6"),
            max_tokens=2000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        post_text = response.content[0].text.strip()
        record_client_used(post_text, brand, state)
        return post_text, hook_type, cta_type
    except Exception as e:
        log.error("Claude rewrite failed: %s", e)
        return None, hook_type, cta_type


# ---------------------------------------------------------------------------
# Content Queue Management
# ---------------------------------------------------------------------------

def add_to_queue(state: dict, post_text: str, source: dict, config: dict,
                 hook_type: str = "unknown", cta_type: str = "unknown") -> None:
    """Add a rewritten post to the content queue."""
    queue = state.setdefault("content_queue", [])
    entry = {
        "post_text": post_text,
        "source_url": source.get("url", ""),
        "source_author": source.get("author", "Unknown"),
        "viral_score": source.get("viral_score", 0),
        "rewritten_at": datetime.now(timezone.utc).isoformat(),
        "original_excerpt": source.get("text", "")[:100],
        "hook_type": hook_type,
        "cta_type": cta_type,
    }
    queue.append(entry)

    # Cap queue size
    max_size = config["publish"]["queue_size"]
    if len(queue) > max_size:
        state["content_queue"] = queue[-max_size:]
        log.info("Queue trimmed to %d entries", max_size)


def pop_from_queue(state: dict) -> dict | None:
    """Remove and return the oldest post from the queue (FIFO)."""
    queue = state.get("content_queue", [])
    if not queue:
        return None
    return queue.pop(0)


# ---------------------------------------------------------------------------
# LinkedIn Publishing
# ---------------------------------------------------------------------------

def publish_to_linkedin(post_text: str) -> dict | None:
    """Publish a post to LinkedIn via v2 UGC Posts API."""
    access_token = os.getenv("LINKEDIN_ACCESS_TOKEN")
    person_urn = os.getenv("LINKEDIN_PERSON_URN")

    if not access_token or not person_urn:
        log.error("LINKEDIN_ACCESS_TOKEN or LINKEDIN_PERSON_URN not set")
        return None

    url = "https://api.linkedin.com/v2/ugcPosts"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0",
    }
    body = {
        "author": person_urn,
        "lifecycleState": "PUBLISHED",
        "specificContent": {
            "com.linkedin.ugc.ShareContent": {
                "shareCommentary": {
                    "text": post_text,
                },
                "shareMediaCategory": "NONE",
            }
        },
        "visibility": {
            "com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC",
        },
    }

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)

        if resp.status_code == 401:
            log.error("LinkedIn token expired (401). Sending Slack alert.")
            slack_failure(
                "LinkedIn Engine",
                "LinkedIn access token has EXPIRED. Renew at https://www.linkedin.com/developers/apps -> Auth -> Generate token. "
                "Then update LINKEDIN_ACCESS_TOKEN in .env and GitHub secrets."
            )
            return None

        if resp.status_code == 429:
            log.warning("LinkedIn rate limited (429). Post stays in queue for next run.")
            return None

        resp.raise_for_status()

        data = resp.json()
        post_id = data.get("id", "")
        log.info("Published to LinkedIn: %s", post_id)
        return {
            "id": post_id,
            "url": f"https://www.linkedin.com/feed/update/{post_id}" if post_id else "",
        }

    except requests.exceptions.HTTPError as e:
        log.error("LinkedIn API error: %s — %s", e, resp.text if resp else "")
        return None
    except Exception as e:
        log.error("LinkedIn publish failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Archive & Notifications
# ---------------------------------------------------------------------------

def archive_post(post_text: str, source_info: dict, published: bool) -> Path | None:
    """Archive a post locally as a markdown file."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    first_line = post_text.split("\n")[0][:60]
    slug = re.sub(r"[^a-z0-9]+", "-", first_line.lower()).strip("-")[:80]
    filename = f"{date_str}-{slug}.md"
    filepath = ARCHIVE_DIR / filename

    counter = 1
    while filepath.exists():
        filepath = ARCHIVE_DIR / f"{date_str}-{slug}-{counter}.md"
        counter += 1

    content = f"""---
source: {source_info.get('source_url', '')}
source_author: {source_info.get('source_author', 'Unknown')}
viral_score: {source_info.get('viral_score', 0)}
generated: {datetime.now(timezone.utc).isoformat()}
published: {published}
---

{post_text}
"""
    filepath.write_text(content, encoding="utf-8")
    log.info("Archived to %s", filepath.name)
    return filepath


def track_published_post(state: dict, post_text: str, source_info: dict, linkedin_result: dict) -> None:
    """Record a published post in state for analytics tracking. Keeps last 50 posts."""
    published = state.setdefault("published_posts", [])
    first_line = post_text.split("\n")[0][:100]
    published.append({
        "linkedin_urn": linkedin_result.get("id", ""),
        "linkedin_url": linkedin_result.get("url", ""),
        "first_line": first_line,
        "hook_type": source_info.get("hook_type", "unknown"),
        "cta_type": source_info.get("cta_type", "unknown"),
        "viral_score": source_info.get("viral_score", 0),
        "source_author": source_info.get("source_author", "Unknown"),
        "published_at": datetime.now(timezone.utc).isoformat(),
        "char_count": len(post_text),
    })
    if len(published) > 50:
        state["published_posts"] = published[-50:]


def notify_slack_publish(post_text: str, source_info: dict, linkedin_result: dict | None) -> None:
    """Send Slack notification about published post."""
    first_line = post_text.split("\n")[0][:100]

    if linkedin_result:
        msg = (
            f":mega: *LinkedIn Post Published*\n"
            f"{first_line}\n\n"
            f"Source: {source_info.get('source_author', 'Unknown')} "
            f"(score: {source_info.get('viral_score', 0)})\n"
            f"Link: {linkedin_result.get('url', 'N/A')}"
        )
    else:
        msg = (
            f":memo: *LinkedIn Post Queued (not published)*\n"
            f"{first_line}\n\n"
            f"Source: {source_info.get('source_author', 'Unknown')} "
            f"(score: {source_info.get('viral_score', 0)})"
        )
    slack_send(msg)


def notify_slack_summary(found: int, queued: int, published: int, errors: int, queue_depth: int) -> None:
    """Send run summary to Slack."""
    msg = (
        f":bar_chart: *LinkedIn Engine Run Summary*\n"
        f"Found: {found} | Queued: {queued} | Published: {published} | "
        f"Errors: {errors} | Queue depth: {queue_depth}"
    )
    slack_send(msg)


# ---------------------------------------------------------------------------
# Performance Analytics Collection
# ---------------------------------------------------------------------------

def collect_post_analytics(state: dict) -> list[dict]:
    """Collect engagement analytics for published posts via Apify LinkedIn scraper."""
    apify_key = os.getenv("APIFY_API_KEY") or os.getenv("APIFY_API_TOKEN")
    if not apify_key:
        log.warning("APIFY_API_KEY not set, skipping analytics")
        return []

    published = state.get("published_posts", [])
    if not published:
        log.info("No published posts to collect analytics for")
        return []

    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    eligible = [
        p for p in published
        if p.get("linkedin_url") and p.get("published_at", "") >= cutoff
    ]

    if not eligible:
        log.info("No eligible posts for analytics (need URL, published within 14 days)")
        return published

    urls = [p["linkedin_url"] for p in eligible]
    log.info("Fetching analytics for %d posts via Apify...", len(urls))

    try:
        api_url = "https://api.apify.com/v2/acts/supreme_coder~linkedin-post/run-sync-get-dataset-items"
        resp = requests.post(
            api_url,
            params={"token": apify_key},
            json={"urls": urls},
            timeout=180,
        )
        resp.raise_for_status()
        results = resp.json()
    except Exception as e:
        log.warning("Apify analytics fetch failed: %s", e)
        return published

    log.info("Apify returned %d results for %d URLs", len(results), len(urls))

    results_by_input = {}
    for item in results:
        input_url = item.get("inputUrl", "") or item.get("url", "")
        if input_url:
            results_by_input[input_url] = item

    matched = 0
    for post in eligible:
        url = post.get("linkedin_url", "")
        item = results_by_input.get(url)

        if not item and len(results) == len(eligible):
            idx = eligible.index(post)
            item = results[idx]

        if item:
            total_reactions = item.get("numLikes", 0) or 0
            total_comments = item.get("numComments", 0) or 0
            total_reposts = item.get("numShares", 0) or 0

            snapshot = {
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "total_reactions": total_reactions,
                "total_comments": total_comments,
                "total_reposts": total_reposts,
            }

            analytics_history = post.get("analytics_history", [])
            analytics_history.append(snapshot)
            post["analytics_history"] = analytics_history[-10:]
            post["latest_analytics"] = snapshot
            matched += 1

            log.info(
                "  %s: %d reactions, %d comments, %d reposts",
                post.get("first_line", "")[:50], total_reactions, total_comments, total_reposts,
            )
        else:
            log.warning("  No match for: %s", url[:80])

    log.info("Analytics matched %d/%d posts", matched, len(eligible))
    state["published_posts"] = published
    return published


def log_analytics_summary(state: dict) -> None:
    """Log a summary of post performance."""
    published = state.get("published_posts", [])
    if not published:
        return

    posts_with_data = [p for p in published if p.get("latest_analytics")]
    if not posts_with_data:
        return

    log.info("=" * 50)
    log.info("POST PERFORMANCE SUMMARY")
    log.info("=" * 50)

    ranked = sorted(
        posts_with_data,
        key=lambda p: p["latest_analytics"].get("total_reactions", 0),
        reverse=True,
    )

    for i, post in enumerate(ranked, 1):
        a = post["latest_analytics"]
        log.info(
            "  %d. [%d reactions, %d comments] Hook: %s | CTA: %s — %s",
            i,
            a.get("total_reactions", 0),
            a.get("total_comments", 0),
            post.get("hook_type", "?"),
            post.get("cta_type", "?"),
            post.get("first_line", "")[:60],
        )

    if len(posts_with_data) >= 3:
        avg_reactions = sum(p["latest_analytics"]["total_reactions"] for p in posts_with_data) / len(posts_with_data)
        avg_comments = sum(p["latest_analytics"]["total_comments"] for p in posts_with_data) / len(posts_with_data)
        log.info("  Avg: %.1f reactions, %.1f comments across %d posts", avg_reactions, avg_comments, len(posts_with_data))

        hook_perf = {}
        for p in posts_with_data:
            ht = p.get("hook_type", "unknown")
            hook_perf.setdefault(ht, []).append(p["latest_analytics"]["total_reactions"])
        best_hook = max(hook_perf, key=lambda k: sum(hook_perf[k]) / len(hook_perf[k]))
        log.info("  Best hook type: %s (avg %.1f reactions)", best_hook, sum(hook_perf[best_hook]) / len(hook_perf[best_hook]))

        cta_perf = {}
        for p in posts_with_data:
            ct = p.get("cta_type", "unknown")
            cta_perf.setdefault(ct, []).append(p["latest_analytics"]["total_comments"])
        best_cta = max(cta_perf, key=lambda k: sum(cta_perf[k]) / len(cta_perf[k]))
        log.info("  Best CTA type: %s (avg %.1f comments)", best_cta, sum(cta_perf[best_cta]) / len(cta_perf[best_cta]))

    log.info("=" * 50)


# ---------------------------------------------------------------------------
# Schedule Variance
# ---------------------------------------------------------------------------

def should_skip_this_run() -> bool:
    """Randomly skip some runs to create natural posting rhythm."""
    now = datetime.now(timezone.utc)
    seed_str = now.strftime("%Y-%m-%d-%H")
    rng = random.Random(hashlib.md5(seed_str.encode()).hexdigest())

    hour = now.hour
    day = now.weekday()

    # Sun/Mon only have the primary slot — never skip
    if day in (0, 6) and hour == 14:
        return False

    # 3rd slot — skip ~35%
    if hour == 23:
        return rng.random() < 0.35

    # 2nd slot — skip ~15%
    if hour == 18:
        return rng.random() < 0.15

    # 1st slot — skip ~5%
    if hour == 14:
        return rng.random() < 0.05

    return False


# ---------------------------------------------------------------------------
# Backfill & Learnings
# ---------------------------------------------------------------------------

def backfill_published_from_archive(state: dict) -> int:
    """Scan archive files and add published posts not already tracked."""
    if not ARCHIVE_DIR.exists():
        log.info("No archive directory found")
        return 0

    added = 0
    for md_file in sorted(ARCHIVE_DIR.glob("*.md")):
        try:
            content = md_file.read_text(encoding="utf-8")
            if not content.startswith("---"):
                continue
            end = content.index("---", 3)
            frontmatter = content[3:end].strip()
            body = content[end + 3:].strip()

            meta = {}
            for line in frontmatter.split("\n"):
                if ":" in line:
                    key, val = line.split(":", 1)
                    meta[key.strip()] = val.strip()

            if meta.get("published", "").lower() != "true":
                continue

            first_line = body.split("\n")[0][:100]
            already_tracked = any(
                p.get("first_line", "")[:50] == first_line[:50]
                for p in state.get("published_posts", [])
            )
            if already_tracked:
                continue

            state.setdefault("published_posts", []).append({
                "linkedin_urn": "",
                "linkedin_url": "",
                "first_line": first_line,
                "hook_type": "unknown",
                "cta_type": "unknown",
                "viral_score": float(meta.get("viral_score", 0)),
                "source_author": meta.get("source_author", "Unknown"),
                "published_at": meta.get("generated", ""),
                "char_count": len(body),
                "backfilled": True,
            })
            added += 1
            log.info("  Backfilled: %s", first_line[:60])

        except Exception as e:
            log.warning("Error parsing %s: %s", md_file.name, e)

    return added


def show_learnings_report(state: dict) -> None:
    """Print a human-readable performance report."""
    published = state.get("published_posts", [])

    print("\n" + "=" * 70)
    print("  LINKEDIN PERFORMANCE REPORT")
    print("=" * 70)

    if not published:
        print("\n  No published posts tracked yet.")
        print("  Posts will be tracked automatically starting with the next publish.")
        print("  Run --backfill-published to import from archive files.\n")
        return

    posts_with_data = [p for p in published if p.get("latest_analytics")]
    posts_without_data = [p for p in published if not p.get("latest_analytics")]

    print(f"\n  Tracked posts: {len(published)}")
    print(f"  With analytics: {len(posts_with_data)}")
    print(f"  Awaiting data:  {len(posts_without_data)}")

    no_urn = [p for p in published if not p.get("linkedin_urn")]
    if no_urn:
        print(f"\n  Posts without LinkedIn URN (backfilled, can't pull analytics): {len(no_urn)}")

    if posts_with_data:
        print("\n" + "-" * 70)
        print("  POST PERFORMANCE (sorted by reactions)")
        print("-" * 70)

        ranked = sorted(
            posts_with_data,
            key=lambda p: p["latest_analytics"].get("total_reactions", 0),
            reverse=True,
        )

        for i, post in enumerate(ranked, 1):
            a = post["latest_analytics"]
            reactions = a.get("total_reactions", 0)
            comments = a.get("total_comments", 0)
            reposts = a.get("total_reposts", 0)
            hook = post.get("hook_type", "?")
            cta = post.get("cta_type", "?")
            chars = post.get("char_count", 0)

            print(f"\n  {i}. {post.get('first_line', '')[:65]}")
            print(f"     Reactions: {reactions}  |  Comments: {comments}  |  Reposts: {reposts}  |  Chars: {chars}")
            print(f"     Hook: {hook}  |  CTA: {cta}")

        if len(posts_with_data) >= 2:
            print("\n" + "-" * 70)
            print("  AGGREGATE INSIGHTS")
            print("-" * 70)

            avg_reactions = sum(p["latest_analytics"]["total_reactions"] for p in posts_with_data) / len(posts_with_data)
            avg_comments = sum(p["latest_analytics"]["total_comments"] for p in posts_with_data) / len(posts_with_data)
            print(f"\n  Average: {avg_reactions:.1f} reactions, {avg_comments:.1f} comments per post")

            hook_perf = {}
            for p in posts_with_data:
                ht = p.get("hook_type", "unknown")
                hook_perf.setdefault(ht, []).append(p["latest_analytics"]["total_reactions"])

            if len(hook_perf) > 1 or (len(hook_perf) == 1 and "unknown" not in hook_perf):
                print("\n  Hook Type Performance (avg reactions):")
                for ht, vals in sorted(hook_perf.items(), key=lambda x: -sum(x[1]) / len(x[1])):
                    avg = sum(vals) / len(vals)
                    bar = "#" * int(avg / max(avg_reactions, 1) * 20)
                    print(f"    {ht:25s} {avg:6.1f}  {bar}  (n={len(vals)})")

            cta_perf = {}
            for p in posts_with_data:
                ct = p.get("cta_type", "unknown")
                cta_perf.setdefault(ct, []).append(p["latest_analytics"]["total_comments"])

            if len(cta_perf) > 1 or (len(cta_perf) == 1 and "unknown" not in cta_perf):
                print("\n  CTA Type Performance (avg comments):")
                for ct, vals in sorted(cta_perf.items(), key=lambda x: -sum(x[1]) / len(x[1])):
                    avg = sum(vals) / len(vals)
                    bar = "#" * int(avg / max(avg_comments, 1) * 20)
                    print(f"    {ct:25s} {avg:6.1f}  {bar}  (n={len(vals)})")

            short = [p for p in posts_with_data if p.get("char_count", 0) < 1300]
            sweet = [p for p in posts_with_data if 1300 <= p.get("char_count", 0) <= 1900]
            long_ = [p for p in posts_with_data if p.get("char_count", 0) > 1900]

            if short or sweet or long_:
                print("\n  Length vs Performance:")
                for label, group in [("< 1300 chars", short), ("1300-1900 chars (sweet spot)", sweet), ("> 1900 chars", long_)]:
                    if group:
                        avg_r = sum(p["latest_analytics"]["total_reactions"] for p in group) / len(group)
                        print(f"    {label:30s}  avg {avg_r:.1f} reactions  (n={len(group)})")

    if posts_without_data:
        print("\n" + "-" * 70)
        print(f"  AWAITING ANALYTICS ({len(posts_without_data)} posts)")
        print("-" * 70)
        for p in posts_without_data[:5]:
            urn_status = "has URN" if p.get("linkedin_urn") else "no URN (backfilled)"
            print(f"    {p.get('first_line', '')[:55]}  [{urn_status}]")
        if len(posts_without_data) > 5:
            print(f"    ... and {len(posts_without_data) - 5} more")

    print("\n" + "=" * 70)
    print()


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def main():
    load_dotenv(override=True)

    parser = argparse.ArgumentParser(description="LinkedIn Content Engine")
    parser.add_argument("--dry-run", action="store_true", help="Discover + rewrite but don't publish or queue")
    parser.add_argument("--discover-only", action="store_true", help="Only run discovery, print results")
    parser.add_argument("--publish-queue", action="store_true", help="Skip discovery, publish next queued post")
    parser.add_argument("--backfill", type=int, default=7, help="Look back N days for content (default: 7)")
    parser.add_argument("--collect-analytics", action="store_true", help="Collect engagement analytics for published posts")
    parser.add_argument("--show-learnings", action="store_true", help="Show performance report and learnings from analytics data")
    parser.add_argument("--backfill-published", action="store_true", help="Backfill published_posts from archive files")
    parser.add_argument("--skip-check", action="store_true", help="Skip the schedule variance check (force run)")
    args = parser.parse_args()

    config = load_config()
    brand = load_brand()
    state = load_state()

    # Pull testimonials live from Airtable (replaces static client_wins in brand.json)
    airtable_wins = fetch_airtable_testimonials(config)
    if airtable_wins:
        brand["client_wins"] = airtable_wins

    log.info("=" * 60)
    log.info("LinkedIn Content Engine — starting run")
    log.info("=" * 60)

    # BACKFILL MODE
    if args.backfill_published:
        log.info("Backfilling published posts from archive files...")
        count = backfill_published_from_archive(state)
        log.info("Backfilled %d posts", count)
        save_state(state)
        print(f"\nBackfilled {count} posts. Total tracked: {len(state.get('published_posts', []))}")
        return

    # SHOW LEARNINGS
    if args.show_learnings:
        posts_with_urns = [p for p in state.get("published_posts", []) if p.get("linkedin_urn")]
        if posts_with_urns:
            log.info("Collecting fresh analytics before showing report...")
            collect_post_analytics(state)
            save_state(state)
        show_learnings_report(state)
        return

    # ANALYTICS-ONLY MODE
    if args.collect_analytics:
        log.info("Collecting analytics for published posts...")
        collect_post_analytics(state)
        log_analytics_summary(state)
        save_state(state)
        return

    # SCHEDULE VARIANCE
    if not args.skip_check and not args.dry_run and not args.discover_only and not args.publish_queue:
        if should_skip_this_run():
            log.info("Schedule variance: skipping this run for natural posting rhythm")
            slack_send(":calendar: LinkedIn engine skipped this slot (schedule variance)")
            return

    # COLLECT ANALYTICS on prior posts
    if state.get("published_posts") and not args.dry_run:
        log.info("Collecting analytics on prior posts...")
        collect_post_analytics(state)
        log_analytics_summary(state)
        save_state(state)

    stats = {"found": 0, "queued": 0, "published": 0, "errors": 0}

    # PUBLISH-QUEUE MODE
    if args.publish_queue:
        log.info("Publish-queue mode: publishing next queued post")
        entry = pop_from_queue(state)
        if entry:
            result = publish_to_linkedin(entry["post_text"])
            archive_post(entry["post_text"], entry, published=bool(result))
            if result:
                stats["published"] = 1
                track_published_post(state, entry["post_text"], entry, result)
                notify_slack_publish(entry["post_text"], entry, result)
            else:
                state.setdefault("content_queue", []).insert(0, entry)
                stats["errors"] = 1
        else:
            log.warning("Queue is empty, nothing to publish")
        save_state(state)
        notify_slack_summary(0, 0, stats["published"], stats["errors"], len(state.get("content_queue", [])))
        return

    # DISCOVERY PHASE
    log.info("Phase 1: Discovering viral LinkedIn posts...")
    discovered = discover_viral_posts(config, brand, state, backfill_days=args.backfill)
    stats["found"] = len(discovered)

    if args.discover_only:
        log.info("Discover-only mode. Results:")
        for i, post in enumerate(discovered, 1):
            log.info(
                "  %d. [Score: %.1f] %s — %s (%d reactions, %d comments)",
                i, post["viral_score"], post["author"],
                post["text"][:80].replace("\n", " "),
                post["reactions"], post["comments"],
            )
        return

    if not discovered:
        log.info("No new viral posts found this run")
        if not args.dry_run and state.get("content_queue"):
            log.info("Publishing from existing queue...")
            entry = pop_from_queue(state)
            if entry:
                result = publish_to_linkedin(entry["post_text"])
                archive_post(entry["post_text"], entry, published=bool(result))
                if result:
                    stats["published"] = 1
                    track_published_post(state, entry["post_text"], entry, result)
                    notify_slack_publish(entry["post_text"], entry, result)
                else:
                    state.setdefault("content_queue", []).insert(0, entry)
                    stats["errors"] = 1
        save_state(state)
        notify_slack_summary(stats["found"], stats["queued"], stats["published"], stats["errors"],
                           len(state.get("content_queue", [])))
        return

    # REWRITE PHASE
    log.info("Phase 2: Rewriting %d posts with Claude...", len(discovered))

    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    except Exception as e:
        log.error("Failed to initialize Anthropic client: %s", e)
        slack_failure("LinkedIn Engine", f"Anthropic client init failed: {e}")
        return

    for post in discovered:
        log.info("  Rewriting: %s (score: %.1f)", post["text"][:60].replace("\n", " "), post["viral_score"])
        rewritten, hook_type, cta_type = rewrite_post(client, post, config, brand, state)

        if rewritten:
            if args.dry_run:
                log.info("  [DRY RUN] Rewritten post (%d chars):", len(rewritten))
                print("\n" + "=" * 40)
                print(rewritten)
                print("=" * 40 + "\n")
                archive_post(rewritten, {
                    "source_url": post.get("url", ""),
                    "source_author": post.get("author", "Unknown"),
                    "viral_score": post.get("viral_score", 0),
                }, published=False)
            else:
                add_to_queue(state, rewritten, post, config, hook_type=hook_type, cta_type=cta_type)
                stats["queued"] += 1
                log.info("  Added to queue (depth: %d)", len(state.get("content_queue", [])))

            state.setdefault("processed_ids", []).append(post["id"])
        else:
            stats["errors"] += 1
            log.warning("  Failed to rewrite post")

    save_state(state)

    if args.dry_run:
        log.info("Dry run complete. %d posts rewritten, nothing published.", stats["queued"] + stats["found"])
        return

    # PUBLISH PHASE
    log.info("Phase 3: Publishing from queue...")
    posts_to_publish = config["publish"]["posts_per_run"]

    for _ in range(posts_to_publish):
        entry = pop_from_queue(state)
        if not entry:
            log.warning("Queue empty, nothing to publish")
            break

        if config["publish"]["auto_publish"]:
            result = publish_to_linkedin(entry["post_text"])
            archive_post(entry["post_text"], entry, published=bool(result))

            if result:
                stats["published"] += 1
                track_published_post(state, entry["post_text"], entry, result)
                notify_slack_publish(entry["post_text"], entry, result)
                log.info("Published successfully: %s", result.get("url", ""))
            else:
                state.setdefault("content_queue", []).insert(0, entry)
                stats["errors"] += 1
                log.error("Publish failed, post returned to queue")
        else:
            archive_post(entry["post_text"], entry, published=False)
            notify_slack_publish(entry["post_text"], entry, None)
            log.info("Post archived (auto_publish disabled)")

    # FINALIZE
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(state)

    log.info("Run complete. Found: %d | Queued: %d | Published: %d | Errors: %d | Queue: %d",
             stats["found"], stats["queued"], stats["published"], stats["errors"],
             len(state.get("content_queue", [])))

    notify_slack_summary(
        stats["found"], stats["queued"], stats["published"], stats["errors"],
        len(state.get("content_queue", []))
    )


if __name__ == "__main__":
    main()
