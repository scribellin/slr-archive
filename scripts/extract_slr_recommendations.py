#!/usr/bin/env python3
"""Extract individual SLR recommendations from a DOCX archive list."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
import json
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

DOC_NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pr": "http://schemas.openxmlformats.org/package/2006/relationships",
}

MIN_HEADLINE_LEN = 12
BYLINE_RE = re.compile(
    r"^By\s+(?P<writer>.+?)\s+for\s+(?P<outlet>.+?)(?:\s*\(~?\d+.*)?$",
    flags=re.IGNORECASE,
)
INLINE_BYLINE_RE = re.compile(
    r"^(?P<title>.+?)\s+By\s+(?P<writer>.+?)\s+for\s+(?P<outlet>.+?)(?:\s*\(~?\d+.*)?$",
    flags=re.IGNORECASE,
)

NOISE_FRAGMENTS = (
    "subscribe",
    "open in browser",
    "view web version",
    "email us",
    "follow us",
    "enjoy the best longform journalism",
    "how you can support",
    "issue #",
    "the sunday long read exists",
    "bonus issues",
    "gratitude",
    "update your preferences",
    "unsubscribe from this list",
    "co-founder",
    "editor-in-chief",
    "managing editor",
)

BAD_HEADLINE_FRAGMENTS = (
    "recommended in",
    "dvn's favorite",
    "jacob's favorite",
    "don's favorite",
    "our favorites",
    "favorite reads",
    "contributing editors",
    "most-clicked",
    "top 10",
    "best of",
    "editors' note",
    "editors note",
    "quotation of the week",
    "lede of the week",
    "how you can support",
    "enjoy the best longform journalism",
)

GENERIC_HEADINGS = {
    "don's favorite",
    "don’s favorite",
    "jacob's favorite",
    "jacob’s favorite",
    "don and jacob's favorite",
    "don and jacob’s favorite",
    "dave’s favorite",
    "dave's favorite",
    "seyward's favorite",
    "seyward’s favorite",
    "don's favorite read:",
    "jacob's favorite read:",
    "hello and welcome back",
}

TOPIC_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Politics", ("election", "congress", "senate", "white house", "trump", "biden", "campaign", "policy", "court")),
    ("Technology", ("ai", "artificial intelligence", "tech", "silicon valley", "internet", "social media", "crypto")),
    ("Business", ("market", "economy", "finance", "investor", "startup", "company", "ceo", "bank")),
    ("Media", ("journalism", "newsroom", "reporter", "editor", "press", "publisher", "magazine")),
    ("Health", ("health", "medical", "hospital", "disease", "mental", "doctor", "patient")),
    ("Science", ("science", "research", "space", "physics", "biology", "climate", "environment")),
    ("Sports", ("sports", "football", "basketball", "baseball", "olympic", "athlete")),
    ("Culture", ("film", "music", "book", "art", "theater", "tv", "celebrity", "fashion")),
    ("Crime", ("crime", "murder", "police", "trial", "prison", "fraud", "investigation")),
    ("International", ("ukraine", "china", "russia", "gaza", "israel", "europe", "africa", "global")),
]

ARTICLE_IMAGE_META_KEYS = (
    "og:image",
    "og:image:url",
    "og:image:secure_url",
    "twitter:image",
    "twitter:image:src",
    "image",
)

EXCLUDED_HEADLINES = {
    "read more books",
}


@dataclass
class Issue:
    date: str
    title: str
    url: str


def clean_text(raw: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", raw)
    decoded = html.unescape(no_tags).replace("\xa0", " ")
    return " ".join(decoded.split()).strip()


def dedupe_repeated_phrase(text: str) -> str:
    candidate = text.strip()
    while candidate:
        match = re.match(r"^(?P<phrase>.+?)\s+\1$", candidate)
        if not match:
            return candidate
        candidate = match.group("phrase").strip()
    return text.strip()


def first_sentence(text: str) -> str:
    text = text.strip()
    if not text:
        return ""
    split = re.split(r"(?<=[.!?])\s+", text)
    sentence = split[0] if split else text
    return sentence[:320].strip()


def looks_like_noise(text: str) -> bool:
    lower = text.lower()
    if not text or len(text) < 6:
        return True
    return any(fragment in lower for fragment in NOISE_FRAGMENTS)


def looks_like_summary_noise(text: str) -> bool:
    lower = text.lower().strip()
    if looks_like_noise(text):
        return True
    if lower.startswith(">"):
        return True
    if "free for slr readers" in lower:
        return True
    if "non-paywalled link created for sunday long read subscribers" in lower:
        return True
    if "listen to" in lower and "podcast" in lower:
        return True
    return False


def detect_favorite_owner(text: str) -> str:
    key = normalize_compare_key(text)
    if not key:
        return ""
    if "don and jacob" in key and "favorite" in key:
        return "Don & Jacob"
    if "jacob and don" in key and "favorite" in key:
        return "Don & Jacob"
    if "don s favorite" in key or "dvn s favorite" in key:
        return "Don"
    if "jacob s favorite" in key:
        return "Jacob"
    return ""


def is_repeat_section_heading(text: str) -> bool:
    key = normalize_compare_key(text)
    if not key:
        return False
    if len(key.split()) > 12:
        return False
    if key.startswith("last week s most reads"):
        return True
    if key.startswith("last weeks most reads"):
        return True
    if key.startswith("last week s most clicked"):
        return True
    if key.startswith("last weeks most clicked"):
        return True
    if key in {"most read last week", "most clicked last week"}:
        return True
    return False


def detect_package_marker(text: str) -> tuple[str, tuple[str, ...]]:
    key = normalize_compare_key(text)
    if not key:
        return "", ()
    if "slr syllabus" in key:
        cleaned = re.sub(r"\s+", " ", text).strip(" :")
        return cleaned or "SLR Syllabus", ()
    if "the locals" in key:
        return "The Locals", ()
    if "epstein" in key and "pieces worth your time" in key:
        return "SLR Syllabus: Epstein Files", ("epstein",)
    if "pieces in this syllabus" in key and "epstein" in key:
        return "SLR Syllabus: Epstein Files", ("epstein",)
    return "", ()


def detect_favorite_bio_owner(text: str) -> str:
    key = normalize_compare_key(text)
    if key.startswith("don van natta jr") and "pulitzer" in key:
        return "Don"
    if key.startswith("jacob feldman") and ("sportico" in key or "sports illustrated" in key):
        return "Jacob"
    return ""


def infer_favorites_from_bios(headings: list[dict[str, str]]) -> dict[int, str]:
    bio_markers: list[tuple[int, str]] = []
    for idx, heading in enumerate(headings):
        owner = detect_favorite_bio_owner(heading["text"])
        if owner and idx < 70:
            bio_markers.append((idx, owner))
    if not bio_markers:
        return {}

    story_indices = [
        idx
        for idx, heading in enumerate(headings)
        if heading["level"] == "1" and looks_like_story_title(normalize_headline(heading["text"]))
    ]
    favorites: dict[int, str] = {}
    previous_marker = -1
    for marker_idx, owner in bio_markers:
        between = [story_idx for story_idx in story_indices if previous_marker < story_idx < marker_idx]
        if 1 <= len(between) <= 3:
            for story_idx in between:
                favorites[story_idx] = owner
        previous_marker = marker_idx
    return favorites


def should_exclude_item(
    headline: str,
    outlet: str,
    summary: str,
    url: str,
    exclude_repeats: bool,
    exclude_curators: bool,
) -> bool:
    if exclude_repeats:
        return True
    if exclude_curators:
        return True
    headline_key = normalize_compare_key(headline)
    summary_key = normalize_compare_key(summary)
    outlet_key = normalize_compare_key(outlet)
    url_lower = url.lower().strip()

    if headline_key in EXCLUDED_HEADLINES:
        return True
    if "sponsored" in headline_key or "sponsored" in summary_key:
        return True
    if "staff curators" in summary_key or "staff curators" in headline_key:
        return True
    if "curators" in summary_key and "classics" in summary_key and "photos" in summary_key:
        return True
    if "beehiiv" in url_lower and headline_key.startswith("read more"):
        return True
    if outlet_key == "beehiiv" and headline_key.startswith("read more"):
        return True
    return False


def normalize_headline(text: str) -> str:
    text = text.strip(" :")
    text = re.sub(r"^[a-z0-9.-]+\.[a-z]{2,}\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\W*f\s+", "", text)
    return text.strip(" -:")


def valid_story_url(url: str) -> bool:
    if not url:
        return False
    lower = url.lower()
    if lower.startswith(("mailto:", "javascript:")):
        return False
    blocked = (
        "list-manage.com/subscribe",
        "list-manage.com/profile",
        "list-manage.com/unsubscribe",
        "campaign-archive.com/home",
        "twitter.com/share",
        "facebook.com",
        "google.com/translate",
    )
    return not any(item in lower for item in blocked)


def choose_link(inner_html: str) -> str:
    links = re.findall(r"""<a[^>]+href=["']([^"']+)["']""", inner_html, flags=re.IGNORECASE)
    for link in links:
        cleaned = html.unescape(link).strip()
        if valid_story_url(cleaned):
            return cleaned
    return ""


def parse_byline_text(text: str) -> tuple[str, str]:
    text = text.strip()
    m = BYLINE_RE.match(text)
    if m:
        writer = m.group("writer").strip(" .")
        outlet = clean_outlet(m.group("outlet"))
        return writer, outlet
    return "", ""


def clean_outlet(raw: str) -> str:
    text = raw.strip(" .")
    text = re.sub(r"\s*\(~?\d+.*$", "", text).strip()
    text = text.replace("[$]", "").strip()
    return text


def infer_topic(headline: str, outlet: str, summary: str) -> str:
    haystack = f"{headline} {outlet} {summary}".lower()
    for topic, keywords in TOPIC_RULES:
        if any(keyword in haystack for keyword in keywords):
            return topic
    return "General"


def parse_docx_issues(docx_path: Path) -> list[Issue]:
    with zipfile.ZipFile(docx_path) as zf:
        doc_root = ET.fromstring(zf.read("word/document.xml"))
        rels_root = ET.fromstring(zf.read("word/_rels/document.xml.rels"))

    rel_map = {
        rel.attrib["Id"]: html.unescape(rel.attrib.get("Target", ""))
        for rel in rels_root.findall("pr:Relationship", DOC_NS)
    }

    issues: list[Issue] = []
    for paragraph in doc_root.findall(".//w:p", DOC_NS):
        text = "".join(t.text or "" for t in paragraph.findall(".//w:t", DOC_NS))
        rid = None
        link_text = ""
        for h in paragraph.findall(".//w:hyperlink", DOC_NS):
            rid = h.attrib.get(f"{{{DOC_NS['r']}}}id")
            link_text = "".join(t.text or "" for t in h.findall(".//w:t", DOC_NS))
            if rid:
                break
        if not rid:
            continue
        url = rel_map.get(rid, "")
        if "campaign-archive.com/?u=" not in url:
            continue
        merged = clean_text(text)
        if link_text and normalize_compare_key(link_text) not in normalize_compare_key(merged):
            merged = clean_text(f"{merged} {link_text}")
        date_match = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", merged)
        if not date_match:
            continue
        date = datetime.strptime(date_match.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
        title = re.sub(r"^\d{2}/\d{2}/\d{4}\s*-\s*", "", merged).strip()
        title = dedupe_repeated_phrase(title)
        issues.append(Issue(date=date, title=title, url=url))

    # Preserve source order and remove exact duplicate issue URLs.
    deduped: list[Issue] = []
    seen: set[str] = set()
    for issue in issues:
        if issue.url in seen:
            continue
        seen.add(issue.url)
        deduped.append(issue)
    return deduped


def fetch_issue_html(url: str, cache_dir: Path | None) -> str:
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        issue_id = parse_qs(urlparse(url).query).get("id", ["unknown"])[0]
        cache_path = cache_dir / f"{issue_id}.html"
        if cache_path.exists():
            return cache_path.read_text(encoding="utf-8", errors="ignore")

    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=45) as response:
        html_text = response.read().decode("utf-8", errors="ignore")
    if cache_dir:
        cache_path.write_text(html_text, encoding="utf-8")
    time.sleep(0.08)
    return html_text


def canonical_story_url(url: str) -> str:
    parsed = urlparse(url.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""
    return urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path or "/",
            "",
            "",
            "",
        )
    )


def parse_meta_attributes(tag: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for name, value in re.findall(
        r"""([a-zA-Z_:.-]+)\s*=\s*(".*?"|'.*?'|[^\s>]+)""",
        tag,
        flags=re.DOTALL,
    ):
        value = value.strip().strip("'\"")
        attrs[name.lower()] = html.unescape(value).strip()
    return attrs


def normalize_image_url(raw_url: str, page_url: str) -> str:
    if not raw_url:
        return ""
    candidate = html.unescape(raw_url).strip()
    if not candidate:
        return ""
    if candidate.startswith("data:image/"):
        return ""
    normalized = urljoin(page_url, candidate)
    parsed = urlparse(normalized)
    if parsed.scheme.lower() not in {"http", "https"}:
        return ""
    return normalized


def extract_article_image(html_text: str, page_url: str) -> str:
    for match in re.finditer(r"<meta\b[^>]*>", html_text, flags=re.IGNORECASE):
        attrs = parse_meta_attributes(match.group(0))
        key = attrs.get("property") or attrs.get("name") or attrs.get("itemprop") or ""
        if key.lower() not in ARTICLE_IMAGE_META_KEYS:
            continue
        image = normalize_image_url(attrs.get("content", ""), page_url)
        if image:
            return image
    return ""


def fetch_article_image(url: str, timeout: float = 12.0, max_bytes: int = 800_000) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urlopen(req, timeout=timeout) as response:
            content_type = (response.headers.get("Content-Type") or "").lower()
            if content_type.startswith("image/"):
                return url
            payload = response.read(max_bytes)
            encoding = response.headers.get_content_charset() or "utf-8"
    except Exception:  # noqa: BLE001
        return ""

    html_text = payload.decode(encoding, errors="ignore")
    return extract_article_image(html_text, url)


def load_article_image_cache(cache_file: Path) -> dict[str, str]:
    if not cache_file.exists():
        return {}
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    if isinstance(data, dict):
        return {str(k): str(v) for k, v in data.items()}
    return {}


def save_article_image_cache(cache_file: Path, cache: dict[str, str]) -> None:
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def enrich_with_article_images(
    items: list[dict[str, str]],
    cache_file: Path,
    workers: int = 12,
    timeout: float = 12.0,
) -> tuple[int, int]:
    cache = load_article_image_cache(cache_file)
    canonical_to_url: dict[str, str] = {}
    for item in items:
        story_url = str(item.get("url", "")).strip()
        if not valid_story_url(story_url):
            continue
        canonical = canonical_story_url(story_url)
        if canonical and canonical not in canonical_to_url:
            canonical_to_url[canonical] = story_url

    pending = [key for key in canonical_to_url if key not in cache]
    if pending:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            future_map = {
                pool.submit(fetch_article_image, canonical_to_url[key], timeout): key
                for key in pending
            }
            completed = 0
            for future in as_completed(future_map):
                key = future_map[future]
                try:
                    cache[key] = future.result() or ""
                except Exception:  # noqa: BLE001
                    cache[key] = ""
                completed += 1
                if completed % 200 == 0:
                    print(f"Article image lookup: {completed}/{len(pending)}")
        save_article_image_cache(cache_file, cache)

    with_image = 0
    for item in items:
        story_url = str(item.get("url", "")).strip()
        canonical = canonical_story_url(story_url)
        image = cache.get(canonical, "") if canonical else ""
        item["leadImage"] = image
        if image:
            with_image += 1
    return with_image, len(canonical_to_url)


def parse_headings(html_text: str) -> list[dict[str, str]]:
    headings: list[dict[str, str]] = []
    for match in re.finditer(r"<h([1-4])[^>]*>(.*?)</h\1>", html_text, flags=re.IGNORECASE | re.DOTALL):
        level = int(match.group(1))
        raw = match.group(2)
        text = clean_text(raw)
        if text:
            headings.append(
                {
                    "level": str(level),
                    "raw": raw,
                    "text": text,
                    "start": str(match.start()),
                    "end": str(match.end()),
                }
            )
    return headings


def normalize_compare_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def anchor_catalog(html_text: str) -> list[dict[str, str]]:
    anchors: list[dict[str, str]] = []
    for match in re.finditer(
        r"""<a[^>]+href=["']([^"']+)["'][^>]*>(.*?)</a>""",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        href = html.unescape(match.group(1)).strip()
        if not valid_story_url(href):
            continue
        text = clean_text(match.group(2))
        if not text:
            continue
        anchors.append({"href": href, "text": text, "key": normalize_compare_key(text)})
    return anchors


def match_anchor_url(title: str, anchors: list[dict[str, str]]) -> str:
    key = normalize_compare_key(title)
    if not key:
        return ""
    for anchor in anchors:
        if anchor["key"] == key:
            return anchor["href"]
    if len(key) >= 18:
        for anchor in anchors:
            if key in anchor["key"] or anchor["key"] in key:
                return anchor["href"]
    return ""


def parse_images(html_text: str) -> list[dict[str, str]]:
    images: list[dict[str, str]] = []
    for match in re.finditer(r"<img\b[^>]*>", html_text, flags=re.IGNORECASE):
        raw = match.group(0)
        src_match = re.search(r"""src=["']([^"']+)["']""", raw, flags=re.IGNORECASE)
        if not src_match:
            continue
        src = html.unescape(src_match.group(1)).strip()
        if not src:
            continue
        width_match = re.search(r"""width=["']?(\d+)["']?""", raw, flags=re.IGNORECASE)
        height_match = re.search(r"""height=["']?(\d+)["']?""", raw, flags=re.IGNORECASE)
        width = int(width_match.group(1)) if width_match else 0
        height = int(height_match.group(1)) if height_match else 0
        images.append(
            {
                "src": src,
                "start": str(match.start()),
                "end": str(match.end()),
                "width": str(width),
                "height": str(height),
            }
        )
    return images


def is_story_image(src: str, width: int, height: int) -> bool:
    lower = src.lower()
    blocked = (
        "cdn-images.mailchimp.com/icons/",
        "google.com/s2/favicons",
        "intuit-mc-rewards",
        "social-block-v2",
        "mailchimp.com",
        "monkey_rewards",
        "acc795a1-25b3-424b-83a7-c4676f26fd45",
    )
    if any(token in lower for token in blocked):
        return False
    if lower.startswith("data:image/"):
        return False
    if width and width < 180:
        return False
    if height and height < 120:
        return False
    return True


def choose_story_image(images: list[dict[str, str]], previous_start: int, story_start: int) -> str:
    candidates: list[dict[str, str]] = []
    for image in images:
        pos = int(image["start"])
        if pos <= previous_start or pos >= story_start:
            continue
        src = image["src"]
        width = int(image["width"])
        height = int(image["height"])
        if not is_story_image(src, width, height):
            continue
        candidates.append(image)
    if not candidates:
        return ""
    return candidates[-1]["src"]


def html_text_lines(html_text: str) -> list[str]:
    cleaned = re.sub(r"(?is)<script.*?</script>", " ", html_text)
    cleaned = re.sub(r"(?is)<style.*?</style>", " ", cleaned)
    cleaned = re.sub(r"(?is)<!--.*?-->", " ", cleaned)
    cleaned = re.sub(r"(?i)<br\\s*/?>", "\n", cleaned)
    cleaned = re.sub(r"<[^>]+>", "\n", cleaned)
    lines = [clean_text(line) for line in cleaned.splitlines()]
    lines = [line for line in lines if line]
    compact: list[str] = []
    for line in lines:
        if compact and compact[-1] == line:
            continue
        compact.append(line)
    return compact


def looks_like_writer_line(line: str) -> bool:
    if len(line.split()) > 12:
        return False
    if re.search(r"[.!?]", line):
        return False
    lower = line.lower()
    if looks_like_noise(line):
        return False
    if lower.startswith(("no.", "the ", "dvn", "jacob", "chosen ", "help ", "want ")):
        return False
    if "@" in line or "#" in line:
        return False
    return any(ch.isalpha() for ch in line)


def parse_legacy_numbered_items(html_text: str) -> list[dict[str, str]]:
    lines = html_text_lines(html_text)
    anchors = anchor_catalog(html_text)
    items: list[dict[str, str]] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        match = re.match(r"^No\.\s*(\d+)\s*:?\s*(.*)$", line, flags=re.IGNORECASE)
        if not match:
            i += 1
            continue

        title = match.group(2).strip()
        j = i + 1
        if not title:
            while j < len(lines):
                if re.match(r"^No\.\s*\d+", lines[j], flags=re.IGNORECASE):
                    break
                candidate = lines[j]
                if len(candidate) > 6 and not looks_like_noise(candidate):
                    title = candidate
                    j += 1
                    break
                j += 1

        outlet = ""
        while j < len(lines):
            candidate = lines[j]
            if re.match(r"^No\.\s*\d+", candidate, flags=re.IGNORECASE):
                break
            if looks_like_noise(candidate):
                j += 1
                continue
            if len(candidate) <= 70 and len(candidate.split()) <= 8 and not re.search(r"[.!?]", candidate):
                outlet = candidate
                j += 1
                break
            j += 1

        writer = ""
        if j < len(lines) and looks_like_writer_line(lines[j]):
            writer = lines[j]
            j += 1

        summary_parts: list[str] = []
        while j < len(lines):
            candidate = lines[j]
            if re.match(r"^No\.\s*\d+", candidate, flags=re.IGNORECASE):
                break
            if looks_like_noise(candidate) or len(candidate) < 25:
                j += 1
                continue
            summary_parts.append(candidate)
            if re.search(r"[.!?]$", candidate):
                break
            if len(" ".join(summary_parts)) > 360:
                break
            j += 1

        if title:
            items.append(
                {
                    "headline": title,
                    "outlet": outlet,
                    "writer": writer,
                    "summary": first_sentence(" ".join(summary_parts)),
                    "url": match_anchor_url(title, anchors),
                }
            )

        i += 1
    return items


def looks_like_story_title(text: str) -> bool:
    lower = text.lower().strip()
    if len(text) < MIN_HEADLINE_LEN:
        return False
    if len(text) > 190:
        return False
    if len(text.split()) > 28:
        return False
    if lower in GENERIC_HEADINGS:
        return False
    if any(fragment in lower for fragment in BAD_HEADLINE_FRAGMENTS):
        return False
    if looks_like_noise(text):
        return False
    if lower.endswith(":"):
        return False
    return any(ch.isalpha() for ch in text)


def parse_inline_story_line(text: str) -> tuple[str, str, str]:
    """Extract title, writer, outlet from one heading line when possible."""
    m = INLINE_BYLINE_RE.match(text)
    if not m:
        return "", "", ""
    title = normalize_headline(m.group("title"))
    writer = m.group("writer").strip(" .")
    outlet = clean_outlet(m.group("outlet"))
    if not looks_like_story_title(title):
        return "", "", ""
    if len(writer) > 90 or len(writer.split()) > 12:
        return "", "", ""
    if len(outlet) > 80 or len(outlet.split()) > 10:
        return "", "", ""
    return title, writer, outlet


def heading_is_story_marker(heading: dict[str, str]) -> bool:
    text = heading["text"]
    if heading["level"] == "1" and looks_like_story_title(normalize_headline(text)):
        return True
    title, writer, outlet = parse_inline_story_line(text)
    return bool(title and (writer or outlet))


def is_curator_section_heading(text: str) -> bool:
    key = normalize_compare_key(text)
    if not key:
        return False
    if "staff curators" in key:
        return True
    if "last week we sent out a member s only edition" in key:
        return True
    return False


def section_context_for_index(headings: list[dict[str, str]], idx: int) -> tuple[str, bool, bool, str]:
    favorite_by = ""
    exclude_repeats = False
    exclude_curators = False
    package = ""
    package_keywords: tuple[str, ...] = ()
    package_marker_idx = -1

    for j in range(idx - 1, -1, -1):
        text = headings[j]["text"]
        if not favorite_by:
            favorite_by = detect_favorite_owner(text)

        if is_repeat_section_heading(text):
            exclude_repeats = True
        if is_curator_section_heading(text):
            exclude_curators = True

        if not package:
            marker, keywords = detect_package_marker(text)
            if marker:
                package = marker
                package_keywords = keywords
                package_marker_idx = j

        if heading_is_story_marker(headings[j]) and favorite_by:
            break

    if package and package_keywords:
        haystack = f"{headings[idx]['text']}".lower()
        if not any(keyword in haystack for keyword in package_keywords):
            package = ""
    elif package and package_marker_idx >= 0:
        story_markers_between = 0
        for k in range(package_marker_idx + 1, idx):
            if heading_is_story_marker(headings[k]):
                story_markers_between += 1
        if story_markers_between > 4:
            package = ""
    return favorite_by, exclude_repeats, exclude_curators, package


def passes_quality_checks(headline: str, outlet: str, writer: str, summary: str) -> bool:
    if not looks_like_story_title(headline):
        return False
    if not outlet and not writer:
        return False
    if outlet and (len(outlet) > 80 or len(outlet.split()) > 10):
        return False
    if writer and (len(writer) > 90 or len(writer.split()) > 14):
        return False
    if not summary or len(summary) < 20:
        return False
    letters = [ch for ch in headline if ch.isalpha()]
    if letters:
        upper_ratio = sum(1 for ch in letters if ch.isupper()) / len(letters)
        if upper_ratio > 0.75 and len(headline.split()) <= 8:
            return False
    return True


def summary_from_nearby(headings: list[dict[str, str]], start: int, stop: int) -> str:
    for j in range(start, min(stop, len(headings))):
        text = headings[j]["text"]
        level = headings[j]["level"]
        if level not in {"3", "4"}:
            continue
        lower = text.lower()
        if looks_like_summary_noise(text):
            continue
        if BYLINE_RE.match(text):
            continue
        if re.match(r"^~?\d+\s*(minutes?|mins?)$", lower):
            continue
        if len(text) < 45:
            continue
        if " by " in lower and " for " in lower and len(text) < 180:
            continue
        sentence = first_sentence(text)
        if sentence and 40 <= len(sentence) <= 340:
            return sentence
    return ""


def summary_from_block(block_html: str, headline: str) -> str:
    cleaned = re.sub(r"(?is)<script.*?</script>", " ", block_html)
    cleaned = re.sub(r"(?is)<style.*?</style>", " ", cleaned)
    cleaned = re.sub(r"(?is)<!--.*?-->", " ", cleaned)
    cleaned = re.sub(r"(?i)<br\\s*/?>", "\n", cleaned)
    cleaned = re.sub(r"(?i)</(p|div|h[1-6]|li|tr|td|blockquote)>", "\n", cleaned)
    cleaned = re.sub(r"(?i)<(p|div|h[1-6]|li|tr|td|blockquote)[^>]*>", "", cleaned)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    lines = [clean_text(line) for line in cleaned.splitlines()]
    headline_key = normalize_compare_key(headline)
    for idx, line in enumerate(lines):
        if not line:
            continue
        if looks_like_summary_noise(line):
            continue
        if BYLINE_RE.match(line):
            continue
        if re.match(r"^~?\d+\s*(minutes?|mins?)$", line.lower()):
            continue
        if normalize_compare_key(line) == headline_key:
            continue
        if len(line) < 45:
            continue
        combined = line
        if not re.search(r"[.!?]$", combined):
            for look_ahead in range(idx + 1, min(idx + 3, len(lines))):
                nxt = lines[look_ahead]
                if not nxt or looks_like_summary_noise(nxt):
                    continue
                if BYLINE_RE.match(nxt):
                    continue
                if len(nxt) < 20:
                    continue
                combined = f"{combined} {nxt}".strip()
                if re.search(r"[.!?]", combined):
                    break
        sentence = first_sentence(combined)
        if 40 <= len(sentence) <= 340:
            return sentence
    return ""


def outlet_from_url(url: str) -> str:
    if not url:
        return ""
    host = urlparse(url).netloc.lower()
    host = host.replace("www.", "")
    parts = host.split(".")
    if len(parts) >= 2:
        return parts[-2].replace("-", " ").title()
    return host.title()


def extract_recommendations(issue: Issue, html_text: str) -> list[dict[str, str]]:
    headings = parse_headings(html_text)
    images = parse_images(html_text)
    favorite_overrides = infer_favorites_from_bios(headings)
    items: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str, str]] = set()

    def add_item(
        headline: str,
        writer: str,
        outlet: str,
        summary: str,
        story_url: str,
        source: str,
        lead_image: str = "",
        favorite_by: str = "",
        exclude_repeats: bool = False,
        exclude_curators: bool = False,
        package: str = "",
    ) -> None:
        headline = normalize_headline(headline)
        writer = writer.strip()
        outlet = outlet.strip()
        summary = first_sentence(summary)
        if not outlet:
            outlet = outlet_from_url(story_url)
        if should_exclude_item(headline, outlet, summary, story_url, exclude_repeats, exclude_curators):
            return
        if not passes_quality_checks(headline, outlet, writer, summary):
            return
        topic = infer_topic(headline, outlet, summary)
        key = (issue.url, headline.lower(), writer.lower(), outlet.lower())
        if key in seen_keys:
            return
        seen_keys.add(key)
        items.append(
            {
                "issueDate": issue.date,
                "issueTitle": issue.title,
                "issueUrl": issue.url,
                "headline": headline,
                "outlet": outlet,
                "writer": writer,
                "topic": topic,
                "summary": summary,
                "url": story_url or issue.url,
                "leadImage": lead_image,
                "isFavorite": bool(favorite_by),
                "favoriteBy": favorite_by,
                "package": package,
                "sourceFormat": source,
            }
        )

    # Newer template pass: h1 (headline) + nearby h2/h4 (byline) + h3/h4 (summary)
    for i, h in enumerate(headings):
        if h["level"] != "1":
            continue
        title = normalize_headline(h["text"])
        if not looks_like_story_title(title):
            continue
        story_url = choose_link(h["raw"])
        writer = ""
        outlet = ""
        summary = ""
        favorite_by, exclude_repeats, exclude_curators, package = section_context_for_index(headings, i)
        if not favorite_by:
            favorite_by = favorite_overrides.get(i, "")

        # Sometimes the h1 itself contains ", by ..."
        by_in_title = re.search(r",\s*by\s+(.+)$", title, flags=re.IGNORECASE)
        if by_in_title:
            writer = by_in_title.group(1).strip()
            title = re.sub(r",\s*by\s+.+$", "", title, flags=re.IGNORECASE).strip()

        next_h1 = next((idx for idx in range(i + 1, len(headings)) if headings[idx]["level"] == "1"), len(headings))
        next_idx = i + 1
        for j in range(i + 1, min(i + 6, next_h1)):
            next_text = headings[j]["text"]
            parsed_writer, parsed_outlet = parse_byline_text(next_text)
            if parsed_writer:
                writer, outlet = parsed_writer, parsed_outlet
                if not story_url:
                    story_url = choose_link(headings[j]["raw"])
                next_idx = j + 1
                break

            # Fallback for lines that combine title + byline
            t2, w2, o2 = parse_inline_story_line(next_text)
            if t2 and w2 and o2 and t2.lower() == title.lower():
                writer, outlet = w2, o2
                if not story_url:
                    story_url = choose_link(headings[j]["raw"])
                next_idx = j + 1
                break

        summary = summary_from_nearby(headings, next_idx, next_h1)
        if not summary:
            block_start = int(h["end"])
            block_end = int(headings[next_h1]["start"]) if next_h1 < len(headings) else len(html_text)
            summary = summary_from_block(html_text[block_start:block_end], title)

        previous_h1 = next((idx for idx in range(i - 1, -1, -1) if headings[idx]["level"] == "1"), -1)
        previous_start = int(headings[previous_h1]["start"]) if previous_h1 >= 0 else 0
        lead_image = choose_story_image(images, previous_start, int(h["start"]))

        add_item(
            title,
            writer,
            outlet,
            summary,
            story_url,
            "h1-sequence",
            lead_image,
            favorite_by,
            exclude_repeats,
            exclude_curators,
            package,
        )

    # Older template pass: one line often contains title + byline + source.
    for i, h in enumerate(headings):
        if h["level"] not in {"2", "4"}:
            continue
        text = h["text"]
        title, writer, outlet = parse_inline_story_line(text)
        if not title:
            continue
        story_url = choose_link(h["raw"])
        summary = ""
        favorite_by, exclude_repeats, exclude_curators, package = section_context_for_index(headings, i)
        if not favorite_by:
            favorite_by = favorite_overrides.get(i, "")
        minute_split = re.split(r"\(~?\d+.*?\)\s*", text, maxsplit=1)
        if len(minute_split) == 2 and minute_split[1].strip():
            summary = minute_split[1].strip()
        if not summary:
            next_h1 = next((idx for idx in range(i + 1, len(headings)) if headings[idx]["level"] == "1"), len(headings))
            summary = summary_from_nearby(headings, i + 1, next_h1)
        add_item(
            title,
            writer,
            outlet,
            summary,
            story_url,
            "inline-byline",
            "",
            favorite_by,
            exclude_repeats,
            exclude_curators,
            package,
        )

    # Legacy template fallback for early issues using numbered plain-text blocks.
    if len(items) < 8:
        for legacy in parse_legacy_numbered_items(html_text):
            add_item(
                legacy["headline"],
                legacy["writer"],
                legacy["outlet"],
                legacy["summary"],
                legacy["url"],
                "legacy-numbered",
                "",
                "",
                False,
                False,
                "",
            )

    return items


def run(
    docx_path: Path,
    output_path: Path,
    cache_dir: Path | None,
    lead_image_source: str,
    article_image_cache: Path | None,
    article_image_workers: int,
    article_image_timeout: float,
) -> None:
    issues = parse_docx_issues(docx_path)
    all_items: list[dict[str, str]] = []
    failed: list[str] = []

    for idx, issue in enumerate(issues, start=1):
        try:
            html_text = fetch_issue_html(issue.url, cache_dir)
            items = extract_recommendations(issue, html_text)
            all_items.extend(items)
            print(f"[{idx:03d}/{len(issues)}] {issue.date}: {len(items)} items")
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{issue.date} {issue.url} :: {exc}")
            print(f"[{idx:03d}/{len(issues)}] FAILED {issue.url} :: {exc}")

    if lead_image_source == "article":
        default_cache = (cache_dir / "article-image-cache.json") if cache_dir else (
            output_path.parent / ".cache" / "article-image-cache.json"
        )
        cache_file = article_image_cache or default_cache
        with_image, resolved = enrich_with_article_images(
            all_items,
            cache_file=cache_file,
            workers=article_image_workers,
            timeout=article_image_timeout,
        )
        print(f"Article image matches: {with_image}/{len(all_items)} items ({resolved} unique URLs)")
    elif lead_image_source == "none":
        for item in all_items:
            item["leadImage"] = ""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(all_items, indent=2, ensure_ascii=False), encoding="utf-8")

    print("")
    print(f"Issues parsed: {len(issues)}")
    print(f"Recommendations extracted: {len(all_items)}")
    print(f"Failed issues: {len(failed)}")
    if failed:
        (output_path.parent / "failed_issues.txt").write_text("\n".join(failed), encoding="utf-8")
        print(f"Failure log written: {output_path.parent / 'failed_issues.txt'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docx", type=Path, required=True, help="Path to the SLR archive DOCX.")
    parser.add_argument("--output", type=Path, required=True, help="Path to output JSON.")
    parser.add_argument("--cache-dir", type=Path, default=None, help="Optional cache directory for downloaded issue HTML.")
    parser.add_argument(
        "--lead-image-source",
        choices=("article", "newsletter", "none"),
        default="article",
        help="Where to pull lead images from: linked article metadata, newsletter layout, or none.",
    )
    parser.add_argument(
        "--article-image-cache",
        type=Path,
        default=None,
        help="Optional path to article image cache JSON file (used with --lead-image-source article).",
    )
    parser.add_argument(
        "--article-image-workers",
        type=int,
        default=12,
        help="Concurrent worker count for article image lookups.",
    )
    parser.add_argument(
        "--article-image-timeout",
        type=float,
        default=12.0,
        help="Timeout in seconds for each article image lookup request.",
    )
    args = parser.parse_args()

    run(
        args.docx,
        args.output,
        args.cache_dir,
        args.lead_image_source,
        args.article_image_cache,
        args.article_image_workers,
        args.article_image_timeout,
    )


if __name__ == "__main__":
    main()
