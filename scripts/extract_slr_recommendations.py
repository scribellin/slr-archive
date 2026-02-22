#!/usr/bin/env python3
"""Extract individual SLR recommendations from a DOCX archive list."""

from __future__ import annotations

import argparse
import html
import json
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlparse
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
    "members",
    "staff ",
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


@dataclass
class Issue:
    date: str
    title: str
    url: str


def clean_text(raw: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", raw)
    decoded = html.unescape(no_tags).replace("\xa0", " ")
    return " ".join(decoded.split()).strip()


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
        merged = clean_text(f"{text} {link_text}")
        date_match = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", merged)
        if not date_match:
            continue
        date = datetime.strptime(date_match.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
        title = re.sub(r"^\d{2}/\d{2}/\d{4}\s*-\s*", "", merged).strip()
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


def parse_headings(html_text: str) -> list[dict[str, str]]:
    headings: list[dict[str, str]] = []
    for match in re.finditer(r"<h([1-4])[^>]*>(.*?)</h\1>", html_text, flags=re.IGNORECASE | re.DOTALL):
        level = int(match.group(1))
        raw = match.group(2)
        text = clean_text(raw)
        if text:
            headings.append({"level": str(level), "raw": raw, "text": text})
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


def summary_from_nearby(headings: list[dict[str, str]], start: int) -> str:
    for j in range(start, min(start + 5, len(headings))):
        text = headings[j]["text"]
        lower = text.lower()
        if looks_like_noise(text):
            continue
        if BYLINE_RE.match(text):
            continue
        if " by " in lower and " for " in lower and len(text) < 180:
            continue
        sentence = first_sentence(text)
        if sentence and len(sentence) >= 40:
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
    items: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str, str]] = set()

    def add_item(headline: str, writer: str, outlet: str, summary: str, story_url: str, source: str) -> None:
        headline = normalize_headline(headline)
        writer = writer.strip()
        outlet = outlet.strip()
        summary = first_sentence(summary)
        if not summary:
            summary = f"Recommended in {issue.title}."
        if not outlet:
            outlet = outlet_from_url(story_url)
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

        # Sometimes the h1 itself contains ", by ..."
        by_in_title = re.search(r",\s*by\s+(.+)$", title, flags=re.IGNORECASE)
        if by_in_title:
            writer = by_in_title.group(1).strip()
            title = re.sub(r",\s*by\s+.+$", "", title, flags=re.IGNORECASE).strip()

        next_idx = i + 1
        for j in range(i + 1, min(i + 5, len(headings))):
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

        summary = summary_from_nearby(headings, next_idx)
        add_item(title, writer, outlet, summary, story_url, "h1-sequence")

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
        minute_split = re.split(r"\(~?\d+.*?\)\s*", text, maxsplit=1)
        if len(minute_split) == 2 and minute_split[1].strip():
            summary = minute_split[1].strip()
        if not summary:
            summary = summary_from_nearby(headings, i + 1)
        add_item(title, writer, outlet, summary, story_url, "inline-byline")

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
            )

    return items


def run(docx_path: Path, output_path: Path, cache_dir: Path | None) -> None:
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
    args = parser.parse_args()

    run(args.docx, args.output, args.cache_dir)


if __name__ == "__main__":
    main()
