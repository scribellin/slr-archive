# Newsletter Story Hub

Searchable archive of individual recommendations from your Sunday Long Read newsletter history.

## Included fields

Each story record includes:

- `headline`
- `outlet`
- `writer`
- `topic` (inferred)
- `summary` (one sentence)
- `leadImage` (when available)
- `isFavorite` and `favoriteBy` (when a story appears in favorite-pick sections)
- `package` (for grouped sections like `SLR Syllabus` / `The Locals`)
- `url`
- `issueDate`, `issueTitle`, `issueUrl`

## Run locally

Because the app fetches `stories.json`, run a local server:

```bash
python3 -m http.server 8000
```

Then open [http://localhost:8000](http://localhost:8000).

## Refresh from your DOCX archive

```bash
python3 scripts/extract_slr_recommendations.py \
  --docx "/Users/jacobfeldman/Downloads/full slr archive.docx" \
  --output "stories.json" \
  --cache-dir "/tmp/slr-archive-cache" \
  --lead-image-source article
```

Notes:

- First run downloads all linked issues and may take several minutes.
- Re-runs are fast because HTML is cached.
- With `--lead-image-source article`, the script fetches each story page and uses metadata (`og:image` / `twitter:image`) for `leadImage`.
- Article-image lookups are cached to `.cache/article-image-cache.json` (or under `--cache-dir`) so future runs are much faster.
- If you want to skip article-image lookups and keep newsletter images, use `--lead-image-source newsletter`.
- Sponsored blocks, staff-curator blocks, and "last week's most reads" repeats are excluded.
- Old templates vary a lot, so extraction quality is best from mid-2016 onward.

## Publish permanently with GitHub Pages

This repo already includes a Pages deploy workflow at:

- `.github/workflows/deploy-pages.yml`

Steps:

1. Create an empty GitHub repo (for example `newsletter-story-hub`).
2. Connect this local repo to GitHub and push:

```bash
git remote add origin https://github.com/<your-username>/<your-repo>.git
git add .
git commit -m "Initial story hub"
git push -u origin main
```

3. In GitHub: `Settings` → `Pages` → `Source` = `GitHub Actions`.
4. Wait for the `Deploy To GitHub Pages` workflow to finish.
5. Your permanent URL will be:

`https://<your-username>.github.io/<your-repo>/`
