# PLA Part 2 — Publishing Guide

A step-by-step sequence to publish Part 2 across GitHub, Medium, and LinkedIn without missing anything.

---

## Files in this package

| File | Where it lives | What it is |
|---|---|---|
| `pla-part2-cdv-principle.html` | GitHub repo root | The full Part 2 article (HTML, renders on GitHub Pages) |
| `blueprints/PLA-OBP-001-v01-virtualize-progressively.py` | GitHub `blueprints/` folder | Updated standalone blueprint code, matches the article |
| `pla-part2-medium-article.md` | Paste into Medium + commit to GitHub root | The Medium article (3 paragraphs + bio, links to GitHub) |
| `pla-part2-linkedin-post.md` | Paste into LinkedIn + commit to GitHub root | The LinkedIn post (text + hashtags + image guidance) |
| `README.md` | GitHub repo root | Upload manifest documenting all changes |
| `METAllion_-_PLA_Reference_Architecture_-CDV.png` | LinkedIn post image (your file, 20% fade version) | The architecture diagram with CDV bar highlighted |

---

## Phase 1 — GitHub

### Step 1.1 — Replace the HTML article

1. Go to https://github.com/habburu/pragmaticlakehouse
2. Click on `pla-part2-cdv-principle.html` in the file list
3. Click the pencil icon (Edit this file) in the top right
4. Select all existing content (`Ctrl+A` / `Cmd+A`) and delete it
5. Open the new `pla-part2-cdv-principle.html` from your downloaded package
6. Copy all of its content
7. Paste into the GitHub editor
8. Scroll down to "Commit changes"
9. Use commit message: `Part 2: revisions to Live Federation, Governed Replication, OSI framing, and closing paragraph`
10. Click "Commit changes"

### Step 1.2 — Replace the blueprint Python file

1. Navigate to the `blueprints/` folder in the repo
2. Click on `PLA-OBP-001-v01-virtualize-progressively.py`
3. Click the pencil icon (Edit this file)
4. Select all and delete
5. Open the new `PLA-OBP-001-v01-virtualize-progressively.py` from your downloaded package
6. Copy all content
7. Paste into the GitHub editor
8. Commit message: `Update blueprint code to match Part 2 article revisions`
9. Click "Commit changes"

### Step 1.3 — Add the Medium and LinkedIn markdown files to the repo

These are new files. Adding them gives anyone browsing the repo access to the same announcements you posted on Medium and LinkedIn.

1. From the repo root, click "Add file" > "Create new file"
2. Name it: `pla-part2-medium-article.md`
3. Paste the content from your downloaded `pla-part2-medium-article.md`
4. Commit message: `Add Medium article markdown for Part 2`
5. Click "Commit new file"
6. Repeat steps 1–5 for `pla-part2-linkedin-post.md`
7. Repeat for `README.md` if you want the upload manifest in the repo (optional)

### Step 1.4 — Verify GitHub Pages renders correctly

1. Wait 1–2 minutes for GitHub Pages to rebuild
2. Open this URL in a private/incognito browser window:
   https://habburu.github.io/pragmaticlakehouse/pla-part2-cdv-principle.html
3. Scroll through the whole article. Specifically check:
   - The opening paragraph mentions METAllion™ with the TM symbol
   - The Virtualize subsection ends with the OSI paragraph (no separate OSI section)
   - Blueprint 1: Live Federation code block shows the Fabric Direct Query to Snowflake example (TMSL JSON), NOT the old "CREATE SHARE" Snowflake Delta Sharing example
   - Blueprint 1: Governed Replication shows TWO options (Option A — native mirroring, Option B — Spark notebook ETL)
   - Blueprint 1: Bottom Line says "Live federation second — protocol-level, query-time access" (not "zero movement")
   - Blueprint 1: Shortcuts narrative mentions Snowflake-managed Iceberg via OneLake shortcut
   - The closing paragraph ends with "The next installment in the series will continue along the same path..."
4. If anything looks wrong, fix it in GitHub before publishing on Medium and LinkedIn

---

## Phase 2 — Medium

### Step 2.1 — Start a new Medium story

1. Go to https://medium.com/new-story while signed in as @hnabburu
2. The title field is at the top — type:
   `CDV Principle — A Deep Dive (PLA Part 2)`
3. Click the subtitle area (just below the title) and type:
   `Converge, Diverge, Virtualize — from concept to implementation`
   Then highlight the subtitle text and apply italic (`Ctrl+I` / `Cmd+I`)

### Step 2.2 — Paste the body content

1. Open `pla-part2-medium-article.md` from your downloaded package
2. Skip the first three lines (`# CDV Principle...`, the tagline, and `---`) — you already typed the title and subtitle above
3. Start copying from the line beginning with `[Part 1 of this series](...)` to the very end of the file
4. Paste into the Medium editor

### Step 2.3 — Fix the formatting Medium drops on paste

Medium does NOT preserve markdown bold/italic formatting from a paste. You'll need to apply formatting manually:

1. **Section dividers (`---`):** Medium has its own divider. Place your cursor on an empty line, click the "+" icon that appears in the left margin, and choose the line-divider option. Do this in three places where the `---` markers were in the file.

2. **Convert markdown link to a Medium link:**
   - Find the text `[Part 1 of this series](https://medium.com/@hnabburu/pragmatic-lakehouse-architecture-6029ed4035a4)`
   - Delete the brackets and parentheses-URL, leaving only the text `Part 1 of this series`
   - Highlight `Part 1 of this series`
   - Press `Ctrl+K` / `Cmd+K`
   - Paste: `https://medium.com/@hnabburu/pragmatic-lakehouse-architecture-6029ed4035a4`
   - Press Enter

3. **Bold text:** Highlight any word/phrase that was wrapped in `**...**` in the markdown — specifically `Converge.`, `Diverge.`, `Virtualize.` — and press `Ctrl+B` / `Cmd+B`. Remove the `**` characters around them.

4. **Italic text:** The author bio paragraph at the bottom and the *Virtualize Progressively*, *Governing the Zone Boundaries*, *Classify Once. Enforce Everywhere.* phrases need italics. Highlight each and press `Ctrl+I` / `Cmd+I`. Remove the `*` markers from the visible text.

5. **Author bio block quote:** The bio paragraph (starts with "Hari Abburu — A senior data...") should be formatted as a block quote. Place cursor in that paragraph, then click the quotation-mark icon in Medium's floating toolbar, or use `Ctrl+Alt+5`.

6. **Two clickable hyperlinks in the link section:** For each of the GitHub Pages link and GitHub repo link:
   - Highlight the URL text
   - Press `Ctrl+K` / `Cmd+K`
   - Paste the same URL in the link box, press Enter
   - This makes the URL itself clickable

### Step 2.4 — Add tags before publishing

1. Click "Publish" in the top right (it doesn't publish immediately)
2. Medium will ask you to add up to 5 tags
3. Use these:
   - `Data Architecture`
   - `Data Engineering`
   - `Data Mesh`
   - `Lakehouse`
   - `Data Governance`

### Step 2.5 — Publish

1. Review the preview Medium shows
2. Click "Publish now"
3. **Copy the URL** of the published story (you'll need it for LinkedIn)

---

## Phase 3 — LinkedIn

### Step 3.1 — Save the LinkedIn image

1. Locate your file `METAllion_-_PLA_Reference_Architecture_-CDV.png` — the 20% fade version where the CDV bar is the visual anchor and the rest of the diagram is dimmed
2. Have it ready to attach

### Step 3.2 — Update the LinkedIn post with the Medium URL

1. Open `pla-part2-linkedin-post.md` from your downloaded package
2. Find the line `*[paste Medium link once published]*`
3. Replace it with the actual Medium URL you copied in Step 2.5

### Step 3.3 — Create the LinkedIn post

1. Go to https://www.linkedin.com and sign in
2. Click "Start a post" at the top of your feed
3. Open `pla-part2-linkedin-post.md` from your downloaded package
4. Skip the first line (`# LinkedIn Post — PLA Part 2 Launch`) — this is a heading for the file, not part of the post
5. Skip the last section that begins `**Image:** Use the PLA Reference Architecture...` — this is your note to yourself
6. Copy from the line beginning `The **Pragmatic Lakehouse Architecture**...` through the hashtag line at the end
7. Paste into the LinkedIn post composer

### Step 3.4 — Apply bold formatting manually

LinkedIn doesn't render markdown either. Apply bold using LinkedIn's built-in formatter:

1. Highlight any text that was wrapped in `**...**` in the source file. Specifically these phrases need bolding:
   - `Pragmatic Lakehouse Architecture` (first paragraph)
   - `Part 2 — CDV Principle: A Deep Dive.`
   - `Converge → Diverge → Virtualize.`
   - `Converge`, `Diverge`, `Virtualize` (each one separately, as the labels for the three paragraphs)
   - `Read the full article on GitHub Pages (works everywhere, no paywall):`
   - `Or on Medium:`
2. For each highlighted phrase, press `Ctrl+B` / `Cmd+B` to apply bold
3. Delete the `**` characters that are still visible in the text (LinkedIn doesn't auto-strip them)

### Step 3.5 — Attach the image

1. In the post composer, click the image icon (looks like a small picture)
2. Select the 20% fade architecture diagram
3. Wait for the upload to complete

### Step 3.6 — Final review before posting

Check these specifically:
- Both URLs (GitHub Pages and Medium) appear as clickable links — LinkedIn auto-detects URLs on a separate line
- All bold formatting is applied (no stray `**` characters)
- The image preview shows the architecture diagram with the CDV bar as the visible anchor
- Hashtags appear in blue and are clickable

### Step 3.7 — Time and publish

1. **Best time:** Tuesday, Wednesday, or Thursday between 9:00 AM and 11:00 AM India time
2. Click "Post"
3. **Stay near LinkedIn for the next 90 minutes** and reply to every comment within that window
4. This single behavior is the biggest reach multiplier on LinkedIn

---

## Phase 4 — Post-launch

### Hour 1
- Reply to every comment
- Repost or share to any relevant LinkedIn groups you belong to

### Day 1
- Reply to all comments
- If anyone reaches out via DM about contributing a blueprint, log their name and topic

### Week 1
- Note engagement metrics: impressions, reactions, comments, click-through to GitHub Pages
- Check Medium read count
- Use this data to inform Part 3 messaging

---

## Common mistakes to avoid

1. **Don't paste markdown formatting** into Medium or LinkedIn expecting it to render — neither platform reads markdown. Apply bold/italic with the platform's own keyboard shortcuts after pasting.
2. **Don't forget to test the GitHub Pages link in incognito** — your browser may cache an older version, hiding rendering issues from you.
3. **Don't publish LinkedIn before Medium** — you'll have an empty placeholder where the Medium URL should be.
4. **Don't post LinkedIn on a Friday afternoon or weekend** — reach drops 60–70% versus midweek mornings.
5. **Don't edit the LinkedIn post after the first hour** — LinkedIn deprioritizes edited posts, which kills the reach you've already built.
