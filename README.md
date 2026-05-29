# PLA Part 2 — Final Upload Package

Everything you need to publish Part 2 of the Pragmatic Lakehouse Architecture series.

## What's in this package

| File | Destination | Notes |
|---|---|---|
| `pla-part2-cdv-principle.html` | GitHub repo root, replacing the existing file | The Part 2 article, with all blueprint code revisions applied |
| `blueprints/PLA-OBP-001-v01-virtualize-progressively.py` | GitHub repo `blueprints/` folder, replacing the existing file | Standalone blueprint code, kept in sync with the article |
| `pla-part2-medium-article.md` | Paste into a new Medium story; also commit to GitHub repo root for reference | 3-paragraph article linking to GitHub Pages, with contribution invitation |
| `pla-part2-linkedin-post.md` | Paste into a new LinkedIn post; also commit to GitHub repo root for reference | Hook post driving traffic to GitHub Pages and Medium |

## What changed in this revision

The article and the standalone Python blueprint both received the following revisions from the previously published Part 2:

1. **Standalone OSI section removed.** The "Platinum zone and Open Semantic Interchange" h2 section was deleted. OSI is now mentioned briefly inside the Virtualize subsection as "the architecture extends naturally to emerging open semantic standards" — proportionate to a topic worth noting without overclaiming.

2. **Platinum context layer framing added.** A new paragraph at the end of the Virtualize subsection establishes Platinum as the context layer for enterprise BI, conversational AI, agentic workflows, and APIs on its own merits — independent of OSI.

3. **Blueprint ordering note added.** One sentence in the blueprint intro confirming the three blueprints are not in any particular order and can be applied independently.

4. **Closing paragraph rewritten.** The Part 3 teaser that promised "jurisdictions, platforms, and organizations that merge before their data does" was replaced with a neutral close that leaves the next topic open.

5. **Live Federation example replaced** (Blueprint PLA-OBP-001-v01). The original "Snowflake exposing data via Delta Sharing" example was incorrect — Snowflake does not natively publish via Delta Sharing as a producer. Replaced with Fabric Platinum semantic model querying Snowflake Gold via Direct Query (TMSL connection definition). The narrative line above the code now reads "the Platinum semantic layer connects to the source platform through its native query protocol and reads at query time."

6. **Shortcuts narrative extended.** A sentence was added noting that the same Shortcuts pattern applies to Snowflake-managed Iceberg tables via OneLake shortcut with automatic Iceberg-to-Delta metadata virtualization. No additional code block — the existing Databricks-Delta example illustrates the pattern.

7. **Governed Replication broadened.** The original mirroring-only example was expanded to show two paths: Option A — native mirroring (Snowflake → Fabric Mirrored Database, existing code retained), and Option B — ETL into Fabric (new Spark notebook example reading from Snowflake and writing a physical Delta table in OneLake). The narrative line and Bottom Line were updated to reflect both paths.

8. **Bottom Line for Blueprint 1 tightened.** "Live federation second — protocol-level, zero movement" changed to "Live federation second — protocol-level, query-time access" — more honest about what Direct Query actually delivers.

## Upload sequence

1. Replace `pla-part2-cdv-principle.html` in the repo root
2. Replace `blueprints/PLA-OBP-001-v01-virtualize-progressively.py`
3. Commit and push
4. Verify the GitHub Pages render at https://habburu.github.io/pragmaticlakehouse/pla-part2-cdv-principle.html
5. Publish the Medium teaser, capture the Medium URL
6. Update the LinkedIn post draft with the Medium URL, then publish

## Posting strategy reminder

- Post LinkedIn Tuesday-Thursday morning India time
- Reply to every comment in the first 90 minutes — this is the single biggest reach multiplier for technical posts
- The opening scenario hook (eight weeks, three business units, two recognition policies) is designed to land with cold readers — resist the urge to soften it to a "proud to share" opener
