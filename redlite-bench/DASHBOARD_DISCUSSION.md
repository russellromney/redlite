# Dashboard Strategy Discussion

## Goals & Constraints

**Goal**: Visualize benchmark results from a single run as an artifact
**Constraints**:
- No historical tracking needed
- No database required
- Minimal complexity
- Easy to view/share

## Three Approaches

### Approach A: HTML Dashboard (Recommended)

**What**: Generate a single self-contained HTML file with embedded JSON data and visualization

**Architecture**:
```
JSON Report -> Dashboard Generator -> Single HTML File (with embedded JSON + JS)
                                    ↓
                        Interactive visualization in browser
                        (charts.js, simple tables, filtering)
```

**Pros**:
- Single file artifact = easy to share/upload
- No backend needed
- Works offline in any browser
- Can customize charts/styling easily
- HTML/JS libraries are stable and well-known
- Fast load (no network calls)

**Cons**:
- Requires HTML/JS knowledge
- File size potentially large with big datasets (but still manageable)

**Implementation**:
```rust
// In report_generator.rs
pub fn generate_html_dashboard(report: &BenchmarkReport) -> String {
    let json_data = serde_json::to_string(report)?;
    format!(r#"
    <!DOCTYPE html>
    <html>
    <head>
        <title>Redlite Benchmark Report</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>{css}</style>
    </head>
    <body>
        <div id="app"></div>
        <script>
            const data = {json_data};
            // Render charts, tables, etc.
        </script>
    </body>
    </html>
    "#)
}
```

**Visualizations**:
- Throughput comparison bars (Redis vs Redlite)
- Latency heatmap or percentile charts
- Scenario summary cards
- "Winner" badges
- Latency distribution histograms

**File size**: ~500KB-2MB depending on data (charts.js is ~100KB)

---

### Approach B: Markdown Report (Already Done)

**What**: We already have this working

**Pros**:
- Already implemented
- Works on GitHub/GitLab natively
- Human-readable
- Text-based, tiny file size

**Cons**:
- No interactivity
- Hard to visualize distributions
- Tables don't scale well with many scenarios

**Status**: ✅ Complete

---

### Approach C: PDF Report

**What**: Generate a styled PDF from the report data

**Pros**:
- Professional looking
- Print-friendly
- Good for archiving
- Self-contained

**Cons**:
- Requires PDF library (adds dependency)
- No interactivity
- Larger file than markdown
- PDF rendering can be finicky

**Not recommended** for this use case.

---

## Recommended Path: A + B (HTML + Markdown)

**Keep markdown reports** for:
- Easy viewing on GitHub
- Quick reference
- CI/CD log outputs

**Add HTML dashboard** for:
- Detailed analysis
- Visual comparisons
- Interactive filtering
- Professional presentation

**Workflow**:
```
Benchmark Run
    ↓
JSON Report Generated
    ↓
├─→ Markdown Report (for GitHub)
├─→ HTML Dashboard (for detailed view)
└─→ Artifacts Uploaded (both)
```

**CLI Integration**:
```bash
./redlite-bench run-benchmarks \
  --iterations 10000 \
  --report-format markdown \
  --dashboard                        # New flag
  --report-file report.md            # Still generates this
  --dashboard-file dashboard.html    # Plus this
```

---

## HTML Dashboard Details

### Key Metrics Visualization

**1. Throughput Comparison**
```
┌─────────────────────────────────────────┐
│ get_only                                │
│ Redis: 2,352 ops/sec                    │
│ Redlite: 208,685 ops/sec (+8770%)       │
│                                         │
│ ■■■■■■■■■■ Redis (small bar)          │
│ ■■■■■■■■■■■■■■■■■■■■■■■■... Redlite  │
└─────────────────────────────────────────┘
```

**2. Latency Distribution (P50, P95, P99)**
```
Line chart showing:
- Redis: 380-410 µs across scenarios
- Redlite: 3-25 µs across scenarios
- Clear visual gap showing advantage
```

**3. Scenario Cards**
```
┌────────────────────────────┐
│ read_heavy                 │
│ 80% read, 20% write        │
│                            │
│ Redis: 2,282 ops/sec       │
│ Latency P50: 406 µs        │
│                            │
│ Redlite: 90,571 ops/sec    │
│ Latency P50: 4.6 µs        │
│                            │
│ Winner: Redlite ✓          │
│ Improvement: +3868%        │
└────────────────────────────┘
```

**4. Summary Section**
```
Total Scenarios: 32
Redis Completed: 32
Redlite Completed: 32

Redlite Faster: 31 scenarios (97%)
Redis Faster: 1 scenario (3%)

Average Improvement: 3,847%
```

### Interactive Features

**Filtering**:
- Filter by scenario type (core, stress, specialized, etc.)
- Show/hide specific metrics (P50, P95, P99, etc.)

**Export**:
- Download individual scenario data as CSV
- Copy metrics to clipboard

**Sorting**:
- Sort scenarios by improvement
- Sort by throughput
- Sort by latency

---

## Implementation Estimate

**If we go with HTML Dashboard**:

1. **Dashboard module** (`src/dashboard.rs`): ~150-200 lines
   - Generates HTML template
   - Embeds JSON data
   - Includes minimal CSS/JS

2. **CLI integration** in `main.rs`: ~30-50 lines
   - New `--dashboard` flag
   - Call dashboard generation function

3. **Testing**: ~20-30 lines
   - Verify HTML generation
   - Check embedded data is valid

**Total**: ~200-280 lines of code
**Time**: ~2-3 hours

---

## Technical Questions to Consider

1. **Charts Library**:
   - Option A: Chart.js (100KB CDN, mature, good documentation)
   - Option B: Plotly.js (~3MB, more features)
   - Option C: Simple SVG generation (no dependency, more manual work)
   - **Recommendation**: Chart.js (best balance)

2. **Styling**:
   - Inline CSS in HTML (single file, ~2KB)
   - Dark mode toggle (1KB additional)
   - Responsive design for mobile/tablet

3. **File Size**:
   - Base HTML/CSS/JS: ~50KB
   - Charts library (CDN): ~100KB
   - Embedded JSON: varies (10KB-500KB depending on scenarios and data points)
   - **Total**: ~160KB-650KB (acceptable artifact size)

4. **Data Limits**:
   - 32 scenarios × 1000 latency samples each = ~32MB of raw data
   - Solution: Store aggregated stats only (percentiles, not every latency point)
   - Result: ~50-100 data points per scenario instead of 1000+ = ~100KB-200KB JSON

---

## Recommendation: Hybrid Approach

**Generate both automatically**:
- **Markdown** report: Human-readable, GitHub-native, quick reference
- **HTML Dashboard**: Detailed visualization, interactive, professional

**CLI**:
```bash
./redlite-bench run-benchmarks \
  --iterations 10000 \
  --dashboard              # Enables HTML generation
  --report-format markdown # Still generates markdown
```

**Outputs**:
- `report.md` (always)
- `dashboard.html` (if --dashboard flag)
- `report.json` (always, for tools)

**GitHub Actions**:
- Upload both as artifacts
- Link to HTML in PR comment
- Display markdown summary

---

## Next Steps

1. **Decide**: HTML dashboard yes/no? If yes, Chart.js or other?
2. **Design**: Pick which metrics to visualize (throughput, latency, percentiles)
3. **Build**: Create `src/dashboard.rs` module
4. **Test**: Verify HTML generation and data embedding
5. **Integrate**: Add CLI flags and GitHub Actions step

**Estimated time**: 2-3 hours for full implementation

---

## Questions for You

1. Do you want the HTML dashboard?
2. Which visualization approach appeals more?
3. Any specific metrics you want highlighted?
4. Should we embed the JSON in HTML or keep separate files?
5. Should markdown and HTML both be generated by default or is --dashboard flag fine?

