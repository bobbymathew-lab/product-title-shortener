# Product Title Shortener

A local Python tool I built to clean 155,000+ Amazon SEO-inflated product titles into short, readable ecommerce titles — without paying a single API call.

---

## The Problem

Large ecommerce catalogues imported from Amazon tend to carry bloated, keyword-stuffed titles like:

> *U.S. Art Supply 23" x 26" Artist Sketch Tote Board - Lightweight Hardboard Drawing Board with Handle, Clip & Rubber Band for Art, Sketching, Travel*

These titles are optimised for Amazon's search algorithm, not for clean product pages. When you're managing a Shopify or custom ecommerce site, they create inconsistent UX, poor scannability, and clutter that affects on-site search and page readability.

Manually editing 156,017 rows wasn't an option. Sending them all through a paid LLM API would have been expensive. So I built a local batch processor using a free, locally-run LLM instead.

---

## What It Does

- Reads product titles from an `.xlsx` file
- Sends them in batches to a locally-running LLM (via Ollama)
- Applies a consistent set of shortening rules
- Outputs clean titles to a new `.xlsx` file

**Shortening rules applied:**
- Maximum 8 words
- Keep brand name, key descriptor, and product type
- Remove slash-separated keyword stuffing
- Remove repetitive colour/material variants
- Keep important specs (size, quantity, model number where relevant)
- Use Title Case throughout
- Output as a clean JSON array per batch

**Example output:**

| Before | After |
|---|---|
| U.S. Art Supply 23" x 26" Artist Sketch Tote Board - Lightweight Hardboard Drawing Board with Handle, Clip & Rubber Band for Art, Sketching, Travel | U.S. Art Supply 23" Sketch Tote Board |

---

## Why Local LLM

Running this through OpenAI or Anthropic's API at 156k rows would cost money and introduce rate limit complexity. Ollama lets you run an LLM entirely on your own machine — free, offline, and without data leaving your system. For a bulk cleaning task like this, it's the right tool.

---

## Tech Stack

| Component | Detail |
|---|---|
| Language | Python 3.x |
| LLM Runtime | [Ollama](https://ollama.com) |
| Model | `llama3.2:3b` |
| Input/Output | `.xlsx` via `openpyxl` |
| Concurrency | `ThreadPoolExecutor` (3 workers) |
| Batch size | 20 titles per request |
| Retry logic | 3 retries per failed batch |

---

## Setup Guide

### Prerequisites

- Windows 10/11 (guide written for Windows; adaptable to Mac/Linux)
- Python 3.8 or later
- Ollama installed and running
- ~4GB VRAM if running on GPU (CPU fallback works, but slower)

---

### Step 1 — Install Python

Download from [python.org](https://www.python.org/downloads/).

During installation, **tick "Add Python to PATH"** before clicking Install. This is the most common setup mistake — if you skip it, commands won't work in terminal.

Verify installation:
```bash
python --version
pip --version
```

---

### Step 2 — Install Ollama

Download from [ollama.com](https://ollama.com) and run the installer.

Once installed, pull the model this script uses:
```bash
ollama pull llama3.2:3b
```

This downloads ~2GB. Once complete, Ollama runs as a background service — no need to launch it manually each time.

Verify it's running:
```bash
ollama list
```
You should see `llama3.2:3b` in the list.

---

### Step 3 — Install Python dependencies

```bash
pip install openpyxl requests
```

---

### Step 4 — Set up your working folder

Create a folder, for example:
```
C:\Users\YourName\Desktop\TitleShortener
```

Place the following files inside it:
```
TitleShortener/
├── shorten_titles.py
├── your_product_file.xlsx
└── (optional) test_batch_50.xlsx
```

---

### Step 5 — Configure the script

Open `shorten_titles.py` and update these variables at the top of the file:

```python
INPUT_FILE = "your_product_file.xlsx"   # Your input filename
OUTPUT_FILE = "shortened_titles_output.xlsx"
TITLE_COLUMN = "Name"                   # Column header containing product titles
BATCH_SIZE = 20
MAX_WORKERS = 3
```

Make sure your input file has a column named exactly as specified in `TITLE_COLUMN`.

---

### Step 6 — Run the script

Open a terminal, navigate to your folder, then run:

```bash
cd C:\Users\YourName\Desktop\TitleShortener
python shorten_titles.py
```

The script will process titles in batches and write results to `shortened_titles_output.xlsx` in the same folder.

---

### Testing first (recommended)

Before running on your full file, test with a smaller batch:

1. Create a copy of your file with ~50 rows
2. Update `INPUT_FILE` in the script to point to the test file
3. Run and verify the output looks correct before scaling up

---

## File Structure

```
TitleShortener/
├── shorten_titles.py          # Main script
├── your_product_file.xlsx     # Input (not included — add your own)
├── shortened_titles_output.xlsx  # Generated on run
└── README.md
```

---

## Known Issues & Fixes

| Issue | Fix |
|---|---|
| `pip not recognized` | Reinstall Python and tick "Add to PATH" |
| `FileNotFoundError` | Filenames with spaces need quotes: `"fresh product file 1.xlsx"` — or rename the file to remove spaces |
| Script runs but nothing happens | Make sure terminal is `cd`'d into the correct folder before running |
| Slow processing | Normal on CPU — the 3b model on a mid-range machine processes roughly 20 titles per batch |

---

## Hardware Used

Built and tested on:
- Intel Core i5-11300H
- 16GB RAM
- NVIDIA RTX 3050 4GB VRAM
- Windows 11

The 3b model runs comfortably within 4GB VRAM. If you're on CPU-only, it'll work — just slower.

---

## Context

This was built as part of a real content operations problem while managing large-scale ecommerce catalogues. The same pattern applies to any bulk title or copy normalisation task where you have structured data and a consistent transformation rule.

---

## License

MIT — use it, adapt it, build on it.
