"""
Product Title Shortener using Ollama (local LLM)
-------------------------------------------------
Reads your input file (Excel or CSV), shortens Amazon-style long titles
using a local Ollama model, and outputs a CSV with original + shortened titles.

Crash-safe: progress is saved to a checkpoint file after every N batches.
If the script is interrupted, re-running it will resume from where it left off.

Requirements:
    pip install openpyxl requests

Usage:
    1. Make sure Ollama is running (it starts automatically after install)
    2. Make sure you've pulled a model: ollama pull llama3.2:3b
    3. Update INPUT_FILE and OUTPUT_FILE paths below
    4. Run: python shorten_titles.py
    5. If interrupted, just run again -- it will resume automatically
    6. To start fresh on a new file, delete the CHECKPOINT_FILE
"""

import requests
import json
import time
import csv
import os
import sys
import openpyxl
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ─────────────────────────────────────────────
# CONFIGURATION -- edit these
# ─────────────────────────────────────────────
INPUT_FILE       = "test_batch_50.xlsx"          # .xlsx or .csv input
OUTPUT_FILE      = "shortened_titles_output.csv" # output (always CSV -- handles 200k rows fine)
CHECKPOINT_FILE  = "shorten_checkpoint.json"     # resume file -- delete this to start fresh

OLLAMA_URL  = "http://localhost:11434/api/generate"
MODEL       = "llama3.2:3b"

BATCH_SIZE  = 1    # 1 title per API call -- eliminates batch offset/hallucination errors
MAX_WORKERS = 1    # single worker to match batch size of 1
RETRY_LIMIT = 3    # retries per batch before falling back to original title

# Save checkpoint to disk every N completed batches.
# Lower = safer against crashes, slightly more disk writes.
CHECKPOINT_EVERY = 5

# ─────────────────────────────────────────────
# PROMPT
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """You are a product title editor. Convert long Amazon SEO-style titles into clean, descriptive titles for a regular ecommerce website.

HARD LIMITS:
- Maximum 85 characters (including spaces). Never exceed this.
- Never invent, infer, or add any word that does not appear in the original title.
- Only use words that exist in the original -- you are editing, not rewriting.

CORE PRINCIPLE -- THE DIFFERENTIABILITY TEST:
Before dropping any attribute, ask: "Could two different versions of this product exist that differ only by this attribute?" If yes, keep it.
  - "Pampers ZZZ Size 7, 36 Count" vs "Pampers ZZZ Size 3, 20 Count" → size AND count both kept
  - "Enfamil NeuroPro Gentlease 20.5 oz" vs "Enfamil NeuroPro Gentlease 30 oz" → oz kept
  - "Garnier Nutrisse 43 Dark Golden Brown" vs "Garnier Nutrisse 5 Medium Brown" → shade code AND name both kept
  - "Nature's Bounty Vitamin C 500mg, 100 Count" vs "1000mg, 60 Count" → dosage AND count both kept
  - "iPhone 16 Pro Case" vs "iPhone 16 Plus Case" → exact device model kept

ALWAYS KEEP (if present in original):
- Brand name
- Product type / category
- Material or fabric (e.g. Silicone, Suede, Organic Cotton, Leather)
- Colour or finish in any form including parentheses (e.g. Black, Blue, Seashell, Matte, Glossy, Shimmer, Satin)
- Size, dimensions, weight, or volume (e.g. 23" x 26", 12 Inches, 8 oz, 5 qt, 23 Quart, 13.2oz) -- NEVER drop these
  NOTE: Weight in parentheses after a size number (e.g. "Size 7 (41+ lbs)") is redundant -- drop the parenthetical, keep the size.
- Count or pack size including when in parentheses (e.g. 2 Pack, 36 Count, (Pack of 6) → Pack of 6)
- Model number or named variant (e.g. S1 Plus, MT300, D10101, Stage 1, Pro, Ultra, FE, Max)
- Age or stage (e.g. 0-6 Months, Stage 1, Newborn, 0-12 Months)
- Key differentiating feature (e.g. Hypoallergenic, Organic, Slow Flow, Overnight, Grass Fed, Goat Milk, Ready-to-Use)

CATEGORY-SPECIFIC RULES (these override general rules):

HAIR DYE & COLOUR:
- Always keep shade code + shade name together (e.g. "43 Dark Golden Brown", "6.4 Copper Red", "12AA Nordic Blonde")
- Always keep colour type: Permanent, Semi-Permanent, Temporary, Demi-Permanent, Toner
- These are the primary SKU identifiers

SUPPLEMENTS & VITAMINS:
- Always keep dosage strength (e.g. 500mg, 1000 IU, 400mcg) AND count/servings (e.g. 100 Count, 60 Softgels) -- both required
- Always keep form: Capsule, Softgel, Gummy, Tablet, Chewable, Powder, Liquid, Lozenge
- Always keep flavour for flavoured supplements (e.g. Chocolate, Vanilla, Unflavored)

PROTEIN POWDER & SPORTS NUTRITION:
- Always keep flavour (e.g. Chocolate, Vanilla, Unflavored) -- separate SKUs
- Always keep size/weight and serving count

SKINCARE & SUNSCREEN:
- Always keep SPF number (e.g. SPF 30, SPF 50) -- primary differentiator
- Always keep active ingredient percentages (e.g. 1% Pyrithione Zinc, 2% Salicylic Acid)
- Always keep skin type if it defines formulation (e.g. Oily, Dry, Sensitive, Combination)

MAKEUP (lipstick, foundation, concealer, eyeshadow, blush, bronzer):
- Always keep shade name AND shade number together (e.g. "Blushing Nude 637", "220 Natural Beige")
- Always keep finish type: Matte, Satin, Gloss, Shimmer, Dewy, Natural, Luminous

HAIR CARE (shampoo, conditioner, mask, treatment):
- Always keep hair type target: Curly, Fine, Color-Treated, Dry, Damaged, Thinning, Oily
- This defines product formulation -- it is not a use-case list

PHONE CASES, SCREEN PROTECTORS & DEVICE ACCESSORIES:
- The compatible device name + full model variant IS the product identity -- always keep it
- Keep exact model variant (iPhone 16 Pro vs Plus vs Max all matter)
- Keep pack count (e.g. 2 Pack, 3 Pack)

CABLES & CHARGERS:
- Always keep cable length (e.g. 3ft, 6ft, 1m), pack count, wattage (e.g. 60W), and connector type (USB-C, Lightning)

AUTOMOTIVE & POWER TOOLS:
- Always keep voltage (V), amperage (A), wattage (W), capacity (Wh)
- Always keep viscosity grades for motor oil (e.g. 5W-30, 10W-40)

ACCESSORIES (bags, covers, cases, mounting plates):
- Keep the compatible device/product name -- it is part of the product identity
- Keep pack size

ALWAYS REMOVE:
- SEO keyword stuffing and slash-separated synonyms (e.g. "table/desk/furniture" → "Table")
- Repetitive synonyms (e.g. "Stuffed Animal, Plush Toy, Plushie" → "Stuffed Animal")
- Redundant restatements (e.g. "Nighttime Protection, Night Time Leak" → "Overnight")
- Marketing filler (e.g. "Award-Winning", "Professionally Designed", "Our Closest Formula to Breast Milk", "Ideal for")
- Use-case lists (e.g. "for Art, Sketching, Travel" → remove)
- Certification/legal text (e.g. "API-Licensed", "Non-GMO", "Officially Licensed")
- Gift occasion filler (e.g. "Perfect Gift for", "Easter Basket Essential", "Great for Kids")
- Redundant brand repetition -- keep brand name once only
- Encoding artifacts (Â®, Â-, Â™, Ã¨ etc. -- remove entirely)

FORMAT RULES:
- Use Title Case
- Output ONLY a raw JSON array. No explanation, no preamble, no markdown, no code fences.
- Your response must contain exactly as many strings as titles given to you.

Transformation examples (for illustration only -- do NOT output these):
  "Pampers ZZZ Overnight Diapers, Size 7 (41+ lbs), 36 Count, Nighttime Protection Disposable Baby Diaper, Night Time Leak and Skin Protection"
    -> "Pampers ZZZ Overnight Diapers Size 7, 36 Count"
  "Bubs 365 Day Grass Fed Infant Formula with Iron, Cow Milk-Based Powder for Infants 0-12 Months, Made with Non-GMO Milk, 20 oz"
    -> "Bubs 365 Day Grass Fed Infant Formula with Iron, 0-12 Months, 20 oz"
  "Enfamil Optimum (Enspire), Our Closest Formula to Breast Milk, Immune-Supporting Lactoferrin, Brain-Supporting DHA Baby Formula, Powder 20.5 Oz Tub"
    -> "Enfamil Optimum Enspire Baby Formula Powder 20.5 oz"
  "Bobbie Organic Baby Formula, Milk Based Powder with Iron, DHA and Vitamin D, Modeled After Breast Milk, Organic Baby Formula for Newborn to 12 Months"
    -> "Bobbie Organic Baby Formula Powder with Iron, DHA, 0-12 Months"
  "Garnier Nutrisse Ultra Creme Hair Color, Permanent Hair Dye with 100% Gray Coverage, 43 Dark Golden Brown, 1 Application"
    -> "Garnier Nutrisse Permanent Hair Color, 43 Dark Golden Brown"
  "Revlon Super Lustrous Lipstick, Creamy Formula For Soft, Fuller-Looking Lips, Moisturized Feel, Blushing Nude (637), 0.15 oz"
    -> "Revlon Super Lustrous Lipstick, Blushing Nude 637, 0.15 oz"
  "Nature's Bounty Vitamin C Tablets, Vitamin Supplement, Supports a Healthy Immune System, 500mg, 100 Count"
    -> "Nature's Bounty Vitamin C 500mg Tablets, 100 Count"
  "Transparent Labs Grass-Fed Whey Protein Isolate, Gluten Free, 28g Protein per Serving, Vanilla, 30 Servings"
    -> "Transparent Labs Grass-Fed Whey Protein Isolate, Vanilla, 30 Servings"
  "YINLAI Case for iPhone 16 Pro 6.3-Inch, iPhone 16 Pro Phone Case Magnetic Compatible with Magsafe Translucent Matte"
    -> "YINLAI iPhone 16 Pro Case, MagSafe, Translucent Matte"
  "USB C to USB C Charging Cable 3ft 60W 5Pack, Type C to Type C Fast Charger Cord Compatible for iPhone 16 15"
    -> "USB-C to USB-C Charging Cable 3ft 60W, 5 Pack"
  "Nioxin Scalp Recovery Purifying Shampoo for Dandruff and Itchy Scalp with Pyrithione Zinc and Green Tea Extracts, 33.8 Fl oz"
    -> "Nioxin Scalp Recovery Purifying Shampoo, Dandruff, 33.8 fl oz"
  "Revlon ColorStay Liquid Foundation for Combination & Oily Skin, SPF 15, Medium-Full Coverage, Matte Finish, 220 Natural Beige, 1 fl oz"
    -> "Revlon ColorStay Foundation, Oily Skin, SPF 15, Matte, 220 Natural Beige"
  "Happy Baby Organics Baby Snacks, Gentle Teething Wafers, Gluten Free & Vegan, Blueberry & Purple Carrot, 12 Count (Pack of 6)"
    -> "Happy Baby Organics Teething Wafers, Blueberry Purple Carrot, 12 Count"
  "Royal Purple 51530 API-Licensed SAE 5W-30 High Performance Synthetic Motor Oil - 5 qt."
    -> "Royal Purple 51530 SAE 5W-30 Synthetic Motor Oil 5 qt"
  "U.S. Art Supply 23\" x 26\" Artist Sketch Tote Board - Lightweight Hardboard Drawing Board with Handle, Clip & Rubber Band for Art, Sketching, Travel"
    -> "U.S. Art Supply 23\" x 26\" Artist Sketch Tote Board"
  "Meguiar's MT300 Variable Speed Dual Action Polisher, Professionally Designed Car Scratch Remover with Digital Torque Management"
    -> "Meguiar's MT300 Variable Speed Dual Action Polisher\""""



# ─────────────────────────────────────────────
# CHECKPOINT HELPERS
# ─────────────────────────────────────────────

def load_checkpoint() -> dict:
    """Load existing checkpoint from disk. Returns empty dict if none exists."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"📌 Resuming from checkpoint: {len(data):,} titles already done")
            return data  # { "row_index_str": "shortened title", ... }
        except Exception as e:
            print(f"⚠️  Could not read checkpoint file: {e} -- starting fresh")
    return {}


def save_checkpoint(checkpoint: dict):
    """Atomically save checkpoint to disk (write to tmp then rename)."""
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False)
    os.replace(tmp, CHECKPOINT_FILE)  # atomic on all platforms


# ─────────────────────────────────────────────
# INPUT LOADING
# ─────────────────────────────────────────────

def load_input(filepath: str) -> tuple:
    """
    Load input file (xlsx or csv).
    Returns (rows, headers) where rows is a list of dicts keyed by header name.
    Uses read_only mode for xlsx to handle large files without loading all into RAM.
    """
    ext = os.path.splitext(filepath)[1].lower()

    if ext == ".csv":
        print(f"📂 Loading CSV: {filepath}")
        with open(filepath, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = list(reader.fieldnames or [])
            rows = list(reader)
        return rows, headers

    elif ext in (".xlsx", ".xls"):
        print(f"📂 Loading Excel: {filepath} (read-only mode)...")
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        row_iter = ws.iter_rows(values_only=True)
        headers = [str(h) if h is not None else "" for h in next(row_iter)]
        rows = []
        for raw_row in row_iter:
            rows.append({
                headers[i]: (raw_row[i] if i < len(raw_row) else "")
                for i in range(len(headers))
            })
        wb.close()
        return rows, headers

    else:
        print(f"❌ Unsupported file type: {ext}. Use .xlsx or .csv")
        sys.exit(1)


# ─────────────────────────────────────────────
# PRE-PROCESSING
# ─────────────────────────────────────────────

import re as _re

# Common encoding artifacts from broken UTF-8 → Windows-1252 conversions
_ENCODING_ARTIFACTS = _re.compile(
    r'Â®|Â™|Â-|Â--|Â·|Â°|Ã©|Ã¨|Ã |Ã®|Ã´|Ã»|Ã¢|Ã£|Ã§|Â¡|Â¿|Â½|Â¼|Â¾|Â‡|Â†|â€™|â€œ|â€|â€¦|â€"'
)

def clean_title(title: str) -> str:
    """Strip encoding artifacts and normalise whitespace before sending to LLM."""
    title = _ENCODING_ARTIFACTS.sub('', title)
    title = _re.sub(r'  +', ' ', title).strip()
    return title


# ─────────────────────────────────────────────
# VARIANT NAME SAFEGUARD
# ─────────────────────────────────────────────

# Named sub-variants that must NOT appear in the shortened title unless
# they also appear in the original. Prevents the model hallucinating e.g.
# "Gentlease" into a non-Gentlease product when similar titles are nearby.
_VARIANT_NAMES = [
    "gentlease", "enspire", "optimum", "neuropro", "nutramigen", "reguline",
    "alimentum", "pro-advance", "pro-sensitive", "pro-total", "360 total",
    "good start", "soothe",
    "overnight", "extra strength", "maximum strength",
]

def check_variant_names(original: str, shortened: str) -> bool:
    """
    Returns True if the shortened title is clean.
    Returns False if the shortened title contains a variant name that
    does NOT appear in the original -- indicating hallucination.
    """
    orig_lower = original.lower()
    short_lower = shortened.lower()
    for variant in _VARIANT_NAMES:
        if variant in short_lower and variant not in orig_lower:
            return False
    return True


# ─────────────────────────────────────────────
# OLLAMA
# ─────────────────────────────────────────────

def check_ollama() -> bool:
    """Check if Ollama is running and the model is available."""
    try:
        r = requests.get("http://localhost:11434/api/tags", timeout=5)
        models = [m["name"] for m in r.json().get("models", [])]
        available = any(MODEL.split(":")[0] in m for m in models)
        if not available:
            print(f"❌ Model '{MODEL}' not found. Available: {models}")
            print(f"   Run: ollama pull {MODEL}")
            return False
        print(f"✅ Ollama running, model '{MODEL}' ready")
        return True
    except Exception:
        print("❌ Ollama is not running. Start it or run 'ollama serve' in a terminal.")
        return False


def shorten_batch(titles: list, batch_index: int) -> list:
    """Send a batch of titles to Ollama and return shortened versions."""
    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"Shorten this {len(titles)} title. "
        f"Return a JSON array with exactly {len(titles)} element:\n"
        f"{json.dumps(titles)}"
    )

    for attempt in range(RETRY_LIMIT):
        try:
            response = requests.post(
                OLLAMA_URL,
                json={
                    "model": MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.2, "num_predict": 2048}
                },
                timeout=120
            )
            response.raise_for_status()
            raw = response.json()["response"].strip()

            # Extract JSON array -- ignore any preamble the model adds
            start = raw.find("[")
            end = raw.rfind("]") + 1
            if start == -1 or end == 0:
                raise ValueError("No JSON array found in response")

            result = json.loads(raw[start:end])

            # Must return exactly the right number of titles
            if len(result) != len(titles):
                raise ValueError(f"Count mismatch: got {len(result)}, expected {len(titles)}")

            # Detect titles that weren't shortened (verbatim copy, too long, or over char limit)
            for idx, (orig, short) in enumerate(zip(titles, result)):
                short_str = str(short).strip()
                orig_str = str(orig).strip()
                if len(short_str) > 85:
                    raise ValueError(f"Item {idx} exceeds 85 chars ({len(short_str)}): '{short_str[:60]}'")
                # Only reject verbatim copy if original was already under 85 chars.
                # If original was over 85, any output at or under 85 chars is a valid shortening.
                if short_str == orig_str and len(orig_str) <= 85:
                    raise ValueError(f"Item {idx} was not shortened: '{short_str[:60]}'")

            # Cross-check: verify shortened title shares key words with its original.
            # This catches offset/shifted results where titles belong to the wrong row.
            for idx, (orig, short) in enumerate(zip(titles, result)):
                orig_words = set(w.strip(".,()[]'\"").lower() for w in orig.split() if len(w) > 4)
                short_words = set(w.strip(".,()[]'\"").lower() for w in str(short).split() if len(w) > 4)
                if not orig_words:
                    continue
                overlap = len(orig_words & short_words)
                overlap_ratio = overlap / min(len(short_words), len(orig_words)) if short_words else 0
                if overlap_ratio < 0.25:
                    raise ValueError(
                        f"Item {idx} looks mismatched (only {overlap} shared words): "
                        f"orig='{orig[:50]}' short='{short[:50]}'"
                    )
                if not check_variant_names(orig, str(short)):
                    raise ValueError(
                        f"Item {idx} contains hallucinated variant name: "
                        f"orig='{orig[:50]}' short='{short[:50]}'"
                    )

            return result

        except Exception as e:
            print(f"  ❌ Batch {batch_index} attempt {attempt + 1}/{RETRY_LIMIT} failed: {e}")
            if attempt < RETRY_LIMIT - 1:
                time.sleep(2 ** attempt)  # exponential backoff: 1s, 2s

    # All retries exhausted -- keep originals so we never lose a row
    print(f"  ⚠️  Batch {batch_index}: all retries failed, keeping original titles")
    return list(titles)


def shorten_single(title: str, row_idx: int) -> str:
    """Shorten a single title -- used as fallback when batch processing fails."""
    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"Shorten this 1 title. Return a JSON array with exactly 1 element:\n"
        f"{json.dumps([title])}"
    )

    for attempt in range(RETRY_LIMIT):
        try:
            response = requests.post(
                OLLAMA_URL,
                json={
                    "model": MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.2, "num_predict": 512}
                },
                timeout=60
            )
            response.raise_for_status()
            raw = response.json()["response"].strip()

            start = raw.find("[")
            end = raw.rfind("]") + 1
            if start == -1 or end == 0:
                raise ValueError("No JSON array in response")

            result = json.loads(raw[start:end])
            if not result or not isinstance(result[0], str):
                raise ValueError("Empty or invalid result")

            shortened = result[0].strip()
            if len(shortened) > 85:
                raise ValueError(f"Exceeds 85 chars ({len(shortened)}): '{shortened[:60]}'")
            # Only reject verbatim copy if original was already under 85 chars.
            # If original was over 85, any output under 85 is valid -- don't reject it.
            if shortened == title.strip() and len(title.strip()) <= 85:
                raise ValueError(f"Not shortened: '{shortened[:60]}'")

            # Cross-check alignment
            orig_words = set(w.strip(".,()[]'\"").lower() for w in title.split() if len(w) > 4)
            short_words = set(w.strip(".,()[]'\"").lower() for w in shortened.split() if len(w) > 4)
            if orig_words and short_words:
                overlap = len(orig_words & short_words)
                overlap_ratio = overlap / min(len(short_words), len(orig_words))
                if overlap_ratio < 0.25:
                    raise ValueError(f"Title looks mismatched ({overlap} shared words): '{shortened[:60]}'")

            if not check_variant_names(title, shortened):
                raise ValueError(f"Hallucinated variant name in: '{shortened[:60]}'")

            return shortened

        except Exception as e:
            print(f"    ❌ Single retry row {row_idx} attempt {attempt + 1}/{RETRY_LIMIT}: {e}")
            if attempt < RETRY_LIMIT - 1:
                time.sleep(1)

    print(f"    ⚠️  Row {row_idx}: single retry failed, keeping original")
    return title




def shorten_dedup(title: str, conflicting_title: str, row_idx: int) -> str:
    """
    Re-shorten a title that produced a duplicate shortened result.
    Tells the model exactly what conflict exists and asks it to differentiate.
    Falls back to appending a disambiguator extracted from the original if LLM fails.
    """
    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"DISAMBIGUATION REQUIRED:\n"
        f"The title below shortened to a result that is IDENTICAL to another product's shortened title.\n"
        f"Conflicting shortened title already in use: \"{conflicting_title}\"\n"
        f"You MUST produce a result that is clearly different from the above.\n"
        f"Look carefully at the original for any attribute that distinguishes this product "
        f"(e.g. size, count, flavour, shade, age range, format, variant) and include it.\n"
        f"Still respect the 85-character limit and all other rules.\n\n"
        f"Return a JSON array with exactly 1 element:\n"
        f"{json.dumps([title])}"
    )

    for attempt in range(RETRY_LIMIT):
        try:
            response = requests.post(
                OLLAMA_URL,
                json={
                    "model": MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.4, "num_predict": 512}
                },
                timeout=60
            )
            response.raise_for_status()
            raw = response.json()["response"].strip()

            start = raw.find("[")
            end = raw.rfind("]") + 1
            if start == -1 or end == 0:
                raise ValueError("No JSON array in response")

            result = json.loads(raw[start:end])
            if not result or not isinstance(result[0], str):
                raise ValueError("Empty or invalid result")

            shortened = result[0].strip()

            if len(shortened) > 85:
                raise ValueError(f"Exceeds 85 chars ({len(shortened)})")
            if shortened == conflicting_title.strip():
                raise ValueError(f"Still identical to conflict: '{shortened[:60]}'")

            # Word overlap check
            orig_words = set(w.strip(".,()[]'\"").lower() for w in title.split() if len(w) > 4)
            short_words = set(w.strip(".,()[]'\"").lower() for w in shortened.split() if len(w) > 4)
            if orig_words and short_words:
                overlap = len(orig_words & short_words) / min(len(short_words), len(orig_words))
                if overlap < 0.25:
                    raise ValueError(f"Title looks mismatched ({overlap:.0%} overlap)")

            return shortened

        except Exception as e:
            print(f"    ❌ Dedup row {row_idx} attempt {attempt + 1}/{RETRY_LIMIT}: {e}")
            if attempt < RETRY_LIMIT - 1:
                time.sleep(1)

    # LLM failed — try a simple rule-based fallback:
    # Extract the first number+unit or size-like token from the original not in the conflict
    import re
    tokens = re.findall(
        r'\b(\d+\.?\d*\s*(?:oz|fl oz|ml|mg|mcg|iu|lb|lbs|kg|g\b|qt|count|ct|pack|ft|inch|inches|"|\'|mm|cm|tablet|capsule|softgel|gummy|serving|months?|stage\s*\d))\b',
        title, re.IGNORECASE
    )
    # Find a token not already present in the conflicting title
    for token in tokens:
        if token.lower() not in conflicting_title.lower():
            candidate = conflicting_title.rstrip() + f", {token.strip()}"
            if len(candidate) <= 85:
                print(f"    ⚠️  Row {row_idx}: LLM dedup failed, appended disambiguator: '{token.strip()}'")
                return candidate

    print(f"    ⚠️  Row {row_idx}: dedup fully failed, flagging for review")
    return None  # Caller will mark as duplicate needing review


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    if not check_ollama():
        return

    # ── Load input ──
    rows, headers = load_input(INPUT_FILE)
    total = len(rows)
    print(f"📊 Found {total:,} products to process")

    if "Name" not in headers:
        print(f"❌ Could not find 'Name' column. Columns found: {headers}")
        return

    titles = [clean_title(str(row.get("Name") or "")) for row in rows]

    # ── Load checkpoint (resume support) ──
    checkpoint = load_checkpoint()
    already_done = sum(1 for k in checkpoint if int(k) < total)
    remaining = total - already_done
    print(f"   {already_done:,} already done, {remaining:,} remaining\n")

    if remaining > 0:
        # Build list of batches that still need processing
        batches_to_run = []
        for i in range(0, total, BATCH_SIZE):
            batch_indices = list(range(i, min(i + BATCH_SIZE, total)))
            if all(str(idx) in checkpoint for idx in batch_indices):
                continue  # entire batch already done
            batches_to_run.append(batch_indices)

        total_batches = len(batches_to_run)
        print(f"🔄 {total_batches} batches to process ({BATCH_SIZE} titles each, {MAX_WORKERS} parallel workers)")
        print(f"   Checkpoint saved every {CHECKPOINT_EVERY} batches -- safe to Ctrl+C and resume\n")

        completed_batches = 0
        start_time = time.time()
        checkpoint_lock = threading.Lock()

        def process_batch(batch_indices):
            batch_titles = [titles[idx] for idx in batch_indices]
            batch_num = batch_indices[0] // BATCH_SIZE
            result = shorten_batch(batch_titles, batch_num)
            return batch_indices, result

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_batch, b): b for b in batches_to_run}

            for future in as_completed(futures):
                batch_indices, result = future.result()

                with checkpoint_lock:
                    for idx, shortened in zip(batch_indices, result):
                        checkpoint[str(idx)] = shortened

                    completed_batches += 1
                    done_count = sum(1 for k in checkpoint if int(k) < total)
                    pct = done_count / total * 100

                    # Estimate time remaining
                    elapsed = time.time() - start_time
                    rate = completed_batches / elapsed if elapsed > 0 else 0
                    eta_secs = int((total_batches - completed_batches) / rate) if rate > 0 else 0
                    eta_str = f"{eta_secs // 3600}h {(eta_secs % 3600) // 60}m" if eta_secs > 60 else f"{eta_secs}s"

                    print(f"  ✅ {done_count:,}/{total:,} ({pct:.1f}%) -- ETA: {eta_str}")

                    # Flush checkpoint to disk periodically
                    if completed_batches % CHECKPOINT_EVERY == 0:
                        save_checkpoint(checkpoint)
                        print(f"     💾 Checkpoint saved ({done_count:,} titles)")

        # Final checkpoint flush
        save_checkpoint(checkpoint)
        print(f"\n💾 Final checkpoint saved ({len(checkpoint):,} titles)")


    # ── Fallback pass: fix any rows where batch processing failed ──
    # These are rows where the "shortened" title still equals the original.
    fallback_indices = [
        idx for idx in range(total)
        if checkpoint.get(str(idx), "").strip() == titles[idx].strip()
        and titles[idx].strip() != ""
    ]

    if fallback_indices:
        print(f"\n🔁 Fallback pass: {len(fallback_indices)} titles weren't shortened -- retrying one-by-one...")
        fixed = 0
        for idx in fallback_indices:
            result = shorten_single(titles[idx], idx)
            checkpoint[str(idx)] = result
            if result != titles[idx]:
                fixed += 1
                print(f"   ✅ Fixed row {idx}: {result}")
        save_checkpoint(checkpoint)
        print(f"   Fixed {fixed}/{len(fallback_indices)} titles")
    else:
        print("\n✅ No fallback needed -- all titles were shortened successfully")

    # ── Dedup pass: fix any rows with identical shortened titles ──
    # Build a map of shortened_title -> first row index that used it
    print("\n🔍 Checking for duplicate shortened titles...")
    seen_titles = {}   # shortened_title (lower) -> first_idx
    duplicate_indices = []  # (idx, conflicting_idx)

    for idx in range(total):
        short = checkpoint.get(str(idx), "").strip()
        if not short:
            continue
        key = short.lower()
        if key in seen_titles:
            duplicate_indices.append((idx, seen_titles[key]))
        else:
            seen_titles[key] = idx

    if duplicate_indices:
        print(f"   Found {len(duplicate_indices)} duplicate(s) — attempting to disambiguate...")
        dedup_failed = set()  # indices that couldn't be resolved
        resolved = 0

        for idx, conflict_idx in duplicate_indices:
            conflicting_short = checkpoint.get(str(conflict_idx), "").strip()
            print(f"   🔁 Row {idx}: '{checkpoint[str(idx)][:60]}' conflicts with row {conflict_idx}")
            result = shorten_dedup(titles[idx], conflicting_short, idx)
            if result is not None:
                checkpoint[str(idx)] = result
                resolved += 1
                print(f"      ✅ Resolved: '{result}'")
            else:
                dedup_failed.add(idx)

        save_checkpoint(checkpoint)
        print(f"   Resolved {resolved}/{len(duplicate_indices)} duplicates")
        if dedup_failed:
            print(f"   ⚠️  {len(dedup_failed)} could not be resolved — flagged in output as REVIEW_DUPLICATE")
    else:
        print("   ✅ No duplicates found")
        dedup_failed = set()

        # ── Write output CSV (streaming -- memory-safe for 200k rows) ──
    print(f"\n📝 Writing output to {OUTPUT_FILE}...")

    pid_key  = "Product ID" if "Product ID" in headers else None
    code_key = "Code"        if "Code"       in headers else None
    url_key  = "Product URL" if "Product URL" in headers else None

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["Product ID", "Code", "Original Title", "Shortened Title", "Duplicate Flag", "Product URL"])
        for idx, row in enumerate(rows):
            shortened = checkpoint.get(str(idx), titles[idx])
            dup_flag = "REVIEW_DUPLICATE" if idx in dedup_failed else ""
            writer.writerow([
                row.get(pid_key,  "") if pid_key  else "",
                row.get(code_key, "") if code_key else "",
                titles[idx],
                shortened,
                dup_flag,
                row.get(url_key,  "") if url_key  else "",
            ])

    print(f"\n🎉 Done! Output saved to: {OUTPUT_FILE}")
    print(f"   {total:,} titles written")
    print(f"\n   ⚠️  Starting a new file? Delete '{CHECKPOINT_FILE}' first.")


if __name__ == "__main__":
    main()
