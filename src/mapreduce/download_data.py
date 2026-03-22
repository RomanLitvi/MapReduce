"""
Download well-known IR test collections.

Small:  20 Newsgroups (~18MB, ~18,000 documents)
        Classic benchmark for text classification and IR.

Large:  English Wikipedia dump (~22GB compressed → ~80GB text)
        The standard large-scale IR collection.

Usage:
  python download_data.py --dataset small --output /data/input/
  python download_data.py --dataset large --output /data/input/
"""
import argparse
import os
import sys
import time
import tarfile
import bz2
import re
import urllib.request
import xml.etree.ElementTree as ET


# ============================================================
# SMALL: 20 Newsgroups (~18MB, ~18,000 docs)
# ============================================================

NEWSGROUPS_URL = (
    "http://qwone.com/~jason/20Newsgroups/20news-bydate.tar.gz"
)


def download_file(url: str, dest: str):
    """Download with progress bar."""
    print(f"Downloading: {url}")
    print(f"Destination: {dest}")

    def progress_hook(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            pct = min(100, downloaded * 100 / total_size)
            mb = downloaded / (1024 * 1024)
            total_mb = total_size / (1024 * 1024)
            print(f"\r  {pct:.0f}% ({mb:.1f}/{total_mb:.1f} MB)", end="", flush=True)

    urllib.request.urlretrieve(url, dest, reporthook=progress_hook)
    print()


def download_small(output_dir: str):
    """
    Download 20 Newsgroups and convert to our format:
    one line per document: "doc_id\ttext"
    """
    os.makedirs(output_dir, exist_ok=True)
    archive_path = os.path.join(output_dir, "20news-bydate.tar.gz")
    corpus_path = os.path.join(output_dir, "20newsgroups.txt")

    if os.path.exists(corpus_path):
        size = os.path.getsize(corpus_path)
        print(f"Corpus already exists: {corpus_path} ({size / (1024*1024):.1f} MB)")
        return

    # Download
    if not os.path.exists(archive_path):
        download_file(NEWSGROUPS_URL, archive_path)

    # Extract and convert
    print("Extracting and converting to corpus format...")
    doc_count = 0

    with tarfile.open(archive_path, "r:gz") as tar, \
         open(corpus_path, "w", encoding="utf-8") as out:

        for member in tar.getmembers():
            if not member.isfile():
                continue

            # Path like: 20news-bydate-train/comp.graphics/38758
            parts = member.name.split("/")
            if len(parts) < 3:
                continue

            category = parts[1]
            file_id = parts[2]
            doc_id = f"{category}_{file_id}"

            f = tar.extractfile(member)
            if f is None:
                continue

            try:
                text = f.read().decode("utf-8", errors="replace")
            except Exception:
                continue

            # Clean: replace newlines and tabs with spaces
            text = text.replace("\n", " ").replace("\t", " ").strip()
            text = re.sub(r"\s+", " ", text)

            if text:
                out.write(f"{doc_id}\t{text}\n")
                doc_count += 1

    # Clean up archive
    os.remove(archive_path)

    size = os.path.getsize(corpus_path)
    print(f"Done: {doc_count:,} documents, {size / (1024*1024):.1f} MB")
    print(f"Saved to: {corpus_path}")


# ============================================================
# LARGE: Wikipedia dump (~22GB compressed → ~80GB text)
# ============================================================

# Full dump (~22GB compressed → ~80GB text)
WIKIPEDIA_FULL_URL = (
    "https://dumps.wikimedia.org/enwiki/latest/"
    "enwiki-latest-pages-articles.xml.bz2"
)

# Partial dumps (~1-2GB compressed each, ~4-6GB text)
# Wikimedia splits articles into chunks: articles1.xml-*, articles2.xml-*, etc.
WIKIPEDIA_PARTIAL_URLS = [
    "https://dumps.wikimedia.org/enwiki/latest/"
    "enwiki-latest-pages-articles1.xml-p1p41242.bz2",
]


def download_large(output_dir: str, max_size_gb: float = 0):
    """
    Download English Wikipedia and convert to our format.
    Streams bz2-compressed XML — never loads full file into memory.
    """
    os.makedirs(output_dir, exist_ok=True)
    dump_path = os.path.join(output_dir, "enwiki-latest-pages-articles.xml.bz2")
    corpus_path = os.path.join(output_dir, "wikipedia.txt")

    if os.path.exists(corpus_path):
        size = os.path.getsize(corpus_path)
        print(f"Corpus already exists: {corpus_path} ({size / (1024**3):.1f} GB)")
        return

    # Choose dump: partial (~1.5GB) if size-limited, full (~22GB) otherwise
    if max_bytes and max_bytes <= 10 * 1024**3:
        url = WIKIPEDIA_PARTIAL_URLS[0]
        print(f"Using partial Wikipedia dump (~1.5GB download for ≤{max_size_gb}GB text)")
    else:
        url = WIKIPEDIA_FULL_URL
        print("Using full Wikipedia dump (~22GB download)")

    if not os.path.exists(dump_path):
        download_file(url, dump_path)

    # Stream-parse XML from bz2
    max_bytes = int(max_size_gb * 1024**3) if max_size_gb > 0 else 0
    if max_bytes:
        print(f"Parsing Wikipedia XML (limit: {max_size_gb} GB)...")
    else:
        print("Parsing Wikipedia XML (full dump, streaming, low memory)...")
    doc_count = 0
    bytes_written = 0

    # MediaWiki XML namespace
    ns = "{http://www.mediawiki.org/xml/export-0.11/}"

    with open(corpus_path, "w", encoding="utf-8") as out:
        # Use iterparse to stream — never loads full XML tree
        with bz2.open(dump_path, "rt", encoding="utf-8", errors="replace") as bz:
            context = ET.iterparse(bz, events=("end",))

            for event, elem in context:
                if elem.tag == f"{ns}page":
                    # Extract title and text
                    title_elem = elem.find(f"{ns}title")
                    text_elem = elem.find(f".//{ns}text")
                    ns_elem = elem.find(f"{ns}ns")

                    # Only main namespace (articles, not talk/user pages)
                    if ns_elem is not None and ns_elem.text == "0":
                        title = title_elem.text if title_elem is not None else ""
                        text = text_elem.text if text_elem is not None else ""

                        if text and not text.startswith("#REDIRECT"):
                            # Clean markup (basic — removes most wiki syntax)
                            text = clean_wikitext(text)
                            text = f"{title} {text}"
                            text = text.replace("\n", " ").replace("\t", " ")
                            text = re.sub(r"\s+", " ", text).strip()

                            if len(text) > 50:  # Skip stubs
                                doc_id = f"wiki_{doc_count}"
                                line = f"{doc_id}\t{text}\n"
                                out.write(line)
                                bytes_written += len(line.encode("utf-8"))
                                doc_count += 1

                                if doc_count % 100_000 == 0:
                                    gb = bytes_written / (1024**3)
                                    print(f"  {doc_count:,} articles, {gb:.2f} GB")

                                # Stop if we hit the size limit
                                if max_bytes and bytes_written >= max_bytes:
                                    print(f"Reached {max_size_gb} GB limit, stopping.")
                                    break

                    # Free memory — critical for streaming large XML
                    elem.clear()

                    if max_bytes and bytes_written >= max_bytes:
                        break

    size = os.path.getsize(corpus_path)
    print(f"Done: {doc_count:,} articles, {size / (1024**3):.2f} GB")
    print(f"Saved to: {corpus_path}")


def clean_wikitext(text: str) -> str:
    """Remove basic wiki markup. Not perfect but good enough for indexing."""
    # Remove references, templates, HTML tags
    text = re.sub(r"\{\{[^}]*\}\}", " ", text)         # {{templates}}
    text = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", r"\1", text)  # [[links]]
    text = re.sub(r"\[https?://[^\]]*\]", " ", text)    # [external links]
    text = re.sub(r"<[^>]+>", " ", text)                # <html tags>
    text = re.sub(r"'{2,}", "", text)                   # '''bold''', ''italic''
    text = re.sub(r"={2,}[^=]+={2,}", " ", text)        # == headings ==
    text = re.sub(r"\|[^\n]*", " ", text)               # table rows
    return text


def main():
    parser = argparse.ArgumentParser(description="Download IR test collections")
    parser.add_argument(
        "--dataset",
        choices=["small", "large"],
        required=True,
        help="small = 20 Newsgroups (18MB), large = Wikipedia (80GB)",
    )
    parser.add_argument(
        "--output",
        default="./data/input",
        help="Output directory",
    )
    parser.add_argument(
        "--max-size-gb",
        type=float,
        default=0,
        help="Stop after N GB of text (0 = no limit). Only for 'large' dataset.",
    )
    args = parser.parse_args()

    t_start = time.time()

    if args.dataset == "small":
        download_small(args.output)
    else:
        download_large(args.output, max_size_gb=args.max_size_gb)

    elapsed = time.time() - t_start
    print(f"Total time: {elapsed:.0f}s")


if __name__ == "__main__":
    main()
