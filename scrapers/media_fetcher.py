"""
Phase 4: Download media assets from the Media table to output/images/.
Updates Media.local_path on success.
"""

import asyncio
import hashlib
import re
from pathlib import Path
from urllib.parse import urlparse

import httpx
from sqlalchemy.orm import Session

from models.schema import Media

IMAGE_DIR = Path("output/images")
CONCURRENCY = 6
TIMEOUT_S = 30


def _local_filename(url: str, role: str, product_id: int) -> str:
    """Deterministic filename: {product_id}_{role}_{url_hash}.{ext}"""
    ext = Path(urlparse(url).path).suffix or ".webp"
    h = hashlib.md5(url.encode()).hexdigest()[:8]
    role_safe = re.sub(r"[^a-z0-9]", "_", role.lower())
    return f"p{product_id:03d}_{role_safe}_{h}{ext}"


async def _download(client: httpx.AsyncClient, url: str, dest: Path) -> bool:
    if dest.exists():
        return True  # already downloaded
    try:
        r = await client.get(url, timeout=TIMEOUT_S, follow_redirects=True)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return True
    except Exception as e:
        print(f"  [warn] {url}: {e}")
        return False


async def download_all_media(session: Session, roles=None) -> dict:
    """
    Download media assets.

    Args:
        session: SQLAlchemy session (must be open).
        roles:   if supplied, only download media with these roles
                 (e.g. ['bottle_shot']). None = all.
    """
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)

    query = session.query(Media)
    if roles:
        query = query.filter(Media.role.in_(roles))
    items: list[Media] = query.all()

    print(f"Media rows to download: {len(items)}")

    sem = asyncio.Semaphore(CONCURRENCY)
    stats = {"ok": 0, "skip": 0, "fail": 0}

    async def fetch_one(media: Media):
        async with sem:
            if not media.url:
                stats["skip"] += 1
                return
            filename = _local_filename(
                media.url,
                media.role or "image",
                media.product_id or 0,
            )
            dest = IMAGE_DIR / filename
            ok = await _download(client, media.url, dest)
            if ok:
                media.local_path = str(dest)
                stats["ok"] += 1
                if not dest.stat().st_size:
                    dest.unlink()
                    media.local_path = None
                    stats["ok"] -= 1
                    stats["fail"] += 1
            else:
                stats["fail"] += 1

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/120.0.0.0 Safari/537.36"},
        http2=False,
    ) as client:
        await asyncio.gather(*[fetch_one(m) for m in items])

    session.commit()
    return stats
