import argparse
import os
import time
import shutil
import hashlib
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp", ".heic"}

# ---- Optional EXIF via Pillow ----
try:
    from PIL import Image, ExifTags
    PIL_AVAILABLE = True
    EXIF_TAGS = {v: k for k, v in ExifTags.TAGS.items()}
    TAG_DTO = EXIF_TAGS.get("DateTimeOriginal")
    TAG_DT = EXIF_TAGS.get("DateTime")
except Exception:
    PIL_AVAILABLE = False
    TAG_DTO = None
    TAG_DT = None


def iter_images(root: Path):
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
            yield p


def get_year_month(path: Path) -> tuple[str, str]:
    """(YYYY, MM) using EXIF DateTimeOriginal if possible; else file mtime."""
    if PIL_AVAILABLE and path.suffix.lower() in {".jpg", ".jpeg", ".tif", ".tiff", ".webp"}:
        try:
            with Image.open(path) as img:
                exif = img.getexif()
                if exif:
                    dt_str = None
                    if TAG_DTO and TAG_DTO in exif:
                        dt_str = exif.get(TAG_DTO)
                    elif TAG_DT and TAG_DT in exif:
                        dt_str = exif.get(TAG_DT)

                    if isinstance(dt_str, str) and len(dt_str) >= 19:
                        try:
                            dt = datetime.strptime(dt_str[:19], "%Y:%m:%d %H:%M:%S")
                            return f"{dt.year:04d}", f"{dt.month:02d}"
                        except Exception:
                            pass
        except Exception:
            pass

    dt = datetime.fromtimestamp(path.stat().st_mtime)
    return f"{dt.year:04d}", f"{dt.month:02d}"


def safe_move(src: Path, dst: Path) -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        stem, suf = dst.stem, dst.suffix
        i = 1
        while True:
            alt = dst.with_name(f"{stem}__{i}{suf}")
            if not alt.exists():
                dst = alt
                break
            i += 1
    shutil.move(str(src), str(dst))
    return dst


def safe_delete(path: Path):
    path.unlink(missing_ok=False)


def format_bytes(n: int) -> str:
    v = float(n)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if v < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(v)}{unit}"
            return f"{v:.2f}{unit}"
        v /= 1024
    return f"{v:.2f}PB"


# ---------- Hashing ----------
def _blake2b_small(data: bytes) -> str:
    h = hashlib.blake2b(digest_size=20)
    h.update(data)
    return h.hexdigest()


def quick_hash(path_str: str, head_tail_kb: int = 64) -> tuple[str, str]:
    p = Path(path_str)
    k = head_tail_kb * 1024
    size = p.stat().st_size
    with p.open("rb") as f:
        head = f.read(k)
        if size > k:
            f.seek(max(0, size - k))
            tail = f.read(k)
        else:
            tail = b""
    return (path_str, _blake2b_small(head + tail))


def full_hash(path_str: str, chunk_mb: int = 2) -> tuple[str, str]:
    p = Path(path_str)
    h = hashlib.blake2b(digest_size=32)
    chunk = chunk_mb * 1024 * 1024
    with p.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return (path_str, h.hexdigest())


def choose_keep(paths: list[Path]) -> Path:
    """Keep shortest filename; tie -> lexicographically smallest full path (case-insensitive)."""
    def key(p: Path):
        return (len(p.name), str(p).lower())
    return min(paths, key=key)


def list_files_in_dir(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    return [p for p in dir_path.iterdir() if p.is_file()]


def log(fp, msg: str):
    fp.write(msg + "\n")
    fp.flush()


def fail_or_continue(fp, fail_fast: bool, msg: str, exc: Exception | None = None):
    if exc is not None:
        log(fp, f"{msg} | EXC: {exc}")
    else:
        log(fp, msg)
    if fail_fast:
        raise RuntimeError(msg) from exc


def move_month_root_files_into_001(month_root: Path, dry_run: bool, fp, fail_fast: bool):
    """Policy A: when splitting starts, move month-root files into YYYY/MM/001."""
    root_files = list_files_in_dir(month_root)
    if not root_files:
        return

    target_dir = month_root / "001"
    if dry_run:
        log(fp, f"[SPLIT-INIT] Would move {len(root_files)} files: {month_root} -> {target_dir}")
        return

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        for f in root_files:
            safe_move(f, target_dir / f.name)
        log(fp, f"[SPLIT-INIT] Moved {len(root_files)} files into {target_dir}")
    except Exception as e:
        fail_or_continue(fp, fail_fast, f"[ERR] split-init move failed for {month_root}", e)


class YMFolderPlanner:
    """
    Output structure:
      base/YYYY/MM/  (flat if <=limit)
      base/YYYY/MM/001, 002... (split if exceeds; Policy A moves month-root files into 001)
    """
    def __init__(self, base_root: Path, limit: int, dry_run: bool, fp, fail_fast: bool):
        self.base_root = base_root
        self.limit = limit
        self.dry_run = dry_run
        self.fp = fp
        self.fail_fast = fail_fast
        self.state = {}  # (yyyy,mm) -> dict

    def _init_state_from_disk(self, yyyy: str, mm: str):
        key = (yyyy, mm)
        if key in self.state:
            return

        month_root = self.base_root / yyyy / mm
        month_root.mkdir(parents=True, exist_ok=True)

        split_dirs = sorted([p for p in month_root.iterdir()
                            if p.is_dir() and p.name.isdigit() and len(p.name) == 3])

        if split_dirs:
            last = split_dirs[-1]
            part = int(last.name)
            count = len([p for p in last.iterdir() if p.is_file()])
            self.state[key] = {"mode": "split", "part": part, "count_in_part": count}
        else:
            flat_count = len(list_files_in_dir(month_root))
            self.state[key] = {"mode": "flat", "flat_count": flat_count}

    def _switch_to_split_mode(self, yyyy: str, mm: str):
        month_root = self.base_root / yyyy / mm
        move_month_root_files_into_001(month_root, self.dry_run, self.fp, self.fail_fast)

        count_in_001 = 0
        if (month_root / "001").exists():
            count_in_001 = len([p for p in (month_root / "001").iterdir() if p.is_file()])
        self.state[(yyyy, mm)] = {"mode": "split", "part": 1, "count_in_part": count_in_001}

    def next_destination(self, yyyy: str, mm: str, filename: str) -> Path:
        self._init_state_from_disk(yyyy, mm)
        st = self.state[(yyyy, mm)]
        month_root = self.base_root / yyyy / mm

        if st["mode"] == "flat":
            if st["flat_count"] + 1 > self.limit:
                log(self.fp, f"[SPLIT] Switching to split mode for {yyyy}/{mm} (limit={self.limit})")
                self._switch_to_split_mode(yyyy, mm)
                st = self.state[(yyyy, mm)]
            else:
                st["flat_count"] += 1
                return month_root / filename

        part = st["part"]
        count = st["count_in_part"]
        if count + 1 > self.limit:
            part += 1
            count = 0
            st["part"] = part
            st["count_in_part"] = 0
        st["count_in_part"] += 1
        return month_root / f"{part:03d}" / filename


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="원본 폴더")
    ap.add_argument("output", help="정리 결과 폴더(ORGANIZED)")
    ap.add_argument("--dups", default="DUPLICATES", help="중복 이동 폴더")
    ap.add_argument("--limit", type=int, default=1000, help="월 폴더당 최대 파일 수")
    ap.add_argument("--delete", action="store_true", help="중복 파일을 이동 대신 영구 삭제(주의)")
    ap.add_argument("--dry-run", action="store_true", help="실제 이동/삭제 없이 로그만 생성")
    ap.add_argument("--workers", type=int, default=1, help="해시 병렬 워커 수(기본 1: 안전)")
    ap.add_argument("--fail-fast", action="store_true", help="에러 1개라도 나면 즉시 중단(안전)")
    ap.add_argument("--report", default="report.txt", help="리포트 txt 파일 경로")
    ap.add_argument("--head-tail-kb", type=int, default=64, help="quick-hash head/tail 크기(KB)")
    ap.add_argument("--chunk-mb", type=int, default=2, help="full-hash chunk size(MB)")
    args = ap.parse_args()

    t0 = time.time()
    in_root = Path(args.input).resolve()
    out_root = Path(args.output).resolve()
    dup_root = Path(args.dups).resolve()
    report_path = Path(args.report).resolve()

    images = list(iter_images(in_root))
    if not images:
        print("No images found.")
        return

    # Stream log from start
    with open(report_path, "w", encoding="utf-8") as fp:
        log(fp, "=== Photo Organizer Report (Safe) ===")
        log(fp, f"Input:  {in_root}")
        log(fp, f"Output: {out_root}  (YYYY/MM/[001..])")
        log(fp, f"Dups:   {dup_root}  (YYYY/MM/[001..])")
        log(fp, f"Limit:  {args.limit} files per month")
        log(fp, f"Mode:   {'DELETE duplicates' if args.delete else 'MOVE duplicates'}")
        log(fp, f"DryRun: {args.dry_run}")
        log(fp, f"Workers:{args.workers}")
        log(fp, f"FailFast:{args.fail_fast}")
        if not PIL_AVAILABLE:
            log(fp, "Note: Pillow 미설치 -> EXIF 촬영일 미사용, mtime 기준 월 분류")
            log(fp, "      EXIF 사용: pip install pillow")
        log(fp, "")

        # 1) size grouping
        by_size = defaultdict(list)
        total_bytes = 0
        stat_fail = 0

        for p in images:
            try:
                st = p.stat()
                total_bytes += st.st_size
                by_size[st.st_size].append(p)
            except Exception as e:
                stat_fail += 1
                fail_or_continue(fp, args.fail_fast, f"[ERR] stat failed: {p}", e)

        log(fp, f"Total images found: {len(images)}")
        log(fp, f"Total input size: {format_bytes(total_bytes)}")
        if stat_fail:
            log(fp, f"Stat failures: {stat_fail}")
        log(fp, "")

        # 2) quick-hash only for size groups with >1
        size_groups = [g for g in by_size.values() if len(g) > 1]
        q_targets = [str(p) for g in size_groups for p in g]

        errors_hash = 0

        quick_map = defaultdict(list)  # (size, quickhex) -> [Path...]
        if q_targets:
            try:
                if args.workers == 1:
                    # single-process: simpler + more predictable
                    for ps in q_targets:
                        try:
                            _, qh = quick_hash(ps, args.head_tail_kb)
                            p = Path(ps)
                            sz = p.stat().st_size
                            quick_map[(sz, qh)].append(p)
                        except Exception as e:
                            errors_hash += 1
                            fail_or_continue(fp, args.fail_fast, f"[ERR] quick-hash failed: {ps}", e)
                else:
                    with ProcessPoolExecutor(max_workers=args.workers) as ex:
                        futs = {ex.submit(quick_hash, ps, args.head_tail_kb): ps for ps in q_targets}
                        for fut in as_completed(futs):
                            ps = futs[fut]
                            try:
                                _, qh = fut.result()
                                p = Path(ps)
                                sz = p.stat().st_size
                                quick_map[(sz, qh)].append(p)
                            except Exception as e:
                                errors_hash += 1
                                fail_or_continue(fp, args.fail_fast, f"[ERR] quick-hash failed: {ps}", e)
            except Exception as e:
                fail_or_continue(fp, args.fail_fast, "[ERR] quick-hash stage crashed", e)

        # 3) full-hash for quick collisions
        full_targets = [str(p) for (_, _), lst in quick_map.items() if len(lst) > 1 for p in lst]
        full_groups = defaultdict(list)  # fullhex -> [Path...]
        if full_targets:
            try:
                if args.workers == 1:
                    for ps in full_targets:
                        try:
                            _, fh = full_hash(ps, args.chunk_mb)
                            full_groups[fh].append(Path(ps))
                        except Exception as e:
                            errors_hash += 1
                            fail_or_continue(fp, args.fail_fast, f"[ERR] full-hash failed: {ps}", e)
                else:
                    with ProcessPoolExecutor(max_workers=args.workers) as ex:
                        futs = {ex.submit(full_hash, ps, args.chunk_mb): ps for ps in full_targets}
                        for fut in as_completed(futs):
                            ps = futs[fut]
                            try:
                                _, fh = fut.result()
                                full_groups[fh].append(Path(ps))
                            except Exception as e:
                                errors_hash += 1
                                fail_or_continue(fp, args.fail_fast, f"[ERR] full-hash failed: {ps}", e)
            except Exception as e:
                fail_or_continue(fp, args.fail_fast, "[ERR] full-hash stage crashed", e)

        duplicate_hashes = {h for h, lst in full_groups.items() if len(lst) > 1}
        keep_for_hash = {h: choose_keep(lst) for h, lst in full_groups.items() if len(lst) > 1}

        # file -> fullhash lookup only for those hashed fully
        file_to_fullhash = {}
        for h, lst in full_groups.items():
            for p in lst:
                file_to_fullhash[str(p)] = h

        # planners
        planner_unique = YMFolderPlanner(out_root, args.limit, args.dry_run, fp, args.fail_fast)
        planner_dups = YMFolderPlanner(dup_root, args.limit, args.dry_run, fp, args.fail_fast)

        moved_unique = 0
        handled_dups = 0
        saved_bytes = 0
        move_errors = 0
        delete_errors = 0

        # 4) apply moves/deletes
        for p in images:
            sp = str(p)
            fh = file_to_fullhash.get(sp)

            try:
                if fh in duplicate_hashes:
                    keep_p = keep_for_hash[fh]
                    if p == keep_p:
                        yyyy, mm = get_year_month(p)
                        dst = planner_unique.next_destination(yyyy, mm, p.name)
                        if args.dry_run:
                            log(fp, f"[KEEP->ORG] {p} -> {dst} (hash={fh})")
                        else:
                            safe_move(p, dst)
                        moved_unique += 1
                    else:
                        saved_bytes += p.stat().st_size
                        if args.delete:
                            if args.dry_run:
                                log(fp, f"[DEL DUP] {p} (kept={keep_p}, hash={fh})")
                            else:
                                safe_delete(p)
                        else:
                            yyyy, mm = get_year_month(p)
                            dst = planner_dups.next_destination(yyyy, mm, p.name)
                            if args.dry_run:
                                log(fp, f"[MOVE DUP] {p} -> {dst} (kept={keep_p}, hash={fh})")
                            else:
                                safe_move(p, dst)
                        handled_dups += 1
                else:
                    yyyy, mm = get_year_month(p)
                    dst = planner_unique.next_destination(yyyy, mm, p.name)
                    if args.dry_run:
                        log(fp, f"[MOVE] {p} -> {dst}")
                    else:
                        safe_move(p, dst)
                    moved_unique += 1

            except Exception as e:
                # errors here are critical because they may stop mid-run
                if args.delete:
                    delete_errors += 1
                else:
                    move_errors += 1
                fail_or_continue(fp, args.fail_fast, f"[ERR] apply stage failed: {p}", e)

        elapsed = time.time() - t0
        log(fp, "")
        log(fp, "=== Summary ===")
        log(fp, f"Unique moved: {moved_unique}")
        log(fp, f"Duplicates handled: {handled_dups}")
        log(fp, f"Saved space from duplicates: {format_bytes(saved_bytes)}")
        log(fp, f"Hash errors: {errors_hash}")
        log(fp, f"Move errors: {move_errors}")
        log(fp, f"Delete errors: {delete_errors}")
        log(fp, f"Elapsed: {elapsed:.2f}s")

    print(f"Done. Report written to: {report_path}")


if __name__ == "__main__":
    # For Windows frozen executables safety (harmless otherwise)
    try:
        import multiprocessing as mp
        mp.freeze_support()
    except Exception:
        pass

    main()
