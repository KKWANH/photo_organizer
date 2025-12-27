import argparse
import os
import time
import shutil
import hashlib
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


def iter_images_stream(root: Path):
    """Generator: yields image paths without materializing full list."""
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
            yield p


def get_year_month(path: Path) -> tuple[str, str]:
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
    def key(p: Path):
        return (len(p.name), str(p).lower())
    return min(paths, key=key)


def list_files_in_dir(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    return [p for p in dir_path.iterdir() if p.is_file()]


def log(fp, msg: str, also_print: bool = True):
    fp.write(msg + "\n")
    fp.flush()
    if also_print:
        print(msg, flush=True)


def heartbeat(path: Path):
    # update timestamp in a file so user can see activity in Explorer
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"RUNNING {datetime.now().isoformat()}\n", encoding="utf-8")


def fail_or_continue(fp, fail_fast: bool, msg: str, exc: Exception | None = None):
    if exc is not None:
        log(fp, f"{msg} | EXC: {exc}")
    else:
        log(fp, msg)
    if fail_fast:
        raise RuntimeError(msg) from exc


def move_month_root_files_into_001(month_root: Path, dry_run: bool, fp, fail_fast: bool):
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
    def __init__(self, base_root: Path, limit: int, dry_run: bool, fp, fail_fast: bool):
        self.base_root = base_root
        self.limit = limit
        self.dry_run = dry_run
        self.fp = fp
        self.fail_fast = fail_fast
        self.state = {}

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
    ap.add_argument("--delete", action="store_true", help="중복 파일 영구 삭제(주의)")
    ap.add_argument("--dry-run", action="store_true", help="실제 이동/삭제 없이 로그만")
    ap.add_argument("--workers", type=int, default=1, help="해시 워커 수(기본 1)")
    ap.add_argument("--fail-fast", action="store_true", help="에러 즉시 중단")
    ap.add_argument("--report", default="report.txt", help="리포트 경로")
    ap.add_argument("--head-tail-kb", type=int, default=64)
    ap.add_argument("--chunk-mb", type=int, default=2)
    ap.add_argument("--progress-every", type=int, default=500, help="스캔/처리 진행 로그 주기(개수)")
    ap.add_argument("--heartbeat-seconds", type=int, default=10, help="RUNNING 파일 갱신 주기(초)")
    args = ap.parse_args()

    in_root = Path(args.input).resolve()
    out_root = Path(args.output).resolve()
    dup_root = Path(args.dups).resolve()
    report_path = Path(args.report).resolve()

    # Create base folders immediately (even in dry-run) so you can see something
    out_root.mkdir(parents=True, exist_ok=True)
    dup_root.mkdir(parents=True, exist_ok=True)
    running_marker = out_root / ".RUNNING"

    t0 = time.time()
    last_hb = 0.0

    with open(report_path, "w", encoding="utf-8") as fp:
        log(fp, f"START {datetime.now().isoformat()}")
        log(fp, f"Input: {in_root}")
        log(fp, f"Output: {out_root} | Dups: {dup_root}")
        log(fp, f"DryRun={args.dry_run} Delete={args.delete} Workers={args.workers} FailFast={args.fail_fast}")
        log(fp, f"Pillow(EXIF)={PIL_AVAILABLE}")
        log(fp, "")

        # ---- Scan & stat (this can take long; show progress) ----
        by_size = defaultdict(list)
        total_bytes = 0
        files_found = 0

        log(fp, "[SCAN] walking files... (this can take a while on HDD)")

        for p in iter_images_stream(in_root):
            files_found += 1
            try:
                st = p.stat()
                total_bytes += st.st_size
                by_size[st.st_size].append(p)
            except Exception as e:
                fail_or_continue(fp, args.fail_fast, f"[ERR] stat failed: {p}", e)

            if files_found % args.progress_every == 0:
                log(fp, f"[SCAN] found={files_found}, total_size={format_bytes(total_bytes)}", also_print=True)

            now = time.time()
            if now - last_hb >= args.heartbeat_seconds:
                heartbeat(running_marker)
                last_hb = now

        log(fp, f"[SCAN DONE] images={files_found}, total_size={format_bytes(total_bytes)}")
        log(fp, "")

        # ---- Hash planning ----
        size_groups = [g for g in by_size.values() if len(g) > 1]
        q_targets = [str(p) for g in size_groups for p in g]
        log(fp, f"[PLAN] size-collision candidates={len(q_targets)} (need quick-hash)")
        heartbeat(running_marker)

        # ---- Quick-hash ----
        quick_map = defaultdict(list)
        errors_hash = 0

        if q_targets:
            log(fp, "[QUICK] start")
            if args.workers == 1:
                for i, ps in enumerate(q_targets, 1):
                    try:
                        _, qh = quick_hash(ps, args.head_tail_kb)
                        p = Path(ps)
                        quick_map[(p.stat().st_size, qh)].append(p)
                    except Exception as e:
                        errors_hash += 1
                        fail_or_continue(fp, args.fail_fast, f"[ERR] quick-hash failed: {ps}", e)

                    if i % args.progress_every == 0:
                        log(fp, f"[QUICK] done={i}/{len(q_targets)}", also_print=True)
                        heartbeat(running_marker)
            else:
                with ProcessPoolExecutor(max_workers=args.workers) as ex:
                    futs = {ex.submit(quick_hash, ps, args.head_tail_kb): ps for ps in q_targets}
                    done = 0
                    for fut in as_completed(futs):
                        ps = futs[fut]
                        done += 1
                        try:
                            _, qh = fut.result()
                            p = Path(ps)
                            quick_map[(p.stat().st_size, qh)].append(p)
                        except Exception as e:
                            errors_hash += 1
                            fail_or_continue(fp, args.fail_fast, f"[ERR] quick-hash failed: {ps}", e)

                        if done % args.progress_every == 0:
                            log(fp, f"[QUICK] done={done}/{len(q_targets)}", also_print=True)
                            heartbeat(running_marker)

            log(fp, "[QUICK DONE]")

        # ---- Full-hash for quick collisions ----
        full_targets = [str(p) for (_, _), lst in quick_map.items() if len(lst) > 1 for p in lst]
        log(fp, f"[PLAN] quick-collision candidates={len(full_targets)} (need full-hash)")
        heartbeat(running_marker)

        full_groups = defaultdict(list)
        if full_targets:
            log(fp, "[FULL] start")
            if args.workers == 1:
                for i, ps in enumerate(full_targets, 1):
                    try:
                        _, fh = full_hash(ps, args.chunk_mb)
                        full_groups[fh].append(Path(ps))
                    except Exception as e:
                        errors_hash += 1
                        fail_or_continue(fp, args.fail_fast, f"[ERR] full-hash failed: {ps}", e)

                    if i % args.progress_every == 0:
                        log(fp, f"[FULL] done={i}/{len(full_targets)}", also_print=True)
                        heartbeat(running_marker)
            else:
                with ProcessPoolExecutor(max_workers=args.workers) as ex:
                    futs = {ex.submit(full_hash, ps, args.chunk_mb): ps for ps in full_targets}
                    done = 0
                    for fut in as_completed(futs):
                        ps = futs[fut]
                        done += 1
                        try:
                            _, fh = fut.result()
                            full_groups[fh].append(Path(ps))
                        except Exception as e:
                            errors_hash += 1
                            fail_or_continue(fp, args.fail_fast, f"[ERR] full-hash failed: {ps}", e)

                        if done % args.progress_every == 0:
                            log(fp, f"[FULL] done={done}/{len(full_targets)}", also_print=True)
                            heartbeat(running_marker)

            log(fp, "[FULL DONE]")

        duplicate_hashes = {h for h, lst in full_groups.items() if len(lst) > 1}
        keep_for_hash = {h: choose_keep(lst) for h, lst in full_groups.items() if len(lst) > 1}

        file_to_fullhash = {}
        for h, lst in full_groups.items():
            for p in lst:
                file_to_fullhash[str(p)] = h

        log(fp, f"[DEDUPE] duplicate_groups={len(duplicate_hashes)}")
        heartbeat(running_marker)

        planner_unique = YMFolderPlanner(out_root, args.limit, args.dry_run, fp, args.fail_fast)
        planner_dups = YMFolderPlanner(dup_root, args.limit, args.dry_run, fp, args.fail_fast)

        moved_unique = 0
        handled_dups = 0
        saved_bytes = 0

        # ---- Apply stage ----
        log(fp, "[APPLY] start (dry-run will not move/delete files)")
        heartbeat(running_marker)

        # If dry-run, we still want visible progress without spamming per-file logs.
        # We only log every progress-every files during apply in dry-run.
        processed = 0

        # We need all images again; we can iterate by by_size values to avoid rescanning disk,
        # but by_size already contains Paths, so we can just flatten:
        all_paths = [p for group in by_size.values() for p in group]

        for p in all_paths:
            processed += 1
            fh = file_to_fullhash.get(str(p))

            if fh in duplicate_hashes:
                keep_p = keep_for_hash[fh]
                if p == keep_p:
                    yyyy, mm = get_year_month(p)
                    dst = planner_unique.next_destination(yyyy, mm, p.name)
                    if not args.dry_run:
                        safe_move(p, dst)
                    moved_unique += 1
                else:
                    saved_bytes += p.stat().st_size
                    if args.delete:
                        if not args.dry_run:
                            safe_delete(p)
                    else:
                        yyyy, mm = get_year_month(p)
                        dst = planner_dups.next_destination(yyyy, mm, p.name)
                        if not args.dry_run:
                            safe_move(p, dst)
                    handled_dups += 1
            else:
                yyyy, mm = get_year_month(p)
                dst = planner_unique.next_destination(yyyy, mm, p.name)
                if not args.dry_run:
                    safe_move(p, dst)
                moved_unique += 1

            if processed % args.progress_every == 0:
                log(fp, f"[APPLY] processed={processed}/{len(all_paths)} | unique={moved_unique} dups={handled_dups}", also_print=True)
                heartbeat(running_marker)

        elapsed = time.time() - t0
        log(fp, "")
        log(fp, "=== Summary ===")
        log(fp, f"Unique: {moved_unique}")
        log(fp, f"Dups handled: {handled_dups}")
        log(fp, f"Saved space (dups): {format_bytes(saved_bytes)}")
        log(fp, f"Hash errors: {errors_hash}")
        log(fp, f"Elapsed: {elapsed:.2f}s")

    # mark completed
    try:
        (out_root / ".RUNNING").unlink(missing_ok=True)
        (out_root / ".DONE").write_text(f"DONE {datetime.now().isoformat()}\n", encoding="utf-8")
    except Exception:
        pass

    print(f"Done. Report: {report_path}")


if __name__ == "__main__":
    try:
        import multiprocessing as mp
        mp.freeze_support()
    except Exception:
        pass
    main()
