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


# --------------------
# Logging: console-only vs report
# --------------------
def log_console(msg: str):
    print(msg, flush=True)

def log_report(fp, msg: str):
    fp.write(msg + "\n")
    fp.flush()

def log_both(fp, msg: str):
    log_report(fp, msg)
    log_console(msg)


# --------------------
# Heartbeat file (Explorer에서도 "돌고 있음" 확인)
# --------------------
def heartbeat(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"RUNNING {datetime.now().isoformat()}\n", encoding="utf-8")


def fail_or_continue(fp, fail_fast: bool, msg: str, exc: Exception | None = None):
    if exc is not None:
        log_both(fp, f"{msg} | EXC: {exc}")
    else:
        log_both(fp, msg)
    if fail_fast:
        raise RuntimeError(msg) from exc


# --------------------
# Hash helpers with timeout (best-effort on Windows)
# --------------------
class HashTimeout(Exception):
    pass

def _blake2b_small(data: bytes) -> str:
    h = hashlib.blake2b(digest_size=20)
    h.update(data)
    return h.hexdigest()

def _read_with_timeout(f, nbytes: int, timeout_sec: float, start_t: float, chunk: int = 4096) -> bytes:
    """Read nbytes in small chunks, checking elapsed time."""
    out = bytearray()
    remain = nbytes
    while remain > 0:
        if time.monotonic() - start_t > timeout_sec:
            raise HashTimeout("read timeout")
        to_read = chunk if remain > chunk else remain
        b = f.read(to_read)
        if not b:
            break
        out.extend(b)
        remain -= len(b)
    return bytes(out)

def quick_hash_timeout(path_str: str, head_tail_kb: int, timeout_sec: float) -> tuple[str, str] | tuple[str, None]:
    """
    Returns (path, quickhex) or (path, None) if skipped (timeout/err).
    """
    p = Path(path_str)
    start_t = time.monotonic()
    try:
        k = head_tail_kb * 1024
        size = p.stat().st_size
        with p.open("rb") as f:
            head = _read_with_timeout(f, k, timeout_sec, start_t)
            if size > k:
                # seek는 보통 빠르지만, 그래도 timeout 체크는 유지
                if time.monotonic() - start_t > timeout_sec:
                    raise HashTimeout("timeout before tail seek")
                f.seek(max(0, size - k))
                tail = _read_with_timeout(f, k, timeout_sec, start_t)
            else:
                tail = b""
        return (path_str, _blake2b_small(head + tail))
    except HashTimeout:
        return (path_str, None)
    except Exception:
        return (path_str, None)

def full_hash_timeout(path_str: str, chunk_mb: int, timeout_sec: float) -> tuple[str, str] | tuple[str, None]:
    """
    Returns (path, fullhex) or (path, None) if skipped (timeout/err).
    """
    p = Path(path_str)
    start_t = time.monotonic()
    try:
        h = hashlib.blake2b(digest_size=32)
        chunk = chunk_mb * 1024 * 1024
        with p.open("rb") as f:
            while True:
                if time.monotonic() - start_t > timeout_sec:
                    raise HashTimeout("full-hash timeout")
                b = f.read(chunk)
                if not b:
                    break
                h.update(b)
        return (path_str, h.hexdigest())
    except HashTimeout:
        return (path_str, None)
    except Exception:
        return (path_str, None)


# --------------------
# Output folder planner (YYYY/MM + split into 001 only after >limit)
# --------------------
def list_files_in_dir(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    return [p for p in dir_path.iterdir() if p.is_file()]

def move_month_root_files_into_001(month_root: Path, dry_run: bool, fp, fail_fast: bool):
    root_files = list_files_in_dir(month_root)
    if not root_files:
        return
    target_dir = month_root / "001"
    if dry_run:
        # 이건 정책 변경이므로 report에 남길 가치가 큼
        log_report(fp, f"[SPLIT-INIT] Would move {len(root_files)} files: {month_root} -> {target_dir}")
        return
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        for f in root_files:
            safe_move(f, target_dir / f.name)
        log_report(fp, f"[SPLIT-INIT] Moved {len(root_files)} files into {target_dir}")
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
                # 정책 전환은 report에 남김(중요 이벤트)
                log_report(self.fp, f"[SPLIT] Switching to split mode for {yyyy}/{mm} (limit={self.limit})")
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


def choose_keep(paths: list[Path]) -> Path:
    def key(p: Path):
        return (len(p.name), str(p).lower())
    return min(paths, key=key)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input")
    ap.add_argument("output")
    ap.add_argument("--dups", default="DUPLICATES")
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--delete", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--fail-fast", action="store_true")
    ap.add_argument("--report", default="report.txt")
    ap.add_argument("--head-tail-kb", type=int, default=64)
    ap.add_argument("--chunk-mb", type=int, default=2)
    ap.add_argument("--hash-timeout-sec", type=float, default=10.0, help="한 파일 해시 처리 타임아웃(초)")
    ap.add_argument("--progress-every", type=int, default=500, help="콘솔 진행 로그 주기")
    ap.add_argument("--heartbeat-seconds", type=int, default=10)
    args = ap.parse_args()

    in_root = Path(args.input).resolve()
    out_root = Path(args.output).resolve()
    dup_root = Path(args.dups).resolve()
    report_path = Path(args.report).resolve()

    # Start visibility immediately (even in dry-run)
    out_root.mkdir(parents=True, exist_ok=True)
    dup_root.mkdir(parents=True, exist_ok=True)
    running_marker = out_root / ".RUNNING"

    t0 = time.time()
    last_hb = 0.0

    with open(report_path, "w", encoding="utf-8") as fp:
        # report에는 "중요한 것"만
        log_report(fp, f"START {datetime.now().isoformat()}")
        log_report(fp, f"Input={in_root}")
        log_report(fp, f"Output={out_root} Dups={dup_root}")
        log_report(fp, f"DryRun={args.dry_run} Delete={args.delete} Workers={args.workers} FailFast={args.fail_fast}")
        log_report(fp, f"Pillow(EXIF)={PIL_AVAILABLE}")
        log_report(fp, f"HashTimeoutSec={args.hash_timeout_sec}")
        log_report(fp, "")

        # 콘솔에만 스캔 진행 표시
        log_console("[SCAN] walking files... (console-only)")
        by_size = defaultdict(list)
        total_bytes = 0
        files_found = 0

        for p in iter_images_stream(in_root):
            files_found += 1
            try:
                st = p.stat()
                total_bytes += st.st_size
                by_size[st.st_size].append(p)
            except Exception as e:
                fail_or_continue(fp, args.fail_fast, f"[ERR] stat failed: {p}", e)

            if files_found % args.progress_every == 0:
                log_console(f"[SCAN] found={files_found}, total_size={format_bytes(total_bytes)}")

            now = time.time()
            if now - last_hb >= args.heartbeat_seconds:
                heartbeat(running_marker)
                last_hb = now

        log_console(f"[SCAN DONE] images={files_found}, total_size={format_bytes(total_bytes)}")
        log_report(fp, f"SCAN_DONE images={files_found} total_size={format_bytes(total_bytes)}")
        log_report(fp, "")

        # quick-hash 대상
        size_groups = [g for g in by_size.values() if len(g) > 1]
        q_targets = [str(p) for g in size_groups for p in g]
        log_console(f"[PLAN] quick candidates={len(q_targets)}")
        log_report(fp, f"PLAN quick_candidates={len(q_targets)}")

        # quick-hash (콘솔 진행만)
        quick_map = defaultdict(list)
        skipped_quick = 0

        if q_targets:
            log_console("[QUICK] start (console-only)")
            if args.workers == 1:
                for i, ps in enumerate(q_targets, 1):
                    path_str, qh = quick_hash_timeout(ps, args.head_tail_kb, args.hash_timeout_sec)
                    if qh is None:
                        skipped_quick += 1
                        log_report(fp, f"[SKIP QUICK] {path_str} (timeout/err)")
                        continue
                    p = Path(path_str)
                    try:
                        sz = p.stat().st_size
                    except Exception:
                        skipped_quick += 1
                        log_report(fp, f"[SKIP QUICK] {path_str} (stat fail after hash)")
                        continue
                    quick_map[(sz, qh)].append(p)

                    if i % args.progress_every == 0:
                        log_console(f"[QUICK] done={i}/{len(q_targets)} skipped={skipped_quick}")
                        heartbeat(running_marker)

            else:
                # 멀티프로세스: worker가 느린 파일에 오래 걸릴 수 있으니 timeout-sec를 짧게 잡는 걸 추천
                with ProcessPoolExecutor(max_workers=args.workers) as ex:
                    futs = {ex.submit(quick_hash_timeout, ps, args.head_tail_kb, args.hash_timeout_sec): ps for ps in q_targets}
                    done = 0
                    for fut in as_completed(futs):
                        done += 1
                        try:
                            path_str, qh = fut.result()
                            if qh is None:
                                skipped_quick += 1
                                log_report(fp, f"[SKIP QUICK] {path_str} (timeout/err)")
                            else:
                                p = Path(path_str)
                                sz = p.stat().st_size
                                quick_map[(sz, qh)].append(p)
                        except Exception as e:
                            skipped_quick += 1
                            log_report(fp, f"[SKIP QUICK] {futs[fut]} (worker exception: {e})")
                            if args.fail_fast:
                                raise

                        if done % args.progress_every == 0:
                            log_console(f"[QUICK] done={done}/{len(q_targets)} skipped={skipped_quick}")
                            heartbeat(running_marker)

            log_console(f"[QUICK DONE] skipped={skipped_quick}")
            log_report(fp, f"QUICK_DONE skipped={skipped_quick}")

        # full-hash 대상
        full_targets = [str(p) for (_, _), lst in quick_map.items() if len(lst) > 1 for p in lst]
        log_console(f"[PLAN] full candidates={len(full_targets)}")
        log_report(fp, f"PLAN full_candidates={len(full_targets)}")

        full_groups = defaultdict(list)
        skipped_full = 0

        if full_targets:
            log_console("[FULL] start (console-only)")
            if args.workers == 1:
                for i, ps in enumerate(full_targets, 1):
                    path_str, fh = full_hash_timeout(ps, args.chunk_mb, args.hash_timeout_sec)
                    if fh is None:
                        skipped_full += 1
                        log_report(fp, f"[SKIP FULL] {path_str} (timeout/err)")
                        continue
                    full_groups[fh].append(Path(path_str))

                    if i % args.progress_every == 0:
                        log_console(f"[FULL] done={i}/{len(full_targets)} skipped={skipped_full}")
                        heartbeat(running_marker)
            else:
                with ProcessPoolExecutor(max_workers=args.workers) as ex:
                    futs = {ex.submit(full_hash_timeout, ps, args.chunk_mb, args.hash_timeout_sec): ps for ps in full_targets}
                    done = 0
                    for fut in as_completed(futs):
                        done += 1
                        try:
                            path_str, fh = fut.result()
                            if fh is None:
                                skipped_full += 1
                                log_report(fp, f"[SKIP FULL] {path_str} (timeout/err)")
                            else:
                                full_groups[fh].append(Path(path_str))
                        except Exception as e:
                            skipped_full += 1
                            log_report(fp, f"[SKIP FULL] {futs[fut]} (worker exception: {e})")
                            if args.fail_fast:
                                raise

                        if done % args.progress_every == 0:
                            log_console(f"[FULL] done={done}/{len(full_targets)} skipped={skipped_full}")
                            heartbeat(running_marker)

            log_console(f"[FULL DONE] skipped={skipped_full}")
            log_report(fp, f"FULL_DONE skipped={skipped_full}")

        duplicate_hashes = {h for h, lst in full_groups.items() if len(lst) > 1}
        keep_for_hash = {h: choose_keep(lst) for h, lst in full_groups.items() if len(lst) > 1}

        file_to_fullhash = {}
        for h, lst in full_groups.items():
            for p in lst:
                file_to_fullhash[str(p)] = h

        log_report(fp, f"DEDUPE duplicate_groups={len(duplicate_hashes)}")
        log_report(fp, "")

        planner_unique = YMFolderPlanner(out_root, args.limit, args.dry_run, fp, args.fail_fast)
        planner_dups = YMFolderPlanner(dup_root, args.limit, args.dry_run, fp, args.fail_fast)

        moved_unique = 0
        handled_dups = 0
        saved_bytes = 0

        # apply는 console로만 진행률, report에는 요약만
        log_console("[APPLY] start")
        all_paths = [p for group in by_size.values() for p in group]
        processed = 0

        for p in all_paths:
            processed += 1
            fh = file_to_fullhash.get(str(p))

            try:
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
            except Exception as e:
                fail_or_continue(fp, args.fail_fast, f"[ERR] apply failed: {p}", e)

            if processed % args.progress_every == 0:
                log_console(f"[APPLY] processed={processed}/{len(all_paths)} unique={moved_unique} dups={handled_dups}")
                heartbeat(running_marker)

        elapsed = time.time() - t0
        log_report(fp, "=== Summary ===")
        log_report(fp, f"Unique={moved_unique}")
        log_report(fp, f"DupsHandled={handled_dups}")
        log_report(fp, f"SavedSpace={format_bytes(saved_bytes)}")
        log_report(fp, f"SkippedQuick={skipped_quick}")
        log_report(fp, f"SkippedFull={skipped_full}")
        log_report(fp, f"ElapsedSec={elapsed:.2f}")

    # mark done
    try:
        running_marker.unlink(missing_ok=True)
        (out_root / ".DONE").write_text(f"DONE {datetime.now().isoformat()}\n", encoding="utf-8")
    except Exception:
        pass

    log_console(f"Done. Report: {report_path}")


if __name__ == "__main__":
    try:
        import multiprocessing as mp
        mp.freeze_support()
    except Exception:
        pass
    main()
