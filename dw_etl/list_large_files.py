from pathlib import Path

from config import DATA_DIR, OUT_DIR, PROJECT_ROOT


def human_mb(num_bytes: int) -> float:
    return num_bytes / (1024 * 1024)


def list_large_files(base: Path, threshold_mb: float = 50.0) -> None:
    """
    List files under a base directory that are larger than threshold_mb.

    This is meant as a small, temporary helper so you can quickly see which
    input/output files are heavy before deciding what to sample or move.
    """
    threshold_bytes = threshold_mb * 1024 * 1024
    print(f"\nScanning '{base}' for files > {threshold_mb} MB:")

    if not base.exists():
        print(f"  (directory does not exist)")
        return

    any_large = False
    for p in sorted(base.rglob("*")):
        if not p.is_file():
            continue
        size = p.stat().st_size
        if size >= threshold_bytes:
            any_large = True
            rel = p.relative_to(PROJECT_ROOT)
            print(f"  - {rel} : {human_mb(size):.1f} MB")

    if not any_large:
        print("  (no files above threshold)")


def main() -> None:
    # Default threshold: 50 MB
    threshold_mb = 50.0
    print(f"=== Large file listing (>{threshold_mb} MB) ===")
    list_large_files(DATA_DIR, threshold_mb)
    list_large_files(OUT_DIR, threshold_mb)


if __name__ == "__main__":
    main()

