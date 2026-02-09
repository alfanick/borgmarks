from pathlib import Path

from borgmarks import __version__


def test_package_version_matches_version_file():
    version_file = (Path(__file__).resolve().parents[1] / "VERSION").read_text(encoding="utf-8").strip()
    assert version_file
    assert __version__ == version_file
