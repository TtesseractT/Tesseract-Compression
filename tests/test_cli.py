"""CLI presentation tests."""

from types import SimpleNamespace

import tesseract.cli as cli


class DummyManifest:
    file_count = 4
    unique_count = 3
    duplicate_group_count = 1
    total_original_size = 4096
    space_savings = 1024


class DummyEncoder:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def encode(self, source, output):
        output.write_bytes(b"archive")
        return DummyManifest()


def test_cmd_encode_prints_summary(monkeypatch, tmp_path, capsys):
    source = tmp_path / "source"
    source.mkdir()
    output = tmp_path / "archive.tesseract"

    monkeypatch.setattr(cli, "TesseractEncoder", DummyEncoder)

    args = SimpleNamespace(
        source=str(source),
        output=str(output),
        password=None,
        encrypt=False,
        workers=1,
        compression_level=9,
        exclude=[],
        solid=False,
        recovery=0,
        comment="",
        permissions=False,
        lock=False,
    )

    cli.cmd_encode(args)

    stdout = capsys.readouterr().out
    assert "Archive created" in stdout
    assert "Unique stored" in stdout
    assert "Dedup savings" in stdout