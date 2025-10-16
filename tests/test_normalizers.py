from hsbc_data_cleaner.cleaning.normalizers import normalize_line, normalize_lines


def test_normalize_line_whitespace_and_punctuation():
    raw = "  Hello\u3001world！   "
    assert normalize_line(raw) == "Hello, world!"


def test_normalize_lines_filters_empty():
    lines = ["   foo   ", "", "  ", "bar"]
    assert normalize_lines(lines) == ["foo", "bar"]
