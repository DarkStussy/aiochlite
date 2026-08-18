import pytest

from aiochlite.core.models import Row


class TestRow:
    """Tests for the Row mapping wrapper."""

    def test_attribute_access_via_index(self):
        names = ["id", "name"]
        row = Row(names, [1, "alice"], index={"id": 0, "name": 1})
        assert row.id == 1
        assert row.name == "alice"

    def test_attribute_access_without_index(self):
        row = Row(["id", "name"], [1, "alice"])
        assert row.id == 1
        assert row.name == "alice"

    def test_attribute_missing_raises(self):
        indexed = Row(["id"], [1], index={"id": 0})
        with pytest.raises(AttributeError, match="missing"):
            _ = indexed.missing

        fallback = Row(["id"], [1])
        with pytest.raises(AttributeError, match="missing"):
            _ = fallback.missing

    def test_getitem_via_index(self):
        row = Row(["id", "name"], [1, "alice"], index={"id": 0, "name": 1})
        assert row["id"] == 1
        assert row["name"] == "alice"

    def test_getitem_without_index(self):
        row = Row(["id", "name"], [1, "alice"])
        assert row["id"] == 1
        assert row["name"] == "alice"

    def test_getitem_missing_raises_keyerror(self):
        row = Row(["id"], [1])
        with pytest.raises(KeyError):
            _ = row["missing"]

    def test_iter_returns_column_names(self):
        row = Row(["a", "b", "c"], [1, 2, 3])
        assert list(row) == ["a", "b", "c"]

    def test_len(self):
        row = Row(["a", "b", "c"], [1, 2, 3])
        assert len(row) == 3

    def test_repr_includes_columns(self):
        row = Row(["id", "name"], [1, "alice"])
        result = repr(row)
        assert result.startswith("Row(")
        assert "'id': 1" in result
        assert "'name': 'alice'" in result

    def test_first(self):
        row = Row(["a", "b"], [42, "x"])
        assert row.first() == 42

    def test_first_empty_values(self):
        row = Row([], [])
        assert row.first() is None

    def test_mapping_interface(self):
        row = Row(["id", "name"], [1, "alice"], index={"id": 0, "name": 1})
        assert list(row.keys()) == ["id", "name"]
        assert list(row.values()) == [1, "alice"]
        assert list(row.items()) == [("id", 1), ("name", "alice")]
        assert "id" in row
        assert "missing" not in row

    def test_row_holds_no_per_row_dict(self):
        """One dict per row would cost more to hold than the values themselves."""
        row = Row(["id"], [1], {"id": 0})
        assert Row.__slots__ == ("_index", "_names", "_values")
        assert not hasattr(row, "__dict__")

    def test_duplicate_names_keep_the_column_count(self):
        """The server disambiguates duplicates, but length and iteration must not depend on that."""
        row = Row(["a", "a"], [1, 2])
        assert len(row) == 2
        assert list(row) == ["a", "a"]
        assert row["a"] == 2
