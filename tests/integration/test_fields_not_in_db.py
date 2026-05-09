"""Integration test for __fields_not_in_db__: transient fields stay
on the Pydantic model but never reach the DB."""

from etchdb import DB, Row


class Note(Row):
    __table__ = "notes_fnidb"
    __fields_not_in_db__ = ("display_label",)
    id: int
    body: str
    display_label: str = ""


async def test_transient_field_round_trip(db: DB):
    """Insert a Note with a transient display_label set; the column
    isn't written, the read-back Row has the default for the field."""
    await db.execute("DROP TABLE IF EXISTS notes_fnidb")
    try:
        await db.execute("CREATE TABLE notes_fnidb (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")

        await db.insert(Note(id=1, body="hello", display_label="ignored"))
        fetched = await db.get(Note, id=1)

        assert fetched is not None
        assert fetched.id == 1
        assert fetched.body == "hello"
        # Default kicks in because no column was read for this field.
        assert fetched.display_label == ""
    finally:
        await db.execute("DROP TABLE IF EXISTS notes_fnidb")
