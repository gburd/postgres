# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/003_constraints.pl.

Checks that constraints (FK) are ignored and replica triggers fire on the
subscriber.
"""

_REPLICA_TRIGGER = """
CREATE FUNCTION filter_basic_dml_fn() RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        IF (NEW.id < 10) THEN
            RETURN NEW;
        ELSE
            RETURN NULL;
        END IF;
    ELSIF (TG_OP = 'UPDATE') THEN
        RETURN NULL;
    ELSE
        RAISE WARNING 'Unknown action';
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER filter_basic_dml_trg
    BEFORE INSERT OR UPDATE OF bid ON tab_fk_ref
    FOR EACH ROW EXECUTE PROCEDURE filter_basic_dml_fn();
ALTER TABLE tab_fk_ref ENABLE REPLICA TRIGGER filter_basic_dml_trg;
"""


def test_constraints(create_pg):
    """FK constraints are ignored and replica triggers fire on the subscriber."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")

    publisher.safe_psql("CREATE TABLE tab_fk (bid int PRIMARY KEY);")
    publisher.safe_psql(
        "CREATE TABLE tab_fk_ref "
        "(id int PRIMARY KEY, junk text, bid int REFERENCES tab_fk (bid));"
    )

    # Subscriber structure; column order intentionally different.
    subscriber.safe_psql("CREATE TABLE tab_fk (bid int PRIMARY KEY);")
    subscriber.safe_psql(
        "CREATE TABLE tab_fk_ref "
        "(id int PRIMARY KEY, bid int REFERENCES tab_fk (bid), junk text);"
    )

    publisher_connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR ALL TABLES;")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '{}' PUBLICATION tap_pub "
        "WITH (copy_data = false)".format(publisher_connstr)
    )
    publisher.wait_for_catchup("tap_sub")

    publisher.safe_psql("INSERT INTO tab_fk (bid) VALUES (1);")
    # "junk" large enough to force out-of-line storage.
    publisher.safe_psql(
        "INSERT INTO tab_fk_ref (id, bid, junk) "
        "VALUES (1, 1, repeat(pi()::text,20000));"
    )
    publisher.wait_for_catchup("tap_sub")

    assert (
        subscriber.safe_psql("SELECT count(*), min(bid), max(bid) FROM tab_fk;")
        == "1|1|1"
    ), "check replicated tab_fk inserts on subscriber"
    assert (
        subscriber.safe_psql("SELECT count(*), min(bid), max(bid) FROM tab_fk_ref;")
        == "1|1|1"
    ), "check replicated tab_fk_ref inserts on subscriber"

    # Drop the FK on the publisher and insert; FK is not enforced on subscriber.
    publisher.safe_psql("DROP TABLE tab_fk CASCADE;")
    publisher.safe_psql("INSERT INTO tab_fk_ref (id, bid) VALUES (2, 2);")
    publisher.wait_for_catchup("tap_sub")
    assert (
        subscriber.safe_psql("SELECT count(*), min(bid), max(bid) FROM tab_fk_ref;")
        == "2|1|2"
    ), "check FK ignored on subscriber"

    subscriber.safe_psql(_REPLICA_TRIGGER)

    # Trigger skips the insert (id >= 10) on the subscriber.
    publisher.safe_psql("INSERT INTO tab_fk_ref (id, bid) VALUES (10, 10);")
    publisher.wait_for_catchup("tap_sub")
    assert (
        subscriber.safe_psql("SELECT count(*), min(bid), max(bid) FROM tab_fk_ref;")
        == "2|1|2"
    ), "check replica insert trigger applied on subscriber"

    # Trigger skips the update.
    publisher.safe_psql("UPDATE tab_fk_ref SET bid = 2 WHERE bid = 1;")
    publisher.wait_for_catchup("tap_sub")
    assert (
        subscriber.safe_psql("SELECT count(*), min(bid), max(bid) FROM tab_fk_ref;")
        == "2|1|2"
    ), "check replica update column trigger applied on subscriber"

    # Update on another column still fires the trigger (all columns shipped).
    publisher.safe_psql("UPDATE tab_fk_ref SET id = 6 WHERE id = 1;")
    publisher.wait_for_catchup("tap_sub")
    assert (
        subscriber.safe_psql("SELECT count(*), min(id), max(id) FROM tab_fk_ref;")
        == "2|1|2"
    ), "check column trigger applied even on update for other column"

    subscriber.stop("fast")
    publisher.stop("fast")
