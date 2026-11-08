"""Reconciliation must pass on a good load and FAIL when the output is corrupted.

A check that can never fail proves nothing, so most tests here damage a copy of a
good load in one specific way and assert exactly which checks catch it.
"""
import json
import shutil
import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from skew_pipeline.generate_data import generate
from skew_pipeline.ledger import read_batch
from skew_pipeline.reconcile import main as reconcile_main
from skew_pipeline.reconcile import reconcile, to_markdown
from skew_pipeline.run_pipeline import Config, run
from skew_pipeline.sample_queries import QUERIES, run_queries

HOT = "C0000001"


@pytest.fixture(scope="module")
def world(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("recon")
    expected = generate(root, 40, 400, 0.7, 2, seed=31)
    out = root / "out"
    s = run(Config(str(root / "input/customers"), str(root / "input/orders"), str(out),
                   run_id="rc1", shuffle_partitions=2, master="local[2]"), spark)
    return {"root": root, "out": out, "batch": s["batch_id"], "expected": expected}


@pytest.fixture(scope="module")
def clean_report(spark, world):
    return reconcile(spark, str(world["out"]))


def status(report):
    return {c["id"]: c["status"] for c in report["checks"]}


def copy_world(world, tmp_path):
    """Copy data + output and repoint the run logs, so a test may damage the copy freely."""
    root = tmp_path / "w"
    shutil.copytree(world["root"], root)
    for f in (root / "out/run_log").glob("*.json"):
        # parse the JSON instead of editing its text: on Windows the paths are stored with doubled
        # backslashes, so a plain text replace would silently keep pointing at the original files
        data = _repoint(json.loads(f.read_text()), str(world["root"]), str(root))
        f.write_text(json.dumps(data))
    return root, root / "out"


def _repoint(obj, old, new):
    if isinstance(obj, str):
        return obj.replace(old, new)
    if isinstance(obj, list):
        return [_repoint(x, old, new) for x in obj]
    if isinstance(obj, dict):
        return {k: _repoint(v, old, new) for k, v in obj.items()}
    return obj


def rewrite(spark, out, table, batch, fn):
    """Replace one table partition with fn(list of row dicts)."""
    df = read_batch(spark, out, table, batch)
    rows = fn([r.asDict() for r in df.collect()])
    cols, schema = df.columns, df.schema
    new = spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], schema)
    shutil.rmtree(Path(out) / table / f"batch_id={batch}")
    new.coalesce(1).write.parquet(str(Path(out) / table / f"batch_id={batch}"))


# ---------------------------------------------------------------- good load
def test_clean_load_passes_every_check(clean_report):
    assert clean_report["result"] == "PASS"
    assert clean_report["summary"]["failed"] == 0 and clean_report["summary"]["skipped"] == 0
    st = status(clean_report)
    for must in ("R0", "R1-orders", "R1-customers", "K-orders", "FK", "P1", "P2", "P3", "T1", "T2",
                 "T3", "T4-orders", "L1", "L2", "E3-orders", "E4", "E5"):
        assert st[must] == "PASS", must


def test_row_flow_explains_every_difference(world, clean_report):
    exp = world["expected"]["orders"]
    f = clean_report["row_flow"]["orders"]
    assert f["input"] == exp["input_rows"]
    assert f["input"] == f["valid"] + f["rejected_parse"] + f["rejected_dedup"] + f["rejected_referential"]
    md = to_markdown(clean_report)
    assert "Row flow" in md and "**Result: PASS**" in md


# ------------------------------------------------------------- damaged loads
def test_missing_and_altered_published_rows_are_caught(spark, world, tmp_path):
    _, out = copy_world(world, tmp_path)
    victims = sorted(r["customer_id"] for r in read_batch(spark, out, "published/customer_order_summary",
                                                         world["batch"]).collect() if r["customer_id"] != HOT)

    def damage(rows):
        rows = [r for r in rows if r["customer_id"] != victims[0]]            # lose a customer
        for r in rows:
            if r["customer_id"] == victims[1]:                                  # +0.01 on another
                r["total_amount"] = r["total_amount"] + type(r["total_amount"])("0.01")
        return rows

    rewrite(spark, out, "published/customer_order_summary", world["batch"], damage)
    st = status(reconcile(spark, str(out)))
    assert all(st[c] == "FAIL" for c in ("P1", "P2", "P3", "L1")), st
    assert all(st[c] == "PASS" for c in ("K-orders", "FK", "T1", "R1-orders")), st


def test_lost_and_untraceable_rejects_are_caught(spark, world, tmp_path):
    _, out = copy_world(world, tmp_path)

    def damage(rows):
        parse = [i for i, r in enumerate(rows) if r["entity"] == "orders" and r["reject_stage"] == "parse"]
        rows[parse[1]]["source_file"] = None                  # lose traceability
        rows[parse[2]]["reject_reason"] = "made_up_reason"    # outside the vocabulary
        del rows[parse[0]]                                    # silently drop a rejected row
        return rows

    rewrite(spark, out, "rejects", world["batch"], damage)
    st = status(reconcile(spark, str(out)))
    assert all(st[c] == "FAIL" for c in ("R1-orders", "T1", "T2", "L1", "L2", "E3-orders")), st
    assert all(st[c] == "PASS" for c in ("K-orders", "P1", "FK")), st


def test_duplicate_key_and_orphan_order_in_staging_are_caught(spark, world, tmp_path):
    _, out = copy_world(world, tmp_path)

    def damage(rows):
        rows.append(dict(rows[0]))                                             # same order_id twice
        orphan = dict(rows[1]); orphan.update(order_id="O_ORPHAN", customer_id="C404")
        rows.append(orphan)                                                    # no such customer
        return rows

    rewrite(spark, out, "staging/orders_valid", world["batch"], damage)
    st = status(reconcile(spark, str(out)))
    assert all(st[c] == "FAIL" for c in ("K-orders", "FK", "P1", "P2", "R1-orders")), st
    assert all(st[c] == "PASS" for c in ("K-customers", "T1", "N-orders")), st


def test_input_changed_after_the_load_is_caught(spark, world, tmp_path):
    root, out = copy_world(world, tmp_path)
    f = sorted((root / "input/orders").glob("*.csv"))[0]
    with f.open("a") as fh:
        fh.write("O999999999,C0000002,5.00,PLACED,2024-02-02T10:00:00Z\n")
    st = status(reconcile(spark, str(out)))
    assert all(st[c] == "FAIL" for c in ("R0", "R1-orders", "E2-orders")), st
    assert all(st[c] == "PASS" for c in ("P1", "K-orders", "T1")), st


def test_wrong_expected_labels_are_caught(spark, world, tmp_path):
    exp = json.loads((world["root"] / "expected/expected_results.json").read_text())
    exp["orders"]["valid_total_amount"] = "1.00"
    exp["orders"]["rejected_by_reason"]["invalid_status"] += 1
    bad = tmp_path / "expected_bad.json"
    bad.write_text(json.dumps(exp))
    st = status(reconcile(spark, str(world["out"]), expected_path=str(bad)))
    assert st["E4"] == "FAIL" and st["E3-orders"] == "FAIL"
    assert st["R1-orders"] == "PASS" and st["P1"] == "PASS"


def test_unavailable_inputs_are_skipped_not_passed(spark, world, tmp_path):
    root, out = copy_world(world, tmp_path)
    shutil.rmtree(root / "input")
    r = reconcile(spark, str(out))
    st = status(r)
    assert st["R1-orders"] == "SKIP" and st["R1-customers"] == "SKIP" and "R0" not in st
    assert st["P1"] == "PASS" and r["result"] == "PASS"       # skipped is visible, never counted as pass
    assert r["summary"]["skipped"] >= 2


# ------------------------------------------------------------ command line
def test_cli_exit_code_and_report_files(spark, world, tmp_path, monkeypatch):
    _, bad_out = copy_world(world, tmp_path)
    rewrite(spark, bad_out, "published/customer_order_summary", world["batch"], lambda rows: rows[1:])
    for out, code in ((world["out"], 0), (bad_out, 1)):
        monkeypatch.setattr(sys, "argv", ["reconcile", "--output", str(out)])
        with pytest.raises(SystemExit) as e:
            reconcile_main()
        assert e.value.code == code
        reports = Path(out) / "reconciliation"
        assert list(reports.glob("reconciliation_*.json")) and list(reports.glob("reconciliation_*.md"))
    assert "**Result: FAIL**" in next((Path(bad_out) / "reconciliation").glob("*.md")).read_text()


# ---------------------------------------------------------- sample queries
def test_sample_queries_show_the_expected_results(spark, world):
    md, results = run_queries(spark, str(world["out"]))
    exp = world["expected"]
    top = results[next(iter(QUERIES))][0]
    assert top["customer_id"] == HOT and top["order_count"] == exp["orders"]["valid_orders_for_hot_customer"]
    rejects_total = sum(r["rows"] for r in results[[t for t in QUERIES if t.startswith("Rejected")][0]])
    assert rejects_total == sum(sum(exp[e]["rejected_by_reason"].values()) for e in ("customers", "orders"))
    sanity = results[[t for t in QUERIES if t.startswith("Reconciliation sanity")][0]][0]
    assert sanity["published_orders"] == sanity["valid_orders"] == exp["orders"]["valid_rows"]
    assert sanity["published_amount"] == sanity["valid_amount"]
    assert all(title in md for title in QUERIES)
