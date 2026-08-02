from src.etl.sql_templates import render_sql


def test_render_sql_substitutes_known_params():
    rendered = render_sql(
        "warehouse/merge_fact_sales.sql",
        project_id="demo-project",
        bq_location="asia-southeast2",
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    assert "params." not in rendered
    assert "demo-project" in rendered
    assert "DATE('2024-01-01')" in rendered


def test_render_sql_raises_on_missing_param():
    try:
        render_sql("warehouse/merge_dim_product.sql")
    except KeyError:
        return
    raise AssertionError("expected KeyError for missing project_id")
