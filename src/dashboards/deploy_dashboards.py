"""
Dashboard Deployment Script

This script deploys dashboards using the DashboardBuilder and generates
configuration files for Databricks Asset Bundles.

Usage:
    python deploy_dashboards.py --catalog dev_catalog --schema nyc_taxi --project nyc_taxi
    python deploy_dashboards.py --environment prod --output-dir resources/dashboards
"""

import argparse
import os
import sys
from pathlib import Path
from dashboard_builder import DashboardBuilder


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Generate dashboard configurations for Databricks Asset Bundles"
    )

    parser.add_argument(
        "--catalog",
        type=str,
        default="${var.catalog_name}",
        help="Unity Catalog name (use variable for template)"
    )

    parser.add_argument(
        "--schema",
        type=str,
        default="${var.schema_name}",
        help="Schema name within catalog"
    )

    parser.add_argument(
        "--project",
        type=str,
        default="${var.project_name}",
        help="Project name"
    )

    parser.add_argument(
        "--environment",
        type=str,
        choices=["dev", "staging", "prod"],
        default="dev",
        help="Target environment"
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="resources/dashboards",
        help="Output directory for dashboard YAML files"
    )

    parser.add_argument(
        "--gold-prefix",
        type=str,
        default="gold_",
        help="Prefix for gold layer tables"
    )

    return parser.parse_args()


def ensure_directory(path: str):
    """Ensure directory exists"""
    Path(path).mkdir(parents=True, exist_ok=True)


def generate_executive_dashboard(builder: DashboardBuilder, output_dir: str):
    """Generate executive dashboard configuration"""
    print("Generating Executive KPI Dashboard...")

    dashboard = builder.create_executive_dashboard()

    # Export JSON for review
    json_path = os.path.join(output_dir, "executive_dashboard.json")
    builder.export_to_json(dashboard, json_path)
    print(f"  ✓ Exported JSON to {json_path}")

    # Export YAML resource
    yaml_resource = builder.export_to_yaml_resource("executive_kpi_dashboard", dashboard)
    yaml_path = os.path.join(output_dir, "executive_kpi.yml")

    with open(yaml_path, 'w') as f:
        f.write(yaml_resource)
    print(f"  ✓ Exported YAML resource to {yaml_path}")

    return dashboard


def generate_operational_dashboard(builder: DashboardBuilder, output_dir: str):
    """Generate operational metrics dashboard"""
    print("\nGenerating Operational Metrics Dashboard...")

    # Create operational dashboard pages
    route_performance_widgets = [
        builder.create_table_widget(
            name="top_routes",
            table_name="route_performance",
            position=builder.WidgetPosition(0, 0, 6, 5).__dict__,
            title="Top 20 Most Profitable Routes",
            custom_query=f"""
                SELECT 
                    PULocationID,
                    DOLocationID,
                    route_type,
                    trip_count,
                    ROUND(avg_distance, 2) as avg_distance,
                    ROUND(avg_total_revenue, 2) as avg_revenue,
                    ROUND(revenue_per_minute, 2) as revenue_per_minute,
                    profitability_rank
                FROM {builder.get_table_path('route_performance')}
                ORDER BY revenue_per_minute DESC
                LIMIT 20
            """
        ),

        builder.create_bar_chart(
            name="route_type_distribution",
            table_name="route_performance",
            x_column="route_type",
            y_column="total_trips",
            position=builder.WidgetPosition(0, 5, 3, 4).__dict__,
            title="Trip Volume by Route Type",
            custom_query=f"""
                SELECT 
                    route_type,
                    SUM(trip_count) as total_trips
                FROM {builder.get_table_path('route_performance')}
                GROUP BY route_type
                ORDER BY total_trips DESC
            """
        ),

        builder.create_bar_chart(
            name="route_type_revenue",
            table_name="route_performance",
            x_column="route_type",
            y_column="total_revenue",
            position=builder.WidgetPosition(3, 5, 3, 4).__dict__,
            title="Revenue by Route Type",
            custom_query=f"""
                SELECT 
                    route_type,
                    SUM(total_revenue) as total_revenue
                FROM {builder.get_table_path('route_performance')}
                GROUP BY route_type
                ORDER BY total_revenue DESC
            """
        )
    ]

    # Customer segment widgets
    customer_segment_widgets = [
        builder.create_table_widget(
            name="customer_segments",
            table_name="customer_segments",
            position=builder.WidgetPosition(0, 0, 6, 5).__dict__,
            title="Customer Segments Performance",
            custom_query=f"""
                SELECT 
                    customer_segment,
                    payment_type_name,
                    time_of_day,
                    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
                    trip_count,
                    ROUND(avg_spend, 2) as avg_spend,
                    ROUND(avg_tip_pct, 1) as avg_tip_pct,
                    ROUND(total_revenue, 2) as total_revenue,
                    ROUND(revenue_contribution_pct, 2) as revenue_contribution_pct
                FROM {builder.get_table_path('customer_segments')}
                ORDER BY total_revenue DESC
            """
        )
    ]

    from dashboard_builder import DashboardPage

    dashboard = {
        "pages": [
            DashboardPage(
                name="route_performance",
                display_name="🗺️ Route Performance",
                layout=route_performance_widgets
            ).to_dict(),
            DashboardPage(
                name="customer_segments",
                display_name="👥 Customer Segments",
                layout=customer_segment_widgets
            ).to_dict()
        ]
    }

    # Export JSON
    json_path = os.path.join(output_dir, "operational_dashboard.json")
    builder.export_to_json(dashboard, json_path)
    print(f"  ✓ Exported JSON to {json_path}")

    # Export YAML resource
    yaml_resource = builder.export_to_yaml_resource("operational_metrics_dashboard", dashboard)
    yaml_path = os.path.join(output_dir, "operational_metrics.yml")

    with open(yaml_path, 'w') as f:
        f.write(yaml_resource)
    print(f"  ✓ Exported YAML resource to {yaml_path}")

    return dashboard


def generate_readme(output_dir: str, project_name: str):
    """Generate README for dashboard deployments"""
    readme_content = f"""# Dashboard Deployments

This directory contains dashboard configurations for the **{project_name}** project.

## 📊 Available Dashboards

### 1. Executive KPI Dashboard
**File**: `executive_kpi.yml`
**Purpose**: High-level executive metrics and KPIs
**Pages**:
- Executive Summary: Current performance snapshot
- Daily Trends: Revenue and trip volume trends
- Hourly Patterns: Peak demand analysis

### 2. Operational Metrics Dashboard
**File**: `operational_metrics.yml`
**Purpose**: Operational insights for strategy and operations teams
**Pages**:
- Route Performance: Most profitable routes
- Customer Segments: Behavior-based segmentation

## 🚀 Deployment

These dashboards are automatically deployed via Databricks Asset Bundles.

### Deploy to Development
```bash
databricks bundle deploy --target dev
```

### Deploy to Production
```bash
databricks bundle deploy --target prod
```

## 🔧 Configuration

Dashboard configurations use variables from `databricks.yml`:
- `${{var.catalog_name}}`: Unity Catalog name
- `${{var.schema_name}}`: Schema name
- `${{var.project_name}}`: Project identifier
- `${{var.dashboard_warehouse_id}}`: SQL Warehouse for queries

## 📝 Customization

### Adding New Dashboards

1. Use the `DashboardBuilder` class in `src/dashboards/dashboard_builder.py`
2. Create a new function in `deploy_dashboards.py`
3. Run the deployment script to generate YAML configuration
4. Include in `databricks.yml` using the `include` directive

### Example: Custom Dashboard

```python
from dashboard_builder import DashboardBuilder

builder = DashboardBuilder(
    catalog="${{var.catalog_name}}",
    schema="${{var.schema_name}}",
    project_name="${{var.project_name}}"
)

# Create custom dashboard
custom_dashboard = {{
    "pages": [
        # ... your pages
    ]
}}

# Export
builder.export_to_json(custom_dashboard, "custom_dashboard.json")
yaml_resource = builder.export_to_yaml_resource("custom_dashboard", custom_dashboard)
```

## 🎨 Widget Types

The `DashboardBuilder` supports:
- **KPI Counters**: Single metric displays
- **Line Charts**: Trend visualization
- **Bar Charts**: Categorical comparisons
- **Tables**: Detailed data views
- **Pie Charts**: Distribution analysis

## 📐 Layout System

Widgets use a grid layout system:
- Width: 0-6 (6 columns total)
- Height: Variable (2-8 typical)
- Position: (x, y) coordinates

### Example Widget Position
```python
WidgetPosition(
    x=0,      # Left edge
    y=0,      # Top edge
    width=3,  # Half width
    height=4  # Standard height
)
```

## 🔐 Permissions

Dashboards inherit permissions from the workspace:
- `CAN_RUN`: All users can view
- `CAN_MANAGE`: Admins can edit

Configure in YAML:
```yaml
permissions:
  - level: "CAN_RUN"
    group_name: "users"
  - level: "CAN_MANAGE"
    group_name: "admins"
```

## 🐛 Troubleshooting

### Dashboard Not Appearing
1. Check warehouse ID is valid
2. Verify catalog/schema permissions
3. Ensure gold tables exist

### Query Errors
1. Verify table names match gold layer
2. Check column names in queries
3. Validate Unity Catalog permissions

### Deployment Failures
1. Run `databricks bundle validate`
2. Check YAML syntax
3. Verify workspace permissions

## 📚 References

- [Databricks Dashboards](https://docs.databricks.com/dashboards/index.html)
- [Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
"""

    readme_path = os.path.join(output_dir, "README.md")
    with open(readme_path, 'w') as f:
        f.write(readme_content)
    print(f"\n  ✓ Generated README at {readme_path}")


def main():
    """Main execution function"""
    args = parse_arguments()

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║           Dashboard Configuration Generator                  ║
╚══════════════════════════════════════════════════════════════╝

Configuration:
  Catalog:      {args.catalog}
  Schema:       {args.schema}
  Project:      {args.project}
  Environment:  {args.environment}
  Output Dir:   {args.output_dir}
  Gold Prefix:  {args.gold_prefix}

""")

    # Ensure output directory exists
    ensure_directory(args.output_dir)

    # Create dashboard builder
    builder = DashboardBuilder(
        catalog=args.catalog,
        schema=args.schema,
        project_name=args.project,
        gold_table_prefix=args.gold_prefix
    )

    # Generate dashboards
    try:
        executive_dashboard = generate_executive_dashboard(builder, args.output_dir)
        operational_dashboard = generate_operational_dashboard(builder, args.output_dir)
        generate_readme(args.output_dir, args.project)

        print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    ✓ Success!                                ║
╚══════════════════════════════════════════════════════════════╝

Generated Dashboards:
  1. Executive KPI Dashboard
  2. Operational Metrics Dashboard

Next Steps:
  1. Review generated YAML files in {args.output_dir}
  2. Ensure databricks.yml includes: include: resources/dashboards/*.yml
  3. Deploy with: databricks bundle deploy --target {args.environment}
  4. Access dashboards in: /Shared/{args.project}/dashboards

""")

    except Exception as e:
        print(f"\n❌ Error generating dashboards: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()