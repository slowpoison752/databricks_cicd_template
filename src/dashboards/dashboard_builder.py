"""
Generic Dashboard Builder for Databricks Asset Bundles

This module provides reusable components for building Databricks dashboards
that work across different projects and environments.

Usage:
    from dashboard_builder import DashboardBuilder, DashboardWidget

    builder = DashboardBuilder(
        catalog="dev_catalog",
        schema="nyc_taxi",
        project_name="nyc_taxi"
    )

    dashboard = builder.create_executive_dashboard()
"""

import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class WidgetType(Enum):
    """Supported widget types in Databricks dashboards"""
    TABLE = "table"
    COUNTER = "counter"
    LINE_CHART = "line"
    BAR_CHART = "bar"
    PIE_CHART = "pie"
    AREA_CHART = "area"
    SCATTER = "scatter"


class AggregationType(Enum):
    """Aggregation types for visualizations"""
    SUM = "SUM"
    AVG = "AVG"
    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"


@dataclass
class DashboardQuery:
    """Represents a dashboard query"""
    name: str
    query: str
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "name": self.name,
            "query": self.query
        }
        if self.description:
            result["description"] = self.description
        return result


@dataclass
class WidgetPosition:
    """Widget position and size on dashboard"""
    x: int
    y: int
    width: int
    height: int

    def to_dict(self) -> Dict[str, int]:
        return asdict(self)


@dataclass
class WidgetSpec:
    """Widget specification for visualization"""
    widget_type: WidgetType
    encodings: Optional[Dict[str, Any]] = None
    frame: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        spec = {
            "version": 2,
            "widgetType": self.widget_type.value
        }
        if self.encodings:
            spec["encodings"] = self.encodings
        if self.frame:
            spec["frame"] = self.frame
        return spec


@dataclass
class DashboardWidget:
    """Complete widget definition"""
    name: str
    queries: List[DashboardQuery]
    spec: WidgetSpec
    position: WidgetPosition
    title: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        widget_dict = {
            "widget": {
                "name": self.name,
                "queries": [q.to_dict() for q in self.queries],
                "spec": self.spec.to_dict()
            },
            "position": self.position.to_dict()
        }

        if self.title:
            widget_dict["widget"]["textbox_spec"] = self.title
        if self.description:
            widget_dict["widget"]["description"] = self.description

        return widget_dict


@dataclass
class DashboardPage:
    """Dashboard page containing widgets"""
    name: str
    display_name: str
    layout: List[DashboardWidget]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "displayName": self.display_name,
            "layout": [w.to_dict() for w in self.layout]
        }


class DashboardBuilder:
    """
    Generic dashboard builder that can be reused across projects.

    This builder creates dashboard definitions that are:
    - Environment-agnostic (uses variables for catalog/schema)
    - Project-agnostic (can be reused with different table names)
    - Maintainable (structured, typed components)
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        project_name: str,
        gold_table_prefix: str = "gold_"
    ):
        """
        Initialize dashboard builder

        Args:
            catalog: Unity Catalog name
            schema: Schema name within catalog
            project_name: Project name for naming conventions
            gold_table_prefix: Prefix for gold layer tables
        """
        self.catalog = catalog
        self.schema = schema
        self.project_name = project_name
        self.gold_table_prefix = gold_table_prefix

    def get_table_path(self, table_name: str) -> str:
        """Get fully qualified table path"""
        return f"{self.catalog}.{self.schema}.{self.gold_table_prefix}{table_name}"

    def create_query(self, table_name: str, query_name: str, custom_query: Optional[str] = None) -> DashboardQuery:
        """
        Create a dashboard query

        Args:
            table_name: Name of the gold table (without prefix)
            query_name: Name for the query
            custom_query: Custom SQL query (if None, uses SELECT *)
        """
        if custom_query:
            query = custom_query
        else:
            query = f"SELECT * FROM {self.get_table_path(table_name)}"

        return DashboardQuery(
            name=query_name,
            query=query,
            description=f"Query for {table_name}"
        )

    def create_kpi_counter(
        self,
        name: str,
        table_name: str,
        metric_column: str,
        label: str,
        position: WidgetPosition,
        format_string: str = "{value}"
    ) -> DashboardWidget:
        """Create a KPI counter widget"""

        query = self.create_query(
            table_name=table_name,
            query_name=f"{name}_query",
            custom_query=f"""
                SELECT {metric_column} as value
                FROM {self.get_table_path(table_name)}
                ORDER BY last_updated DESC
                LIMIT 1
            """
        )

        spec = WidgetSpec(
            widget_type=WidgetType.COUNTER,
            encodings={
                "value": {
                    "fieldName": "value",
                    "displayName": label
                }
            }
        )

        return DashboardWidget(
            name=name,
            queries=[query],
            spec=spec,
            position=position,
            title=label
        )

    def create_line_chart(
        self,
        name: str,
        table_name: str,
        x_column: str,
        y_columns: List[str],
        position: WidgetPosition,
        title: str,
        custom_query: Optional[str] = None
    ) -> DashboardWidget:
        """Create a line chart widget"""

        query = self.create_query(
            table_name=table_name,
            query_name=f"{name}_query",
            custom_query=custom_query
        )

        # Build encodings for multiple Y axes
        encodings = {
            "x": {
                "fieldName": x_column,
                "displayName": x_column.replace("_", " ").title()
            }
        }

        for idx, y_col in enumerate(y_columns):
            encodings[f"y{idx if idx > 0 else ''}"] = {
                "fieldName": y_col,
                "displayName": y_col.replace("_", " ").title()
            }

        spec = WidgetSpec(
            widget_type=WidgetType.LINE_CHART,
            encodings=encodings
        )

        return DashboardWidget(
            name=name,
            queries=[query],
            spec=spec,
            position=position,
            title=title
        )

    def create_bar_chart(
        self,
        name: str,
        table_name: str,
        x_column: str,
        y_column: str,
        position: WidgetPosition,
        title: str,
        custom_query: Optional[str] = None
    ) -> DashboardWidget:
        """Create a bar chart widget"""

        query = self.create_query(
            table_name=table_name,
            query_name=f"{name}_query",
            custom_query=custom_query
        )

        spec = WidgetSpec(
            widget_type=WidgetType.BAR_CHART,
            encodings={
                "x": {
                    "fieldName": x_column,
                    "displayName": x_column.replace("_", " ").title()
                },
                "y": {
                    "fieldName": y_column,
                    "displayName": y_column.replace("_", " ").title()
                }
            }
        )

        return DashboardWidget(
            name=name,
            queries=[query],
            spec=spec,
            position=position,
            title=title
        )

    def create_table_widget(
        self,
        name: str,
        table_name: str,
        position: WidgetPosition,
        title: str,
        custom_query: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> DashboardWidget:
        """Create a table widget"""

        if columns and not custom_query:
            column_list = ", ".join(columns)
            custom_query = f"SELECT {column_list} FROM {self.get_table_path(table_name)}"

        query = self.create_query(
            table_name=table_name,
            query_name=f"{name}_query",
            custom_query=custom_query
        )

        spec = WidgetSpec(widget_type=WidgetType.TABLE)

        return DashboardWidget(
            name=name,
            queries=[query],
            spec=spec,
            position=position,
            title=title
        )

    def create_executive_dashboard(self) -> Dict[str, Any]:
        """
        Create a complete executive dashboard

        Returns:
            Dictionary representing the full dashboard configuration
        """

        # Page 1: Executive Summary
        exec_summary_widgets = [
            # KPI Counters - Top Row
            self.create_kpi_counter(
                name="trips_today",
                table_name="executive_summary",
                metric_column="trips_today",
                label="Trips Today",
                position=WidgetPosition(0, 0, 2, 2)
            ),
            self.create_kpi_counter(
                name="revenue_today",
                table_name="executive_summary",
                metric_column="revenue_today",
                label="Revenue Today",
                position=WidgetPosition(2, 0, 2, 2)
            ),
            self.create_kpi_counter(
                name="avg_revenue_per_trip",
                table_name="executive_summary",
                metric_column="avg_revenue_per_trip_today",
                label="Avg Revenue/Trip",
                position=WidgetPosition(4, 0, 2, 2)
            ),

            # Executive Summary Table
            self.create_table_widget(
                name="exec_summary_table",
                table_name="executive_summary",
                position=WidgetPosition(0, 2, 6, 3),
                title="Executive Summary Metrics"
            )
        ]

        exec_summary_page = DashboardPage(
            name="executive_summary",
            display_name="📊 Executive Summary",
            layout=exec_summary_widgets
        )

        # Page 2: Daily Trends
        daily_trends_widgets = [
            # Revenue Trend Line Chart
            self.create_line_chart(
                name="revenue_trend",
                table_name="daily_kpis",
                x_column="pickup_date",
                y_columns=["total_revenue"],
                position=WidgetPosition(0, 0, 6, 4),
                title="Daily Revenue Trend (Last 30 Days)",
                custom_query=f"""
                    SELECT pickup_date, total_revenue
                    FROM {self.get_table_path('daily_kpis')}
                    ORDER BY pickup_date DESC
                    LIMIT 30
                """
            ),

            # Trip Volume Trend
            self.create_line_chart(
                name="trip_volume_trend",
                table_name="daily_kpis",
                x_column="pickup_date",
                y_columns=["total_trips"],
                position=WidgetPosition(0, 4, 6, 4),
                title="Daily Trip Volume Trend",
                custom_query=f"""
                    SELECT pickup_date, total_trips
                    FROM {self.get_table_path('daily_kpis')}
                    ORDER BY pickup_date DESC
                    LIMIT 30
                """
            ),

            # KPI Table
            self.create_table_widget(
                name="daily_kpis_table",
                table_name="daily_kpis",
                position=WidgetPosition(0, 8, 6, 4),
                title="Daily KPIs",
                custom_query=f"""
                    SELECT 
                        pickup_date,
                        total_trips,
                        ROUND(trips_change_pct, 2) as trips_change_pct,
                        total_revenue,
                        ROUND(revenue_change_pct, 2) as revenue_change_pct,
                        ROUND(avg_revenue_per_trip, 2) as avg_revenue_per_trip
                    FROM {self.get_table_path('daily_kpis')}
                    ORDER BY pickup_date DESC
                    LIMIT 30
                """
            )
        ]

        daily_trends_page = DashboardPage(
            name="daily_trends",
            display_name="📈 Daily Trends",
            layout=daily_trends_widgets
        )

        # Page 3: Hourly Patterns
        hourly_patterns_widgets = [
            # Peak Hours Bar Chart
            self.create_bar_chart(
                name="peak_hours_weekday",
                table_name="peak_hours_analysis",
                x_column="pickup_hour",
                y_column="avg_trips_per_day",
                position=WidgetPosition(0, 0, 6, 4),
                title="Peak Hours - Weekday",
                custom_query=f"""
                    SELECT pickup_hour, avg_trips_per_day
                    FROM {self.get_table_path('peak_hours_analysis')}
                    WHERE is_weekend = FALSE
                    ORDER BY pickup_hour
                """
            ),

            # Peak Hours Weekend
            self.create_bar_chart(
                name="peak_hours_weekend",
                table_name="peak_hours_analysis",
                x_column="pickup_hour",
                y_column="avg_trips_per_day",
                position=WidgetPosition(0, 4, 6, 4),
                title="Peak Hours - Weekend",
                custom_query=f"""
                    SELECT pickup_hour, avg_trips_per_day
                    FROM {self.get_table_path('peak_hours_analysis')}
                    WHERE is_weekend = TRUE
                    ORDER BY pickup_hour
                """
            )
        ]

        hourly_patterns_page = DashboardPage(
            name="hourly_patterns",
            display_name="⏰ Hourly Patterns",
            layout=hourly_patterns_widgets
        )

        # Combine all pages
        dashboard = {
            "pages": [
                exec_summary_page.to_dict(),
                daily_trends_page.to_dict(),
                hourly_patterns_page.to_dict()
            ]
        }

        return dashboard

    def export_to_json(self, dashboard: Dict[str, Any], output_path: str):
        """Export dashboard configuration to JSON file"""
        with open(output_path, 'w') as f:
            json.dump(dashboard, f, indent=2)

    def export_to_yaml_resource(self, dashboard_name: str, dashboard: Dict[str, Any]) -> str:
        """
        Export dashboard as YAML resource definition for databricks.yml

        Returns:
            YAML string that can be included in databricks.yml
        """
        yaml_template = f"""
resources:
  dashboards:
    {dashboard_name}:
      display_name: "{dashboard_name.replace('_', ' ').title()} - ${{var.project_name}}${{var.environment_suffix}}"
      warehouse_id: "${{var.dashboard_warehouse_id}}"
      parent_path: "/Shared/${{var.project_name}}/dashboards${{var.environment_suffix}}"
      
      serialized_dashboard: |
{json.dumps(dashboard, indent=8)}
      
      tags:
        - "dashboard"
        - "executive"
        - "${{var.project_name}}"
      
      permissions:
        - level: "CAN_RUN"
          group_name: "users"
        - level: "CAN_MANAGE"
          group_name: "admins"
"""
        return yaml_template


# Example usage
if __name__ == "__main__":
    # Create builder with your environment configuration
    builder = DashboardBuilder(
        catalog="${var.catalog_name}",  # Uses variable for environment flexibility
        schema="${var.schema_name}",
        project_name="${var.project_name}"
    )

    # Create executive dashboard
    dashboard = builder.create_executive_dashboard()

    # Export to JSON for review
    builder.export_to_json(dashboard, "executive_dashboard.json")

    # Export to YAML resource for databricks.yml
    yaml_resource = builder.export_to_yaml_resource("executive_kpi_dashboard", dashboard)
    print(yaml_resource)