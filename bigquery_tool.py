import bigframes.pandas as bpd
import ipywidgets as widgets
import plotly.express as px
import plotly.io as pio
import pandas as pd
from IPython.display import display, clear_output

# THE FIX: Force 'plotly_mimetype' for reliable rendering in Colab
pio.renderers.default = "plotly_mimetype"

def load_ipython_extension(ipython):
    """Register the magic when %load_ext bigquery_tool is called."""
    ipython.register_magic_function(bigquery_tool, magic_kind='cell')

def bigquery_tool(line, cell):
    table_id = line.strip()
    user_prompt = cell.strip().lower()
    
    # --- 1. UI Elements ---
    refresh_btn = widgets.Button(description="🔄 Sync Data", button_style='primary')
    filter_by = widgets.Dropdown(description='Filter By:')
    filter_val = widgets.Text(description='Value:', placeholder='e.g. active')
    select_by = widgets.SelectMultiple(description='Select By:', layout={'height': '100px', 'width': '35%'}) 
    agg_func = widgets.Dropdown(options=['none', 'count', 'mean', 'sum', 'min', 'max'], value='none', description='Aggregate:')
    output_type = widgets.Dropdown(
        options=['Tabular Data', 'Bar Chart', 'Scatter Plot', 'Histogram', 'Box Plot'], 
        value='Tabular Data', description='Output Type:'
    )
    
    control_ui = widgets.VBox([
        widgets.HBox([refresh_btn, filter_by, filter_val]),
        widgets.HBox([select_by, agg_func, output_type]),
    ], layout=widgets.Layout(padding='15px', border='1px solid #555', border_radius='10px'))
    
    out_area, sql_area = widgets.Output(), widgets.Output()
    current_df = [None]

    def update_viz(change=None):
        with out_area:
            clear_output(wait=True)
            df = current_df[0]
            if df is None or not select_by.value: return

            try:
                view_df = df.copy()
                targets = list(select_by.value)
                for t in targets:
                    view_df[t] = pd.to_numeric(view_df[t], errors='coerce')

                if filter_val.value and filter_by.value:
                    view_df = view_df[view_df[filter_by.value].astype(str).str.contains(filter_val.value, case=False)]

                func = agg_func.value
                
                # --- 2. Data Processing ---
                if func == 'count':
                    plot_df = view_df.groupby(targets).size().reset_index(name='count_records')
                    y_axis = 'count_records'
                elif func != 'none':
                    main_measure = targets[-1]
                    y_axis = f"{func}_{main_measure}"
                    series_res = view_df.groupby(targets)[main_measure].agg(func)
                    plot_df = series_res.reset_index(name=y_axis)
                else:
                    plot_df = view_df[targets].dropna()
                    y_axis = targets[-1] if len(targets) > 1 else targets[0]

                if len(targets) > 1 and func != 'none':
                    plot_df['Group Combination'] = plot_df[targets].astype(str).agg(' | '.join, axis=1)
                    x_axis = 'Group Combination'
                else:
                    x_axis = targets[0]

                # --- 3. Rendering Logic ---
                if output_type.value == 'Tabular Data':
                    display(plot_df)
                else:
                    if output_type.value == 'Scatter Plot':
                        fig = px.scatter(plot_df, x=targets[0], y=y_axis)
                    elif output_type.value == 'Box Plot': 
                        fig = px.box(plot_df, y=y_axis, x=targets[0] if len(targets) > 1 and func == 'none' else None)
                    elif output_type.value == 'Histogram': 
                        fig = px.histogram(plot_df, x=x_axis)
                    else: 
                        fig = px.bar(plot_df, x=x_axis, y=y_axis)
                    
                    fig.update_layout(height=450, template="plotly_white", xaxis_title=x_axis)
                    # Use MIME-type display instead of .show() for external extensions
                    display(fig) 
            except Exception as e:
                print(f"Viz Error: {e}")

    def load_data(b=None):
        with out_area:
            try:
                current_df[0] = bpd.read_gbq(table_id).head(500).to_pandas()
                all_cols = current_df[0].columns.tolist()
                filter_by.options = [''] + all_cols
                select_by.options = all_cols
                
                if any(k in user_prompt for k in ["dist", "count", "mean", "scatter"]):
                    agg_func.value = 'count' if "count" in user_prompt else ('mean' if "mean" in user_prompt else 'none')
                    output_type.value = 'Scatter Plot' if "scatter" in user_prompt else 'Bar Chart'
                else:
                    agg_func.value, output_type.value = 'none', 'Tabular Data'
                
                update_viz()
            except Exception as e: print(f"Query Error: {e}")

    refresh_btn.on_click(load_data)
    for w in [filter_by, filter_val, select_by, agg_func, output_type]: w.observe(update_viz, names='value')
    display(control_ui, sql_area, out_area)
    load_data()