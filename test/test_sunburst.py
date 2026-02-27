import plotly.graph_objects as go

ids = ["อาหาร", "อาหาร/ร้าน", "อาหาร/_other"]
labels = ["อาหาร", "ร้าน", "อื่นๆ"]
parents = ["", "อาหาร", "อาหาร"]
values = [100.0, -10.0, 110.0]

try:
    fig = go.Figure(go.Sunburst(
        ids=ids,
        labels=labels,
        parents=parents,
        values=values,
        branchvalues="total"
    ))
    fig.write_html("test.html")
    print("Success")
except Exception as e:
    print(f"Error: {e}")
