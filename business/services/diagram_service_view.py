

class DiagramService:
    @staticmethod
    def grouped_bar_chart_html():
        # Backend-generated chart using Plotly (Matplotlib-style grouped bars with labels)
        import plotly.graph_objects as go
        from plotly.offline import plot

        groups = ['G1', 'G2', 'G3', 'G4', 'G5']
        men = [20, 34, 30, 35, 27]
        women = [25, 32, 34, 20, 25]

        fig = go.Figure()
        fig.add_bar(name='Men', x=groups, y=men, text=[str(v) for v in men], textposition='outside', marker_color='#1f77b4')
        fig.add_bar(name='Women', x=groups, y=women, text=[str(v) for v in women], textposition='outside', marker_color='#ff7f0e')

        fig.update_layout(
            barmode='group',
            margin=dict(t=40, r=20, b=60, l=40),
            yaxis_title='Scores',
            xaxis_title='Group',
            legend=dict(orientation='h', x=0, y=1.15),
            uniformtext=dict(mode='hide', minsize=10),
            height=450
        )

        # Return a full HTML snippet with a standalone div + script
        return plot(fig, include_plotlyjs='cdn', output_type='div')

