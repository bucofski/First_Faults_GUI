import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.offline import plot

class DiagramService:
    @staticmethod
    def grouped_bar_chart_html():
        # Backend-generated chart using Plotly (Matplotlib-style grouped bars with labels)

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
        # ... existing code ...
        # Return a full HTML snippet with a standalone div + script
        return plot(fig, include_plotlyjs='cdn', output_type='div')

    @staticmethod
    def grouped_bar_chart_2_html():
        species = ("Adelie", "Chinstrap", "Gentoo")
        penguin_means = {
            'Bill Depth': (18.35, 18.43, 14.98),
            'Bill Length': (38.79, 48.83, 47.50),
            'Flipper Length': (189.95, 195.82, 217.19),
        }

        x = list(species)  # x-axis categories
        width = 0.25  # unused with Plotly group mode but kept for clarity

        fig = go.Figure()
        for attribute, measurement in penguin_means.items():
            fig.add_bar(
                name=attribute,
                x=x,
                y=measurement,
                text=[str(v) for v in measurement],
                textposition='outside'
            )

        fig.update_layout(
            barmode='group',
            margin=dict(t=40, r=20, b=60, l=40),
            yaxis_title='Length (mm)',
            xaxis_title='Species',
            legend=dict(orientation='h', x=0, y=1.15),
            uniformtext=dict(mode='hide', minsize=10),
            height=450,
            title='Penguin attributes by species'
        )

        # Do NOT include plotly.js again for the second chart
        return plot(fig, include_plotlyjs=False, output_type='div')

    @staticmethod
    def PieChart_html():
        # Sample data for pie chart
        labels = ['A', 'B', 'C', 'D', 'E']
        values = [20, 15, 30, 25, 10]

        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            textinfo='label+percent',
            insidetextorientation='radial'
        )])

        fig.update_layout(
            margin=dict(t=40, r=20, b=40, l=20),
            height=450,
            title='Sample Pie Chart'
        )

        # Return a full HTML snippet without including plotly.js again
        return plot(fig, include_plotlyjs=False, output_type='div')
